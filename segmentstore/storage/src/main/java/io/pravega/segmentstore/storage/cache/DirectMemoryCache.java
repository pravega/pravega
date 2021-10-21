/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.storage.cache;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.pravega.common.Exceptions;
import io.pravega.common.util.BufferView;
import io.pravega.shared.protocol.netty.ByteBufWrapper;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.NonNull;
import lombok.val;

/**
 * Represents a block-based {@link CacheStorage} that uses direct memory buffers to store data.
 *
 * Data Organization (actual values based on {@link CacheLayout.DefaultLayout}).
 * - The cache space is split into equal sized Buffers of length {@link CacheLayout#bufferSize()}.
 * - Each Buffer is split into equal sized Blocks of length {@link CacheLayout#blockSize()}.
 * - The first Block in a Buffer is reserved for metadata, the remaining Blocks are used to store Entry data.
 * - Each Block metadata requires {@link CacheLayout#blockMetadataSize()} bytes and its format is defined by {@link CacheLayout};
 * there are several methods in {@link CacheLayout} that allow manipulating it.
 * - A Buffer is considered full when all its Blocks are used. Non-full Buffers are kept in a FIFO queue; they are addded
 * at the end when they become non-full and taken from the beginning when we need to insert new Entries.
 * - An Entry may be split across multiple Buffers. It is inserted in the first available Buffer and anything that doesn't
 * fit in it is inserted into the next available buffer(s).
 * - Entries
 * -- Are split into equal sized Blocks ({@link CacheLayout#blockSize()}.
 * -- The Blocks form a singly-linked list with every block pointing to the previous block in the chain.
 * -- The address returned by {@link #insert} or {@link #replace} points to the last block in this chain.
 * -- No two Entries may share the same block. If the entry's size is not a multiple of {@link CacheLayout#blockSize()},
 * then the last block will not be fully used; it will still be {@link CacheLayout#blockSize()} in length but no other
 * entry may be added there.
 * - The last block can be filled with {@link #append} which is optimized to add data to only the last block in an entry
 * until it is full. It cannot be used to add new blocks to an entry (use {@link #getAppendableLength} to determine how
 * much can be appended).
 * - Fragmentation:
 * -- Entries that are larger than {@link CacheLayout#blockSize()} may not be stored in contiguous blocks. They may span
 * multiple non-adjacent Blocks in the same buffer or multiple Blocks in different Buffers.
 * -- The {@link DirectMemoryCache} performs no effort to consolidate or otherwise "defragment" the Buffers. Doing so would
 * introduce costly performance penalties for little benefit (accessing a specific Block in a Buffer is an O(1) lookup).
 *
 * Memory usage:
 * - This implementation makes use of {@link ByteBuf} with an {@link UnpooledByteBufAllocator} to allocate direct memory.
 * - Each {@link DirectMemoryBuffer} that is part of this instance will wrap a {@link ByteBuf} and keep a pointer to it.
 * - The {@link ByteBuf} instances are only allocated the first time their owning {@link DirectMemoryBuffer} instances
 * are used (not when they are constructed).
 * - The {@link ByteBuf} instances are only deallocated when {@link #close()} is invoked. They are not preemptively
 * deallocated (i.e., when utilization is low) to prevent memory fragmentation and the high cost of reallocating memory
 * when they are needed again. This {@link DirectMemoryCache} provides the best runtime performance when all buffers
 * have already been allocated.
 * - The {@link DirectMemoryCache} will make full use of the memory it is given to work with; consider reducing the value
 * of `maxSizeBytes` argument if less memory consumption is desired.
 * - It is important to note that the usable space will be less than `maxSizeBytes` due to internal fragmentation and
 * reserved Blocks (for metadata). Using {@link CacheLayout.DefaultLayout}, which has 2MB buffers with 4KB Blocks, we have:
 * -- 4KB reserved for each 2MB allocated (0.2%)
 * -- An average of 2KB of excess padding per entry due to internal fragmentation (entries occupy whole blocks even if
 * their length is not a multiple of {@link CacheLayout#blockSize()}). Consider using {@link #append} to make full use
 * of the last block capacity.
 * --- 2KB was estimated as an average of 0B and 4095B; 0KB excess is when an Entry's length is a multiple of
 * {@link CacheLayout#blockSize()} and 4095 is when an entry's length exceeds a multiple of {@link CacheLayout#blockSize()}
 * by 1 byte.
 * -- Use {@link #getState()} to get insights into memory usage.
 */
@ThreadSafe
public class DirectMemoryCache implements CacheStorage {
    //region Members

    /**
     * The maximum number of attempts to invoke {@link #tryCleanup} if at capacity and needing to insert more data.
     */
    @VisibleForTesting
    static final int MAX_CLEANUP_ATTEMPTS = 5;
    private final CacheLayout layout;
    private final DirectMemoryBuffer[] buffers;
    @GuardedBy("availableBufferIds")
    private final ArrayDeque<Integer> availableBufferIds;
    @GuardedBy("availableBufferIds")
    private final ArrayDeque<Integer> unallocatedBufferIds;
    private final AtomicBoolean closed;
    private final AtomicLong storedBytes;
    private final AtomicReference<Supplier<Boolean>> tryCleanup;
    private final AtomicInteger retryDelayBaseMillis;
    private final CacheMetrics metrics = new CacheMetrics();

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the {@link DirectMemoryCache} class.
     *
     * @param maxSizeBytes The maximum size (in bytes) of the cache. The actual capacity of the cache may be rounded up
     *                     to the nearest buffer size alignment, which is a multiple of {@link CacheLayout.DefaultLayout#bufferSize()}.
     * @throws IllegalArgumentException If maxSizeBytes is less than or equal to 0 or greater than {@link CacheLayout#MAX_TOTAL_SIZE}.
     */
    public DirectMemoryCache(long maxSizeBytes) {
        this(new CacheLayout.DefaultLayout(), maxSizeBytes);
    }

    /**
     * Creates a new instance of the {@link DirectMemoryCache} class.
     *
     * @param layout       The {@link CacheLayout} to use.
     * @param maxSizeBytes The maximum size (in bytes) of the cache. The actual capacity of the cache may be rounded up
     *                     to the nearest buffer size alignment, which is a multiple of {@link CacheLayout#bufferSize()}
     *                     when applied to layout.
     * @throws IllegalArgumentException If maxSizeBytes is less than or equal to 0 or greater than {@link CacheLayout#MAX_TOTAL_SIZE}.
     */
    @VisibleForTesting
    DirectMemoryCache(@NonNull CacheLayout layout, long maxSizeBytes) {
        Preconditions.checkArgument(maxSizeBytes > 0 && maxSizeBytes <= CacheLayout.MAX_TOTAL_SIZE,
                "maxSizeBytes must be a positive number less than %s.", CacheLayout.MAX_TOTAL_SIZE);
        maxSizeBytes = adjustMaxSizeIfNeeded(maxSizeBytes, layout);

        this.layout = layout;
        this.tryCleanup = new AtomicReference<>(null);
        this.retryDelayBaseMillis = new AtomicInteger(0);
        this.storedBytes = new AtomicLong(0);
        this.closed = new AtomicBoolean(false);
        this.buffers = new DirectMemoryBuffer[(int) (maxSizeBytes / this.layout.bufferSize())];
        this.availableBufferIds = new ArrayDeque<>(this.buffers.length);
        this.unallocatedBufferIds = new ArrayDeque<>(this.buffers.length);
        createBuffers();
    }

    /**
     * Creates all the {@link DirectMemoryBuffer} instances for this {@link DirectMemoryCache} instance.
     */
    @GuardedBy("availableBufferIds")
    private void createBuffers() {
        ByteBufAllocator allocator = createAllocator();
        for (int i = 0; i < this.buffers.length; i++) {
            this.unallocatedBufferIds.addLast(i);
            this.buffers[i] = new DirectMemoryBuffer(i, allocator, this.layout);
        }
    }

    @VisibleForTesting
    protected ByteBufAllocator createAllocator() {
        return new UnpooledByteBufAllocator(true, true);
    }

    /**
     * Rounds up the maxSize argument to be a multiple of {@link CacheLayout#bufferSize()} for the given layout.
     */
    private long adjustMaxSizeIfNeeded(long maxSize, CacheLayout layout) {
        long r = maxSize % layout.bufferSize();
        if (r != 0) {
            maxSize = maxSize - r + layout.bufferSize();
        }
        return maxSize;
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            synchronized (this.availableBufferIds) {
                this.availableBufferIds.clear();
                this.unallocatedBufferIds.clear();
            }

            for (DirectMemoryBuffer b : this.buffers) {
                b.close();
            }

            this.metrics.close();
        }
    }

    //endregion

    //region CacheStorage Implementation

    @Override
    public int getBlockAlignment() {
        return this.layout.blockSize();
    }

    @Override
    public int getMaxEntryLength() {
        return CacheLayout.MAX_ENTRY_SIZE;
    }

    @Override
    public int insert(BufferView data) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkArgument(data.getLength() <= CacheLayout.MAX_ENTRY_SIZE,
                "Entry too long. Expected max %s, given %s.", CacheLayout.MAX_ENTRY_SIZE, data.getLength());

        int lastBlockAddress = CacheLayout.NO_ADDRESS;
        int remainingLength = data.getLength();
        try {
            // As long as we still have data to copy, we try to reuse an existing, non-full buffer or allocate a new one,
            // and write the remaining data to it.
            while (remainingLength > 0 || lastBlockAddress == CacheLayout.NO_ADDRESS) {
                // Get a Buffer to write data to. If we are full, this will throw an appropriate exception.
                DirectMemoryBuffer buffer = getNextAvailableBuffer();

                // Write the data to the buffer.
                BufferView slice = data.slice(data.getLength() - remainingLength, remainingLength);
                DirectMemoryBuffer.WriteResult writeResult = buffer.write(slice, lastBlockAddress);
                if (writeResult == null) {
                    // Someone else grabbed this buffer and wrote to it before we got a chance. Go back and find another one.
                    continue;
                }

                // Update our state, including the number of bytes written. In case of a subsequent error (and rollback),
                // invoking delete() will undo this changes as well.
                assert writeResult.getWrittenLength() >= 0 && writeResult.getWrittenLength() <= remainingLength : writeResult.getWrittenLength();
                remainingLength -= writeResult.getWrittenLength();
                this.storedBytes.addAndGet(writeResult.getWrittenLength());
                lastBlockAddress = writeResult.getLastBlockAddress();
            }
        } catch (Throwable ex) {
            if (!Exceptions.mustRethrow(ex) && lastBlockAddress != CacheLayout.NO_ADDRESS) {
                // We wrote something, but got interrupted. We need to clean up whatever we wrote so we don't leave
                // unreferenced data in the cache.
                delete(lastBlockAddress);
            }
            throw ex;
        }

        this.metrics.insert(data.getLength());
        return lastBlockAddress;
    }

    @Override
    public int replace(int address, BufferView data) {
        // The simplest and safest way to do a replace is to first insert the new data and then remove the old one. This
        // will ensure the Cache remains consistent during the operation and we won't need to worry about concurrency or
        // potential CacheFullExceptions.
        int newAddress = insert(data);
        delete(address);
        return newAddress;
    }

    @Override
    public int getAppendableLength(int currentLength) {
        // If the current length is 0, we still allocate a block for it, so we need to indicate we can return the full
        // block length for that. Otherwise we can only append up to the end of the last block.
        int lastBlockLength = currentLength % this.layout.blockSize();
        return currentLength == 0 ? this.layout.blockSize() : (lastBlockLength == 0 ? 0 : this.layout.blockSize() - lastBlockLength);
    }

    @Override
    public int append(int address, int expectedLength, BufferView data) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkArgument(address != CacheLayout.NO_ADDRESS, "Invalid address.");

        // We can only append to fill the last block. For anything else a new write will be needed.
        int appendedBytes = 0;
        int expectedLastBlockLength = this.layout.blockSize() - getAppendableLength(expectedLength);
        Preconditions.checkArgument(expectedLastBlockLength + data.getLength() <= this.layout.blockSize(),
                "data is too long; use getAppendableLength() to determine how much data can be appended.");

        int bufferId = this.layout.getBufferId(address);
        int blockId = this.layout.getBlockId(address);
        appendedBytes = this.buffers[bufferId].tryAppend(blockId, expectedLastBlockLength, data);

        this.storedBytes.addAndGet(appendedBytes);
        this.metrics.append(appendedBytes);
        return appendedBytes;
    }

    @Override
    public void delete(int address) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        int deletedLength = 0;
        while (address != CacheLayout.NO_ADDRESS) {
            // Locate the Buffer-Block for the current address.
            int bufferId = this.layout.getBufferId(address);
            int blockId = this.layout.getBlockId(address);
            DirectMemoryBuffer b = this.buffers[bufferId];

            // Keep track if this was full before we removed anything from it.
            boolean wasFull = !b.hasCapacity();

            // Delete whatever we can from it and remember the successor.
            DirectMemoryBuffer.DeleteResult result = b.delete(blockId);
            address = result.getPredecessorAddress();
            deletedLength += result.getDeletedLength();
            if (wasFull && b.hasCapacity()) {
                synchronized (this.availableBufferIds) {
                    // This block was full before, but it no longer is now. Add it to the pool of available buffer ids
                    // so we can reuse it if we need to. There is a slim chance that this buffer becomes full in the
                    // time before we checked above and entering this block, but #getNextAvailableBuffer() can handle
                    // that situation.
                    this.availableBufferIds.addLast(b.getId());
                }
            }
        }

        this.storedBytes.addAndGet(-deletedLength);
        this.metrics.delete(deletedLength);
    }

    @Override
    public BufferView get(int address) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        List<ByteBuf> readBuffers = new ArrayList<>();

        while (address != CacheLayout.NO_ADDRESS) {
            // Locate the Buffer-Block for the current address.
            int bufferId = this.layout.getBufferId(address);
            int blockId = this.layout.getBlockId(address);
            DirectMemoryBuffer b = this.buffers[bufferId];

            // Fetch the read data into our buffer collection and then set the address to the next in the chain.
            address = b.read(blockId, readBuffers);
        }

        if (readBuffers.isEmpty()) {
            // Couldn't read anything, so this address must not point to anything.
            return null;
        } else {
            // Compose the result and return it.
            ByteBuf first = readBuffers.get(0);
            ByteBuf result = readBuffers.size() == 1 ? first :
                    new CompositeByteBuf(first.alloc(), false, readBuffers.size(), Lists.reverse(readBuffers));
            this.metrics.get(result.readableBytes());
            return new NonReleaseableByteBufWrapper(result);
        }
    }

    @Override
    public CacheState getState() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        int allocatedBufferCount = 0;
        int blockCount = 0;
        for (DirectMemoryBuffer b : this.buffers) {
            if (b.isAllocated()) {
                allocatedBufferCount++;
                blockCount += b.getUsedBlockCount();
            }
        }

        return new CacheState(
                this.storedBytes.get(),
                (long) blockCount * this.layout.blockSize(),
                (long) allocatedBufferCount * this.layout.blockSize(),
                (long) allocatedBufferCount * this.layout.bufferSize(),
                (long) this.buffers.length * this.layout.bufferSize());
    }

    @Override
    public void setCacheFullCallback(Supplier<Boolean> cacheFullCallback, int retryDelayBaseMillis) {
        this.tryCleanup.set(cacheFullCallback);
        this.retryDelayBaseMillis.set(retryDelayBaseMillis);
    }

    //endregion

    //region Helpers

    private DirectMemoryBuffer getNextAvailableBuffer() {
        int attempts = 0;
        while (attempts < MAX_CLEANUP_ATTEMPTS) {
            synchronized (this.availableBufferIds) {
                while (!this.availableBufferIds.isEmpty() || !this.unallocatedBufferIds.isEmpty()) {
                    while (!this.availableBufferIds.isEmpty()) {
                        // We found a Buffer that is available.
                        DirectMemoryBuffer b = this.buffers[this.availableBufferIds.peekFirst()];
                        if (b.hasCapacity()) {
                            // Reusing a buffer.
                            return b;
                        } else {
                            // Buffer is actually full. Clean up. We lazily remove buffers from this pool, since we want
                            // to introduce as little synchronization overhead in the insert() method so we delay this
                            // as much as we can.
                            this.availableBufferIds.removeFirst();
                        }
                    }

                    if (!this.unallocatedBufferIds.isEmpty()) {
                        // We can't reuse any existing buffers, but there are unallocated ones. Fetch one and use it.
                        this.availableBufferIds.addLast(this.unallocatedBufferIds.removeFirst());
                    }
                }
            }

            // If we get here, there are no available buffers and we have allocated all the buffers we could. Notify
            // any upstream listeners to attempt a cleanup (if possible).
            attempts++;
            tryCleanup(attempts);
        }

        // Unable to reuse any existing buffer or find a new one to allocate and upstream code could not free up data.
        throw new CacheFullException(String.format("%s full: %s.", DirectMemoryCache.class.getSimpleName(), getState()));
    }

    private void tryCleanup(int attempts) {
        val c = this.tryCleanup.get();
        if (c != null && !c.get()) {
            // Unable to clean up the cache. Wait a bit, then try again.
            int sleepMillis = attempts * this.retryDelayBaseMillis.get();
            if (sleepMillis > 0 && attempts < MAX_CLEANUP_ATTEMPTS) {
                Exceptions.handleInterrupted(() -> Thread.sleep(sleepMillis));
            }
        }
    }

    //endregion

    //region NonReleaseableByteBufWrapper

    /**
     * {@link ByteBufWrapper} that does not enable releasing buffers.
     */
    private static class NonReleaseableByteBufWrapper extends ByteBufWrapper {
        NonReleaseableByteBufWrapper(@NonNull ByteBuf buf) {
            super(buf);
        }

        @Override
        public void retain() {
            // Nothing to do.
        }

        @Override
        public void release() {
            // Nothing to do. We don't want an external caller to release and deallocate our internal cache buffers.
        }
    }

    //endregion
}

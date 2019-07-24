/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.datastore;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.pravega.common.Exceptions;
import io.pravega.common.util.BufferView;
import io.pravega.shared.protocol.netty.ByteBufWrapper;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.NonNull;
import lombok.SneakyThrows;

@ThreadSafe
public class DirectMemoryStore implements DataStore {
    private final StoreLayout layout;
    private final UnpooledByteBufAllocator allocator;
    @GuardedBy("buffers")
    private final Buffer[] buffers;
    @GuardedBy("buffers")
    private final ArrayDeque<Integer> availableBufferIds;
    @GuardedBy("buffers")
    private final ArrayDeque<Integer> unallocatedBufferIds;
    private final AtomicBoolean closed;
    private final AtomicLong storedBytes;

    public DirectMemoryStore(long maxSizeBytes) {
        this(new StoreLayout.DefaultLayout(), maxSizeBytes);
    }

    public DirectMemoryStore(@NonNull StoreLayout layout, long maxSizeBytes) {
        Preconditions.checkArgument(maxSizeBytes > 0 && maxSizeBytes < StoreLayout.MAX_TOTAL_SIZE,
                "maxSizeBytes must be a positive number less than %s.", StoreLayout.MAX_TOTAL_SIZE);
        maxSizeBytes = adjustMaxSizeIfNeeded(maxSizeBytes, layout);

        this.layout = layout;
        this.allocator = new UnpooledByteBufAllocator(true, true);
        this.buffers = new Buffer[(int) (maxSizeBytes / this.layout.bufferSize())];
        this.availableBufferIds = new ArrayDeque<>();
        this.unallocatedBufferIds = IntStream.range(0, this.buffers.length - 1).boxed().collect(Collectors.toCollection(ArrayDeque::new));
        this.storedBytes = new AtomicLong(0);
        this.closed = new AtomicBoolean(false);
    }

    private long adjustMaxSizeIfNeeded(long maxSize, StoreLayout layout) {
        long r = maxSize % layout.bufferSize();
        if (r != 0) {
            maxSize = maxSize - r + layout.bufferSize();
        }
        return maxSize;
    }

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            synchronized (this.buffers) {
                for (int i = 0; i < this.buffers.length; i++) {
                    Buffer b = this.buffers[i];
                    if (b != null) {
                        b.close();
                        this.buffers[i] = null;
                    }
                }

                this.availableBufferIds.clear();
                this.unallocatedBufferIds.clear();
            }
        }
    }

    //region DataStore Implementation

    @Override
    public int getBlockAlignment() {
        return this.layout.blockSize();
    }

    @Override
    public int getMaxEntryLength() {
        return StoreLayout.MAX_ENTRY_LENGTH;
    }

    @Override
    @SneakyThrows(IOException.class)
    public int insert(BufferView data) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkArgument(data.getLength() < StoreLayout.MAX_ENTRY_LENGTH);

        int length = data.getLength();
        int firstBlockAddress = StoreLayout.NO_BLOCK_ID;
        try (InputStream s = data.getReader()) {
            // Loop through all the registered buffers
            // Once we get a Buffer, allocate and write data to it. If no more buffers, allocate more.
            // Continue looping until we've written the whole data or run out of space.

            Buffer lastBuffer = null;
            Buffer.WriteResult lastResult = null;
            while (length > 0 || firstBlockAddress == StoreLayout.NO_BLOCK_ID) {
                Buffer buffer = getNextAvailableBuffer();
                if (buffer == null) {
                    buffer = allocateNewBuffer();
                }

                Buffer.WriteResult writeResult = buffer.write(s, length, firstBlockAddress == StoreLayout.NO_BLOCK_ID);
                if (writeResult == null) {
                    // Someone else grabbed this buffer and wrote to it before we got a chance.
                    continue;
                }

                assert writeResult.getRemainingLength() >= 0 && writeResult.getRemainingLength() < length;
                length = writeResult.getRemainingLength();
                if (firstBlockAddress == StoreLayout.NO_BLOCK_ID) {
                    // First write. Remember this address.
                    firstBlockAddress = writeResult.getFirstBlockAddress();
                } else {
                    // Chain
                    lastBuffer.setSuccessor(lastResult.getLastBlockAddress(), writeResult.getFirstBlockAddress());
                }

                lastBuffer = buffer;
                lastResult = writeResult;
            }
        } catch (Throwable ex) {
            if (!Exceptions.mustRethrow(ex)) {
                // Cleanup whatever we have done so far.
                if (firstBlockAddress != StoreLayout.NO_BLOCK_ID) {
                    delete(firstBlockAddress);
                }
            }
            throw ex;
        }

        this.storedBytes.addAndGet(data.getLength());
        return firstBlockAddress;
    }

    @Override
    public int replace(int address, BufferView data) {
        // We need a way to ensure that a replace (with a longer buffer) will not corrupt the data if it fails due to
        // store being full. Doing this correctly is complex and may have a performance penalty. For now, a compromise
        // solution involves: write a new entry, then (if successful) delete the old one. This approach also handles
        // read consistency: we don't need to worry about a caller reading the data while we are updating it.
        int newAddress = insert(data);
        delete(address);
        return newAddress;
    }

    /**
     * Gets the number of bytes that can be appended to an entry of the given length.
     *
     * @param currentLength
     * @return
     */
    @Override
    public int getAppendableLength(int currentLength) {
        // Length == 0 - > BlockSIze;
        // Else if Length at Block Boundary -> 0
        // else Block Length - Excess.
        return currentLength == 0
                ? this.layout.blockSize()
                : (currentLength % this.layout.blockSize() == 0 ? 0 : this.layout.blockSize() - currentLength % this.layout.blockSize());
    }

    @Override
    @SneakyThrows(IOException.class)
    public int append(int address, int expectedLength, BufferView data) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        int expectedLastBlockLength = expectedLength % this.layout.blockSize();
        Preconditions.checkArgument(expectedLastBlockLength + data.getLength() <= this.layout.blockSize(),
                "");

        // We can only append to fill the last block. For anything else a new write will be needed.

        boolean mustExist = false;
        int appended = 0;
        try (InputStream s = data.getReader()) {
            while (address != StoreLayout.NO_ADDRESS) {
                int bufferId = this.layout.getBufferId(address);
                int blockId = this.layout.getBlockId(address);
                Buffer b = getBuffer(bufferId, mustExist);
                if (b == null) {
                    return 0;
                }

                Buffer.AppendResult appendResult = b.tryAppend(blockId, expectedLastBlockLength, s, data.getLength());
                address = appendResult.getNextBlockAddress();
                appended += appendResult.getAppendedlength();
                mustExist = true;
            }
        }

        this.storedBytes.addAndGet(appended);
        return appended;
    }

    @Override
    public boolean delete(int address) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        while (address != StoreLayout.NO_ADDRESS) {
            int bufferId = this.layout.getBufferId(address);
            int blockId = this.layout.getBlockId(address);
            Buffer b;
            boolean wasFull;
            synchronized (this.buffers) {
                b = this.buffers[bufferId];
                if (b == null) {
                    // This could be due to bad initial address or corrupted data. For deletes, we don't care.
                    return false;
                }

                wasFull = !b.hasCapacity();
            }

            Buffer.DeleteResult result = b.delete(blockId);
            address = result.getSuccessorAddress();
            this.storedBytes.addAndGet(-result.getDeletedLength());
            if (wasFull) {
                synchronized (this.buffers) {
                    if (b.hasCapacity()) {
                        this.availableBufferIds.addLast(b.getId());
                    }
                }
            }
        }

        return true;
    }

    @Override
    public BufferView get(int address) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        ArrayList<ByteBuf> readBuffers = new ArrayList<>();
        while (address != StoreLayout.NO_ADDRESS) {
            int bufferId = this.layout.getBufferId(address);
            int blockId = this.layout.getBlockId(address);
            Buffer b = getBuffer(bufferId, !readBuffers.isEmpty());
            if (b == null) {
                return null;
            }

            address = b.read(blockId, readBuffers);
        }

        ByteBuf[] result = readBuffers.stream().filter(ByteBuf::isReadable).toArray(ByteBuf[]::new);
        return new ByteBufWrapper(Unpooled.wrappedBuffer(result));
    }


    @Override
    public StoreSnapshot getSnapshot() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        int allocatedBufferCount = 0;
        int blockCount = 0;
        int totalBufferCount;
        synchronized (this.buffers) {
            for (Buffer b : this.buffers) {
                if (b != null) {
                    allocatedBufferCount++;
                    blockCount += b.getUsedBlockCount();
                }
            }

            totalBufferCount = this.buffers.length;
        }

        return new StoreSnapshot(
                this.storedBytes.get(),
                (long) (blockCount - allocatedBufferCount) * this.layout.blockSize(),
                (long) allocatedBufferCount * this.layout.blockSize(),
                (long) allocatedBufferCount * this.layout.bufferSize(),
                (long) totalBufferCount * this.layout.bufferSize());
    }

    //endregion

    //region Helpers

    private Buffer getBuffer(int bufferId, boolean mustExist) {
        synchronized (this.buffers) {
            Buffer b = this.buffers[bufferId];
            if (b == null && mustExist) {
                // Corrupted state
                throw new RuntimeException(new Exception("corruption"));
            }
            return b;
        }
    }

    private Buffer getNextAvailableBuffer() {
        synchronized (this.buffers) {
            while (!this.availableBufferIds.isEmpty()) {
                Buffer b = this.buffers[this.availableBufferIds.peekFirst()];
                assert b != null;
                if (b.hasCapacity()) {
                    return b;
                } else {
                    // Buffer full. Clean up.
                    this.availableBufferIds.removeFirst();
                }
            }
        }

        return null;
    }

    private Buffer allocateNewBuffer() {
        synchronized (this.buffers) {
            if (this.unallocatedBufferIds.isEmpty()) {
                throw new RuntimeException(new Exception("full")); // todo proper exception
            }

            int bufferId = this.unallocatedBufferIds.removeFirst();
            assert this.buffers[bufferId] == null;
            Buffer b = new Buffer(bufferId, size -> this.allocator.directBuffer(size, size), this.layout);
            this.buffers[bufferId] = b;
            this.availableBufferIds.add(bufferId);
            return b;
        }
    }

    //endregion
}

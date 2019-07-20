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
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@ThreadSafe
public class DirectMemoryStore implements AutoCloseable {
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
        Preconditions.checkArgument(maxSizeBytes % layout.bufferSize() == 0, "maxSizeBytes must be a multiple of %s.", layout.bufferSize());

        this.layout = layout;
        this.allocator = new UnpooledByteBufAllocator(true, true);
        this.buffers = new Buffer[(int) (maxSizeBytes / this.layout.bufferSize())];
        this.availableBufferIds = new ArrayDeque<>();
        this.unallocatedBufferIds = IntStream.range(0, this.buffers.length - 1).boxed().collect(Collectors.toCollection(ArrayDeque::new));
        this.storedBytes = new AtomicLong(0);
        this.closed = new AtomicBoolean(false);
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

    public int insert(BufferView data) throws IOException {
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

    public boolean delete(int firstBlockAddress) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        while (firstBlockAddress != StoreLayout.NO_ADDRESS) {
            int bufferId = this.layout.getBufferId(firstBlockAddress);
            int blockId = this.layout.getBlockId(firstBlockAddress);
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
            firstBlockAddress = result.getSuccessorAddress();
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

    public BufferView get(int firstBlockAddress) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        ArrayList<ByteBuf> readBuffers = new ArrayList<>();
        while (firstBlockAddress != StoreLayout.NO_ADDRESS) {
            int bufferId = this.layout.getBufferId(firstBlockAddress);
            int blockId = this.layout.getBlockId(firstBlockAddress);
            Buffer b;
            synchronized (this.buffers) {
                b = this.buffers[bufferId];
                if (b == null) {
                    if (readBuffers.isEmpty()) {
                        // Bad address.
                        return null;
                    } else {
                        // Corrupted state
                        throw new RuntimeException(new Exception("corruption"));
                    }
                }
            }

            firstBlockAddress = b.read(blockId, readBuffers);
        }

        ByteBuf[] result = readBuffers.stream().filter(ByteBuf::isReadable).toArray(i -> new ByteBuf[i]);
        return new ByteBufWrapper(Unpooled.wrappedBuffer(result));
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

    public boolean update(int id, BufferView data) {
        return false; // TODO: later
    }

    public boolean append(int id, int expectedLength, BufferView data) {
        return false; // TODO: later
    }

    public Snapshot getSnapshot() {
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

        return new Snapshot(
                this.storedBytes.get(),
                (long) (blockCount - allocatedBufferCount) * this.layout.blockSize(),
                (long) allocatedBufferCount * this.layout.blockSize(),
                (long) allocatedBufferCount * this.layout.bufferSize(),
                (long) totalBufferCount * this.layout.bufferSize());
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    @Getter
    public static class Snapshot {
        private final long storedBytes;
        private final long usedBytes;
        private final long reservedBytes;
        private final long allocatedBytes;
        private final long maxBytes;

        @Override
        public String toString() {
            return String.format("Stored = %d, Used = %d, Reserved = %d, Allocated = %d, Max = %d",
                    this.storedBytes, this.usedBytes, this.reservedBytes, this.allocatedBytes, this.maxBytes);
        }
    }
}

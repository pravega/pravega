package com.emc.logservice.storageabstraction.mocks;

import com.emc.logservice.common.Exceptions;
import com.emc.logservice.contracts.*;
import com.emc.logservice.storageabstraction.BadOffsetException;
import com.emc.logservice.storageabstraction.Storage;

import java.io.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * In-Memory mock for Storage. Contents is destroyed when object is garbage collected.
 */
public class InMemoryStorage implements Storage {
    private final HashMap<String, StreamSegmentData> streamSegments = new HashMap<>();
    private final Object lock = new Object();
    private boolean closed;

    //region AutoCloseable Implementation

    @Override
    public void close() {
        this.closed = true;
    }

    //endregion

    //region Storage Implementation

    @Override
    public CompletableFuture<SegmentProperties> create(String streamSegmentName, Duration timeout) {
        Exceptions.checkNotClosed(this.closed, this);
        return CompletableFuture.supplyAsync(() -> {
            synchronized (this.lock) {
                if (this.streamSegments.containsKey(streamSegmentName)) {
                    throw new CompletionException(new StreamSegmentExistsException(streamSegmentName));
                }
                StreamSegmentData data = new StreamSegmentData(streamSegmentName);
                this.streamSegments.put(streamSegmentName, data);
                return data;
            }
        }).thenCompose(StreamSegmentData::getInfo);
    }

    @Override
    public CompletableFuture<Void> write(String streamSegmentName, long offset, InputStream data, int length, Duration timeout) {
        return getStreamSegmentData(streamSegmentName)
                .thenCompose(ssd -> ssd.write(offset, data, length));
    }

    @Override
    public CompletableFuture<Integer> read(String streamSegmentName, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
        return getStreamSegmentData(streamSegmentName)
                .thenCompose(ssd -> ssd.read(offset, buffer, bufferOffset, length));
    }

    @Override
    public CompletableFuture<SegmentProperties> seal(String streamSegmentName, Duration timeout) {
        return getStreamSegmentData(streamSegmentName)
                .thenCompose(StreamSegmentData::markSealed);
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        return getStreamSegmentData(streamSegmentName)
                .thenCompose(StreamSegmentData::getInfo);
    }

    @Override
    public CompletableFuture<Void> concat(String targetStreamSegmentName, String sourceStreamSegmentName, Duration timeout) {
        CompletableFuture<StreamSegmentData> sourceData = getStreamSegmentData(sourceStreamSegmentName);
        CompletableFuture<StreamSegmentData> targetData = getStreamSegmentData(targetStreamSegmentName);
        return CompletableFuture.allOf(sourceData, targetData)
                                .thenCompose(v -> targetData.join().concat(sourceData.join()))
                                .thenCompose(v -> this.delete(sourceStreamSegmentName, timeout));
    }

    @Override
    public CompletableFuture<Void> delete(String streamSegmentName, Duration timeout) {
        Exceptions.checkNotClosed(this.closed, this);
        return CompletableFuture.runAsync(() -> {
            synchronized (this.lock) {
                if (!this.streamSegments.containsKey(streamSegmentName)) {
                    throw new CompletionException(new StreamSegmentNotExistsException(streamSegmentName));
                }
                this.streamSegments.remove(streamSegmentName);
            }
        });
    }

    private CompletableFuture<StreamSegmentData> getStreamSegmentData(String streamSegmentName) {
        Exceptions.checkNotClosed(this.closed, this);
        return CompletableFuture.supplyAsync(() -> {
            synchronized (this.lock) {
                StreamSegmentData data = this.streamSegments.getOrDefault(streamSegmentName, null);
                if (data == null) {
                    throw new CompletionException(new StreamSegmentNotExistsException(streamSegmentName));
                }

                return data;
            }
        });
    }

    //endregion

    //region StreamSegmentData

    private static class StreamSegmentData {
        private static final int BufferSize = 1024 * 1024;
        private final String name;
        private final ArrayList<byte[]> data;
        private final Object lock = new Object();
        private long length;
        private boolean sealed;

        public StreamSegmentData(String name) {
            this.name = name;
            this.data = new ArrayList<>();
            this.length = 0;
            this.sealed = false;
        }

        public CompletableFuture<Void> write(long startOffset, InputStream data, int length) {
            return CompletableFuture.runAsync(() -> {
                synchronized (this.lock) {
                    writeInternal(startOffset, data, length);
                }
            });
        }

        public CompletableFuture<Integer> read(long startOffset, byte[] target, int targetOffset, int length) {
            return CompletableFuture.supplyAsync(() -> {
                synchronized (this.lock) {
                    Exceptions.checkArrayRange(targetOffset, length, target.length, "targetOffset", "length");
                    Exceptions.checkArrayRange(startOffset, length, this.length, "startOffset", "length");

                    long offset = startOffset;
                    int readBytes = 0;
                    while (readBytes < length) {
                        int bufferSeq = getBufferSequence(offset);
                        int bufferOffset = getBufferOffset(offset);
                        int bytesToCopy = Math.min(BufferSize - bufferOffset, length - readBytes);
                        System.arraycopy(this.data.get(bufferSeq), bufferOffset, target, targetOffset + readBytes, bytesToCopy);

                        readBytes += bytesToCopy;
                        offset += bytesToCopy;
                    }

                    return readBytes;
                }
            });
        }

        public CompletableFuture<SegmentProperties> markSealed() {
            return CompletableFuture.supplyAsync(() -> {
                synchronized (this.lock) {
                    if (this.sealed) {
                        throw new CompletionException(new StreamSegmentSealedException(this.name));
                    }

                    this.sealed = true;
                    return new StreamSegmentInformation(this.name, this.length, this.sealed, false, new Date());
                }
            });
        }

        public CompletableFuture<Void> concat(StreamSegmentData other) {
            return CompletableFuture.runAsync(() -> {
                synchronized (other.lock) {
                    other.sealed = true; // Make sure other is sealed.
                    synchronized (this.lock) {
                        long bytesCopied = 0;
                        int currentBlockIndex = 0;
                        while (bytesCopied < other.length) {
                            byte[] currentBlock = other.data.get(currentBlockIndex);
                            int length = (int) Math.min(currentBlock.length, other.length - bytesCopied);
                            ByteArrayInputStream bis = new ByteArrayInputStream(currentBlock, 0, length);
                            writeInternal(this.length, bis, length);
                            bytesCopied += length;
                            currentBlockIndex++;
                        }
                    }
                }
            });
        }

        public CompletableFuture<SegmentProperties> getInfo() {
            return CompletableFuture.completedFuture(new StreamSegmentInformation(this.name, this.length, this.sealed, false, new Date())); //TODO: real modification time
        }

        private void ensureAllocated(long startOffset, int length) {
            long endOffset = startOffset + length;
            int desiredSize = getBufferSequence(endOffset) + 1;
            while (this.data.size() < desiredSize) {
                this.data.add(new byte[BufferSize]);
            }
        }

        private int getBufferSequence(long offset) {
            return (int) (offset / BufferSize);
        }

        private int getBufferOffset(long offset) {
            return (int) (offset % BufferSize);
        }

        private void writeInternal(long startOffset, InputStream data, int length) {
            Exceptions.checkArgument(length >= 0, "length", "bad length");
            if (startOffset != this.length) {
                throw new CompletionException(new BadOffsetException(String.format("Bad Offset. Expected %d.", this.length)));
            }

            if (this.sealed) {
                throw new CompletionException(new StreamSegmentSealedException(this.name));
            }

            long offset = startOffset;
            ensureAllocated(offset, length);

            try {
                int writtenBytes = 0;
                while (writtenBytes < length) {
                    int bufferSeq = getBufferSequence(offset);
                    int bufferOffset = getBufferOffset(offset);
                    int readBytes = data.read(this.data.get(bufferSeq), bufferOffset, BufferSize - bufferOffset);
                    if (readBytes < 0) {
                        throw new IOException("reached end of stream while still expecting data");
                    }
                    writtenBytes += readBytes;
                    offset += readBytes;
                }

                this.length = Math.max(this.length, startOffset + length);
            }
            catch (IOException exception) {
                throw new CompletionException(exception);
            }
        }
    }

    //endregion

    //region StreamSegmentInformation

    private static class StreamSegmentInformation implements SegmentProperties {
        private final long length;
        private final boolean sealed;
        private final boolean deleted;
        private final Date lastModified;
        private final String streamSegmentName;

        StreamSegmentInformation(String streamSegmentName, long length, boolean isSealed, boolean isDeleted, Date lastModified) {
            this.length = length;
            this.sealed = isSealed;
            this.deleted = isDeleted;
            this.lastModified = lastModified;
            this.streamSegmentName = streamSegmentName;
        }

        @Override
        public String getName() {
            return this.streamSegmentName;
        }

        @Override
        public boolean isSealed() {
            return this.sealed;
        }

        @Override
        public boolean isDeleted() {
            return this.deleted;
        }

        @Override
        public Date getLastModified() {
            return this.lastModified;
        }

        @Override
        public long getLength() {
            return this.length;
        }
    }

    //endregion
}

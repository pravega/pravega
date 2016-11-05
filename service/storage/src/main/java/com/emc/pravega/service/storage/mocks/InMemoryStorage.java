/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.storage.mocks;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.service.contracts.BadOffsetException;
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.contracts.StreamSegmentInformation;
import com.emc.pravega.service.contracts.StreamSegmentExistsException;
import com.emc.pravega.service.contracts.StreamSegmentSealedException;
import com.emc.pravega.service.contracts.StreamSegmentNotExistsException;

import com.emc.pravega.service.storage.Storage;
import com.google.common.base.Preconditions;

import javax.annotation.concurrent.GuardedBy;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executor;

/**
 * In-Memory mock for Storage. Contents is destroyed when object is garbage collected.
 */
public class InMemoryStorage implements Storage {
    //region Members

    private final HashMap<String, HashMap<Long, CompletableFuture<Void>>> offsetTriggers;
    private final HashMap<String, CompletableFuture<Void>> sealTriggers;
    @GuardedBy("lock")
    private final HashMap<String, StreamSegmentData> streamSegments = new HashMap<>();
    private final Object lock = new Object();
    private final ScheduledExecutorService executor;
    private boolean closed;
    private boolean ownsExecutorService;

    //endregion

    //region Constructor

    public InMemoryStorage() {
        this(Executors.newScheduledThreadPool(1));
        this.ownsExecutorService = true;
    }

    public InMemoryStorage(ScheduledExecutorService executor) {
        this.executor = executor;
        this.offsetTriggers = new HashMap<>();
        this.sealTriggers = new HashMap<>();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed) {
            if (this.ownsExecutorService) {
                this.executor.shutdown();
            }

            this.closed = true;
        }
    }

    //endregion

    //region Storage Implementation

    @Override
    public CompletableFuture<SegmentProperties> create(String streamSegmentName, Duration timeout) {
        Exceptions.checkNotClosed(this.closed, this);
        return CompletableFuture
                .supplyAsync(() -> {
                    synchronized (this.lock) {
                        if (this.streamSegments.containsKey(streamSegmentName)) {
                            throw new CompletionException(new StreamSegmentExistsException(streamSegmentName));
                        }

                        StreamSegmentData data = new StreamSegmentData(streamSegmentName, this.executor);
                        this.streamSegments.put(streamSegmentName, data);
                        return data;
                    }
                }, this.executor)
                .thenCompose(StreamSegmentData::getInfo);
    }

    @Override
    public CompletableFuture<Boolean> open(String streamSegmentName) {
        return CompletableFuture.completedFuture(Boolean.TRUE);
    }

    @Override
    public CompletableFuture<Void> write(String streamSegmentName, long offset, InputStream data, int length, Duration timeout) {
        CompletableFuture<Void> result = getStreamSegmentData(streamSegmentName)
                .thenCompose(ssd -> ssd.write(offset, data, length));
        result.thenRunAsync(() -> fireOffsetTriggers(streamSegmentName, offset + length), this.executor);
        return result;
    }

    @Override
    public CompletableFuture<Integer> read(String streamSegmentName, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
        return getStreamSegmentData(streamSegmentName)
                .thenCompose(ssd -> ssd.read(offset, buffer, bufferOffset, length));
    }

    @Override
    public CompletableFuture<SegmentProperties> seal(String streamSegmentName, Duration timeout) {
        CompletableFuture<SegmentProperties> result = getStreamSegmentData(streamSegmentName)
                .thenCompose(StreamSegmentData::markSealed);
        result.thenRunAsync(() -> fireSealTrigger(streamSegmentName));
        return result;
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        return getStreamSegmentData(streamSegmentName)
                .thenCompose(StreamSegmentData::getInfo);
    }

    @Override
    public CompletableFuture<Boolean> exists(String streamSegmentName, Duration timeout) {
        boolean exists;
        synchronized (this.lock) {
            exists = this.streamSegments.containsKey(streamSegmentName);
        }
        return CompletableFuture.completedFuture(exists);
    }

    @Override
    public CompletableFuture<Void> concat(String targetStreamSegmentName, long offset, String sourceStreamSegmentName,
            Duration timeout) {
        CompletableFuture<StreamSegmentData> sourceData = getStreamSegmentData(sourceStreamSegmentName);
        CompletableFuture<StreamSegmentData> targetData = getStreamSegmentData(targetStreamSegmentName);
        CompletableFuture<Void> result = CompletableFuture.allOf(sourceData, targetData)
                                                          .thenCompose(v -> targetData.join().concat(sourceData.join(), offset))
                                                          .thenCompose(v -> delete(sourceStreamSegmentName, timeout));
        result.thenRunAsync(() -> {
            fireOffsetTriggers(targetStreamSegmentName, targetData.join().getInfo().join().getLength());
            fireSealTrigger(sourceStreamSegmentName);
        }, this.executor);
        return result;
    }

    @Override
    public CompletableFuture<Void> delete(String streamSegmentName, Duration timeout) {
        Exceptions.checkNotClosed(this.closed, this);
        return CompletableFuture
                .runAsync(() -> {
                    synchronized (this.lock) {
                        if (!this.streamSegments.containsKey(streamSegmentName)) {
                            throw new CompletionException(new StreamSegmentNotExistsException(streamSegmentName));
                        }
                        this.streamSegments.remove(streamSegmentName);
                    }
                }, this.executor);
    }

    private CompletableFuture<StreamSegmentData> getStreamSegmentData(String streamSegmentName) {
        Exceptions.checkNotClosed(this.closed, this);
        return CompletableFuture
                .supplyAsync(() -> {
                    synchronized (this.lock) {
                        StreamSegmentData data = this.streamSegments.getOrDefault(streamSegmentName, null);
                        if (data == null) {
                            throw new CompletionException(new StreamSegmentNotExistsException(streamSegmentName));
                        }

                        return data;
                    }
                }, this.executor);
    }

    //endregion

    //region Size & seal triggers

    /**
     * Registers a size trigger for the given Segment Name and Offset.
     *
     * @param segmentName The Name of the Segment.
     * @param offset      The offset in the segment at which to trigger.
     * @param timeout     The timeout for the wait.
     * @return A CompletableFuture that will complete when the given Segment reaches at least the given minimum size.
     * This Future will fail with a TimeoutException if the Segment did not reach the minimum size within the given timeout.
     */
    public CompletableFuture<Void> registerSizeTrigger(String segmentName, long offset, Duration timeout) {
        CompletableFuture<Void> result;
        boolean newTrigger = false;
        synchronized (this.offsetTriggers) {
            HashMap<Long, CompletableFuture<Void>> segmentTriggers = this.offsetTriggers.getOrDefault(segmentName, null);
            if (segmentTriggers == null) {
                segmentTriggers = new HashMap<>();
                this.offsetTriggers.put(segmentName, segmentTriggers);
            }

            result = segmentTriggers.getOrDefault(offset, null);
            if (result == null) {
                result = createSizeTrigger(segmentName, offset, timeout);
                segmentTriggers.put(offset, result);
                newTrigger = true;
            }
        }

        if (newTrigger && !result.isDone()) {
            // Do the check now to see if we already exceed the trigger threshold.
            getStreamSegmentInfo(segmentName, timeout)
                    .thenAccept(sp -> {
                        // We already exceeded this offset.
                        if (sp.getLength() >= offset) {
                            fireOffsetTriggers(segmentName, sp.getLength());
                        }
                    });
        }

        return result;
    }

    /**
     * Registers a seal trigger for the given Segment Name.
     *
     * @param segmentName The Name of the Segment.
     * @param timeout     The timeout for the wait.
     * @return A CompletableFuture that will complete when the given Segment is sealed. This Future will fail with a TimeoutException
     * if the Segment was not sealed within the given timeout.
     */
    public CompletableFuture<Void> registerSealTrigger(String segmentName, Duration timeout) {
        CompletableFuture<Void> result;
        boolean newTrigger = false;
        synchronized (this.offsetTriggers) {
            result = this.sealTriggers.getOrDefault(segmentName, null);
            if (result == null) {
                result = createSealTrigger(segmentName, timeout);
                this.sealTriggers.put(segmentName, result);
                newTrigger = true;
            }
        }

        if (newTrigger && !result.isDone()) {
            // Do the check now to see if we are already sealed.
            getStreamSegmentInfo(segmentName, timeout)
                    .thenAccept(sp -> {
                        if (sp.isSealed()) {
                            fireSealTrigger(segmentName);
                        }
                    });
        }

        return result;
    }

    private void fireOffsetTriggers(String segmentName, long currentOffset) {
        HashMap<Long, CompletableFuture<Void>> toTrigger = new HashMap<>();
        synchronized (this.offsetTriggers) {
            HashMap<Long, CompletableFuture<Void>> segmentTriggers = this.offsetTriggers.getOrDefault(segmentName, null);
            if (segmentTriggers != null) {
                segmentTriggers.entrySet().forEach(e -> {
                    if (e.getKey() <= currentOffset) {
                        toTrigger.put(e.getKey(), e.getValue());
                    }
                });
            }
        }

        toTrigger.values().forEach(c -> c.complete(null));
    }

    private void fireSealTrigger(String segmentName) {
        CompletableFuture<Void> toTrigger;
        synchronized (this.sealTriggers) {
            toTrigger = this.sealTriggers.getOrDefault(segmentName, null);
        }

        if (toTrigger != null) {
            toTrigger.complete(null);
        }
    }

    private CompletableFuture<Void> createSizeTrigger(String segmentName, long minSize, Duration timeout) {
        CompletableFuture<Void> result = FutureHelpers.futureWithTimeout(timeout, segmentName, this.executor);
        result.whenComplete((r, ex) -> {
            synchronized (this.offsetTriggers) {
                HashMap<Long, CompletableFuture<Void>> segmentTriggers = this.offsetTriggers.getOrDefault(segmentName, null);
                if (segmentTriggers != null) {
                    segmentTriggers.remove(minSize);

                    if (segmentTriggers.size() == 0) {
                        this.offsetTriggers.remove(segmentName);
                    }
                }
            }
        });

        return result;
    }

    private CompletableFuture<Void> createSealTrigger(String segmentName, Duration timeout) {
        CompletableFuture<Void> result = FutureHelpers.futureWithTimeout(timeout, segmentName, this.executor);
        result.whenComplete((r, ex) -> {
            synchronized (this.sealTriggers) {
                this.sealTriggers.remove(segmentName);
            }
        });

        return result;
    }

    //endregion

    //region StreamSegmentData

    private static class StreamSegmentData {
        private static final int BUFFER_SIZE = 1024 * 1024;
        private final String name;
        private final ArrayList<byte[]> data;
        private final Object lock = new Object();
        private final Executor executor;
        private long length;
        private boolean sealed;

        StreamSegmentData(String name, Executor executor) {
            this.name = name;
            this.data = new ArrayList<>();
            this.length = 0;
            this.sealed = false;
            this.executor = executor;
        }

        CompletableFuture<Void> write(long startOffset, InputStream data, int length) {
            return CompletableFuture.runAsync(() -> {
                synchronized (this.lock) {
                    writeInternal(startOffset, data, length);
                }
            }, this.executor);
        }

        CompletableFuture<Integer> read(long startOffset, byte[] target, int targetOffset, int length) {
            return CompletableFuture.supplyAsync(() -> {
                synchronized (this.lock) {
                    Exceptions.checkArrayRange(targetOffset, length, target.length, "targetOffset", "length");
                    Exceptions.checkArrayRange(startOffset, length, this.length, "startOffset", "length");

                    long offset = startOffset;
                    int readBytes = 0;
                    while (readBytes < length) {
                        int bufferSeq = getBufferSequence(offset);
                        int bufferOffset = getBufferOffset(offset);
                        int bytesToCopy = Math.min(BUFFER_SIZE - bufferOffset, length - readBytes);
                        System.arraycopy(this.data.get(bufferSeq), bufferOffset, target, targetOffset + readBytes, bytesToCopy);

                        readBytes += bytesToCopy;
                        offset += bytesToCopy;
                    }

                    return readBytes;
                }
            }, this.executor);
        }

        CompletableFuture<SegmentProperties> markSealed() {
            return CompletableFuture.supplyAsync(() -> {
                synchronized (this.lock) {
                    if (this.sealed) {
                        throw new CompletionException(new StreamSegmentSealedException(this.name));
                    }

                    this.sealed = true;
                    return new StreamSegmentInformation(this.name, this.length, this.sealed, false, new Date());
                }
            }, this.executor);
        }

        CompletableFuture<Void> concat(StreamSegmentData other, long offset) {
            return CompletableFuture.runAsync(() -> {
                synchronized (other.lock) {
                    Preconditions.checkState(other.sealed, "Cannot concat segment '%s' into '%s' because it is not sealed.", other.name, this.name);
                    synchronized (this.lock) {
                        if (offset != this.length) {
                            throw new CompletionException(new BadOffsetException(this.name, this.length, offset));
                        }
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
            }, this.executor);
        }

        CompletableFuture<SegmentProperties> getInfo() {
            synchronized (this.lock) {
                return CompletableFuture.completedFuture(new StreamSegmentInformation(this.name, this.length, this.sealed, false, new Date()));
            }
        }

        private void ensureAllocated(long startOffset, int length) {
            long endOffset = startOffset + length;
            int desiredSize = getBufferSequence(endOffset) + 1;
            while (this.data.size() < desiredSize) {
                this.data.add(new byte[BUFFER_SIZE]);
            }
        }

        private int getBufferSequence(long offset) {
            return (int) (offset / BUFFER_SIZE);
        }

        private int getBufferOffset(long offset) {
            return (int) (offset % BUFFER_SIZE);
        }

        private void writeInternal(long startOffset, InputStream data, int length) {
            Exceptions.checkArgument(length >= 0, "length", "bad length");
            if (startOffset != this.length) {
                throw new CompletionException(new BadOffsetException(this.name, this.length, startOffset));
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
                    int readBytes = data.read(this.data.get(bufferSeq), bufferOffset, BUFFER_SIZE - bufferOffset);
                    if (readBytes < 0) {
                        throw new IOException("reached end of stream while still expecting data");
                    }
                    writtenBytes += readBytes;
                    offset += readBytes;
                }

                this.length = Math.max(this.length, startOffset + length);
            } catch (IOException exception) {
                throw new CompletionException(exception);
            }
        }

        @Override
        public String toString() {
            return String.format("%s: Length = %d, Sealed = %s", this.name, this.length, this.sealed);
        }
    }

    //endregion

}
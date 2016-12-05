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
import com.emc.pravega.service.contracts.StreamSegmentExistsException;
import com.emc.pravega.service.contracts.StreamSegmentInformation;
import com.emc.pravega.service.contracts.StreamSegmentNotExistsException;
import com.emc.pravega.service.contracts.StreamSegmentSealedException;
import com.emc.pravega.service.storage.Storage;
import com.google.common.base.Preconditions;
import lombok.Data;

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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

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
    private final AtomicLong currentOwnerId;
    private final SyncContext syncContext;
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
        this.currentOwnerId = new AtomicLong(0);
        this.syncContext = new SyncContext(this.currentOwnerId::get);
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

                        StreamSegmentData data = new StreamSegmentData(streamSegmentName, this.syncContext);
                        data.open();
                        this.streamSegments.put(streamSegmentName, data);
                        return data;
                    }
                }, this.executor)
                .thenApply(StreamSegmentData::getInfo);
    }

    @Override
    public CompletableFuture<Void> open(String streamSegmentName) {
        return getStreamSegmentData(streamSegmentName)
                .thenAccept(StreamSegmentData::open);
    }

    @Override
    public CompletableFuture<Void> write(String streamSegmentName, long offset, InputStream data, int length, Duration timeout) {
        CompletableFuture<Void> result = getStreamSegmentData(streamSegmentName)
                .thenAccept(ssd -> ssd.write(offset, data, length));
        result.thenRunAsync(() -> fireOffsetTriggers(streamSegmentName, offset + length), this.executor);
        return result;
    }

    @Override
    public CompletableFuture<Integer> read(String streamSegmentName, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
        return getStreamSegmentData(streamSegmentName)
                .thenApply(ssd -> ssd.read(offset, buffer, bufferOffset, length));
    }

    @Override
    public CompletableFuture<SegmentProperties> seal(String streamSegmentName, Duration timeout) {
        CompletableFuture<SegmentProperties> result = getStreamSegmentData(streamSegmentName)
                .thenApply(StreamSegmentData::markSealed);
        result.thenRunAsync(() -> fireSealTrigger(streamSegmentName), this.executor);
        return result;
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        return getStreamSegmentData(streamSegmentName)
                .thenApply(StreamSegmentData::getInfo);
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
                                                          .thenAccept(v -> targetData.join().concat(sourceData.join(), offset))
                                                          .thenCompose(v -> delete(sourceStreamSegmentName, timeout));
        result.thenRunAsync(() -> {
            fireOffsetTriggers(targetStreamSegmentName, targetData.join().getInfo().getLength());
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

    /**
     * Changes the current owner of the Storage Adapter. After calling this, all calls to existing Segments will fail
     * until open() is called again on them.
     */
    public void changeOwner() {
        this.currentOwnerId.incrementAndGet();
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
        private final SyncContext context;
        private long currentOwnerId;
        private long length;
        private boolean sealed;

        StreamSegmentData(String name, SyncContext context) {
            this.name = name;
            this.data = new ArrayList<>();
            this.length = 0;
            this.sealed = false;
            this.context = context;
            this.currentOwnerId = Long.MIN_VALUE;
        }

        void open() {
            synchronized (this.lock) {
                // Get the current InMemoryStorageAdapter owner id and keep track of it; it will be used for validation.
                this.currentOwnerId = this.context.getCurrentOwnerId.get();
            }
        }

        void write(long startOffset, InputStream data, int length) {
            synchronized (this.lock) {
                checkOpened();
                writeInternal(startOffset, data, length);
            }
        }

        int read(long startOffset, byte[] target, int targetOffset, int length) {
            synchronized (this.lock) {
                Exceptions.checkArrayRange(targetOffset, length, target.length, "targetOffset", "length");
                Exceptions.checkArrayRange(startOffset, length, this.length, "startOffset", "length");
                checkOpened();

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
        }

        SegmentProperties markSealed() {
            synchronized (this.lock) {
                checkOpened();
                this.sealed = true;
                return new StreamSegmentInformation(this.name, this.length, this.sealed, false, new Date());
            }
        }

        void concat(StreamSegmentData other, long offset) {
            synchronized (this.context.syncRoot) {
                // In order to do a proper concat, we need to lock on both the source and the target segments. But since
                // there's always a possibility of two concurrent calls to concat with swapped arguments, there is a chance
                // this could deadlock in certain scenarios. One way to avoid that is to ensure that only one call to concat()
                // can be in progress at any given time (for any instance of InMemoryStorage), thus the need to synchronize
                // on SyncContext.syncRoot.
                synchronized (other.lock) {
                    Preconditions.checkState(other.sealed, "Cannot concat segment '%s' into '%s' because it is not sealed.", other.name, this.name);
                    other.checkOpened();
                    synchronized (this.lock) {
                        checkOpened();
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
            }
        }

        SegmentProperties getInfo() {
            synchronized (this.lock) {
                checkOpened();
                return new StreamSegmentInformation(this.name, this.length, this.sealed, false, new Date());
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

        private void checkOpened() {
            Preconditions.checkState(this.currentOwnerId == this.context.getCurrentOwnerId.get(), "StreamSegment '%s' is not open by the current owner.", this.name);
        }

        @Override
        public String toString() {
            return String.format("%s: Length = %d, Sealed = %s", this.name, this.length, this.sealed);
        }
    }

    @Data
    private static class SyncContext {
        final Supplier<Long> getCurrentOwnerId;
        final Object syncRoot = new Object();
    }

    //endregion
}
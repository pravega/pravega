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
package io.pravega.segmentstore.server.writer;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.pravega.common.Exceptions;
import io.pravega.common.function.Callbacks;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.logs.InMemoryLog;
import io.pravega.segmentstore.server.logs.operations.MetadataCheckpointOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentAppendOperation;
import io.pravega.segmentstore.storage.LogAddress;
import io.pravega.test.common.ErrorInjector;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.val;

/**
 * Test version of a WriterDataSource that can accumulate operations in memory (just like the real DurableLog) and only
 * depends on a metadata and a cache as external dependencies.
 * <p>
 * Note that even though it uses an UpdateableContainerMetadata, no changes to this metadata are performed (except recording truncation markers & Sequence Numbers).
 * All other changes (Segment-based) must be done externally.
 */
@ThreadSafe
class TestWriterDataSource implements WriterDataSource, AutoCloseable {
    //region Members

    private final UpdateableContainerMetadata metadata;
    private final InMemoryLog log;
    private final ScheduledExecutorService executor;
    private final DataSourceConfig config;
    @GuardedBy("lock")
    private final HashMap<Long, AppendData> appendData;
    @GuardedBy("lock")
    private final HashMap<Long, Map<AttributeId, Long>> attributeData;
    @GuardedBy("lock")
    private final HashMap<Long, Long> attributeRootPointers;
    @GuardedBy("lock")
    private CompletableFuture<Void> waitFullyAcked;
    @GuardedBy("lock")
    private long ackSeqNo;
    @GuardedBy("lock")
    private CompletableFuture<Void> addProcessed;
    private final AtomicLong lastAddedCheckpoint;
    private final AtomicBoolean ackEffective;
    private final AtomicBoolean closed;
    private final AtomicLong lastAddedSeqNo;
    @GuardedBy("lock")
    private Consumer<Long> segmentMetadataRequested;
    @GuardedBy("lock")
    private ErrorInjector<Exception> readSyncErrorInjector;
    @GuardedBy("lock")
    private ErrorInjector<Exception> readAsyncErrorInjector;
    @GuardedBy("lock")
    private ErrorInjector<Exception> ackSyncErrorInjector;
    @GuardedBy("lock")
    private ErrorInjector<Exception> ackAsyncErrorInjector;
    @GuardedBy("lock")
    private ErrorInjector<Exception> getAppendDataErrorInjector;
    @GuardedBy("lock")
    private ErrorInjector<Exception> persistAttributesErrorInjector;
    /**
     * Arg1: Root Pointer
     * Arg2: LastSeqNo
     * Return: True (validate root pointer), False (do not validate).
     */
    @GuardedBy("lock")
    private BiFunction<Long, Long, CompletableFuture<Boolean>> notifyAttributesPersistedInterceptor;
    @GuardedBy("lock")
    private BiConsumer<Long, Long> completeMergeCallback;
    private final Object lock = new Object();

    //endregion

    //region Constructor

    TestWriterDataSource(UpdateableContainerMetadata metadata, ScheduledExecutorService executor, DataSourceConfig config) {
        Preconditions.checkNotNull(metadata, "metadata");
        Preconditions.checkNotNull(executor, "executor");
        Preconditions.checkNotNull(config, "config");

        this.metadata = metadata;
        this.executor = executor;
        this.config = config;
        this.appendData = new HashMap<>();
        this.attributeRootPointers = new HashMap<>();
        this.attributeData = new HashMap<>();
        this.log = new InMemoryLog();
        this.lastAddedCheckpoint = new AtomicLong(0);
        this.lastAddedSeqNo = new AtomicLong(0);
        this.ackSeqNo = 0;
        this.waitFullyAcked = null;
        this.ackEffective = new AtomicBoolean(true);
        this.closed = new AtomicBoolean(false);
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            // Cancel any pending adds.
            CompletableFuture<Void> addProcessed;
            synchronized (this.lock) {
                addProcessed = this.addProcessed;
                this.addProcessed = null;
            }

            if (addProcessed != null) {
                addProcessed.cancel(true);
            }
        }
    }

    //endregion

    //region add

    public long add(Operation operation) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkArgument(operation.getSequenceNumber() < 0, "Given operation already has a sequence number.");

        // If not a checkpoint op, see if we need to auto-add one.
        boolean isCheckpoint = operation instanceof MetadataCheckpointOperation;
        if (!isCheckpoint) {
            if (this.config.autoInsertCheckpointFrequency != DataSourceConfig.NO_METADATA_CHECKPOINT
                    && this.metadata.getOperationSequenceNumber() - this.lastAddedCheckpoint.get() >= this.config.autoInsertCheckpointFrequency) {
                MetadataCheckpointOperation checkpointOperation = new MetadataCheckpointOperation();
                this.lastAddedCheckpoint.set(add(checkpointOperation));
            }
        }

        // Set the Sequence Number, after the possible recursive call to add a checkpoint (to maintain Seq No order).
        operation.setSequenceNumber(this.metadata.nextOperationSequenceNumber());

        // We need to record the Truncation Marker/Point prior to actually adding the operation to the log (because it could
        // get picked up very fast by the Writer, so we need to have everything in place).
        if (isCheckpoint) {
            this.metadata.recordTruncationMarker(operation.getSequenceNumber(), new TestLogAddress(operation.getSequenceNumber()));
            this.metadata.setValidTruncationPoint(operation.getSequenceNumber());
        }

        this.log.add(operation);
        this.lastAddedSeqNo.set(operation.getSequenceNumber());
        notifyAddProcessed();
        return operation.getSequenceNumber();
    }

    /**
     * Records data for appends (to be fetched with getAppendData()).
     */
    void recordAppend(StreamSegmentAppendOperation operation) {
        AppendData ad;
        synchronized (this.lock) {
            ad = this.appendData.getOrDefault(operation.getStreamSegmentId(), null);
            if (ad == null) {
                ad = new AppendData();
                this.appendData.put(operation.getStreamSegmentId(), ad);
            }
        }

        ad.append(operation.getStreamSegmentOffset(), operation.getData().getCopy());
    }

    /**
     * Clears all append data.
     */
    void clearAppendData() {
        synchronized (this.lock) {
            this.appendData.clear();
        }
    }

    //endregion

    //region WriterDataSource Implementation

    @Override
    public int getId() {
        return this.metadata.getContainerId();
    }

    @Override
    public CompletableFuture<Void> acknowledge(long upToSequenceNumber, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkArgument(this.metadata.isValidTruncationPoint(upToSequenceNumber), "Invalid Truncation Point. Must refer to a MetadataCheckpointOperation.");
        ErrorInjector<Exception> asyncErrorInjector;
        synchronized (this.lock) {
            ErrorInjector.throwSyncExceptionIfNeeded(this.ackSyncErrorInjector);
            asyncErrorInjector = this.ackAsyncErrorInjector;
        }

        return ErrorInjector
                .throwAsyncExceptionIfNeeded(asyncErrorInjector, () -> CompletableFuture.runAsync(() -> {
                    if (this.ackEffective.get()) {
                        // ackEffective determines whether the ack operation has any effect or not.
                        this.metadata.removeTruncationMarkers(upToSequenceNumber);
                    }

                    // See if anyone is waiting for the DataSource to be emptied out; if so, notify them.
                    CompletableFuture<Void> callback = null;
                    synchronized (this.lock) {
                        this.ackSeqNo = upToSequenceNumber;

                        // If someone is waiting for a full ack, check to see if we should notify them.
                        if (this.waitFullyAcked != null && this.lastAddedSeqNo.get() > 0 && this.lastAddedSeqNo.get() <= this.ackSeqNo) {
                            callback = this.waitFullyAcked;
                            this.waitFullyAcked = null;
                        }
                    }

                    if (callback != null) {
                        callback.complete(null);
                    }
                }, this.executor));
    }

    @Override
    public CompletableFuture<Long> persistAttributes(long streamSegmentId, Map<AttributeId, Long> attributes, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        ErrorInjector<Exception> asyncErrorInjector;
        synchronized (this.lock) {
            asyncErrorInjector = this.persistAttributesErrorInjector;
        }

        return ErrorInjector
                .throwAsyncExceptionIfNeeded(asyncErrorInjector, () -> CompletableFuture.supplyAsync(() -> {
                    synchronized (this.lock) {
                        // We use "null" as an indication that the attribute data is deleted, hence the extra work here.
                        Map<AttributeId, Long> segmentAttributes;
                        if (this.attributeData.containsKey(streamSegmentId)) {
                            segmentAttributes = this.attributeData.get(streamSegmentId);
                        } else {
                            segmentAttributes = new HashMap<>();
                            this.attributeData.put(streamSegmentId, segmentAttributes);
                        }

                        if (segmentAttributes == null) {
                            throw new CompletionException(new StreamSegmentNotExistsException(Long.toString(streamSegmentId)));
                        }
                        try {
                            for (val e : attributes.entrySet()) {
                                if (e.getValue() == Attributes.NULL_ATTRIBUTE_VALUE) {
                                    segmentAttributes.remove(e.getKey());
                                } else {
                                    segmentAttributes.put(e.getKey(), e.getValue());
                                }
                            }
                        } catch (UnsupportedOperationException ex) {
                            // This is turned into an UnmodifiableMap, which throws UnsupportedOperationException for modify calls.
                            throw new CompletionException(new StreamSegmentSealedException("attributes_" + streamSegmentId, ex));
                        }

                        long rootPointer = this.attributeRootPointers.getOrDefault(streamSegmentId, 0L) + 1;
                        this.attributeRootPointers.put(streamSegmentId, rootPointer);
                        return rootPointer;
                    }
                }, this.executor));
    }

    @Override
    public CompletableFuture<Void> notifyAttributesPersisted(long segmentId, SegmentType segmentType, long rootPointer, long lastSequenceNumber, Duration timeout) {
        BiFunction<Long, Long, CompletableFuture<Boolean>> interceptor;
        synchronized (this.lock) {
            interceptor = this.notifyAttributesPersistedInterceptor;
        }

        if (interceptor == null) {
            return CompletableFuture.runAsync(() -> notifyAttributesPersisted(segmentId, segmentType, rootPointer, lastSequenceNumber, true), this.executor);
        } else {
            return interceptor.apply(rootPointer, lastSequenceNumber)
                    .thenAcceptAsync(validate -> notifyAttributesPersisted(segmentId, segmentType, rootPointer, lastSequenceNumber, validate), this.executor);
        }
    }

    private void notifyAttributesPersisted(long segmentId, SegmentType segmentType, long rootPointer, long lastSequenceNumber, boolean validateRootPointer) {
        Preconditions.checkArgument(segmentType.equals(this.metadata.getStreamSegmentMetadata(segmentId).getType()));
        synchronized (this.lock) {
            Long expectedRootPointer = this.attributeRootPointers.getOrDefault(segmentId, Long.MIN_VALUE);
            if (!validateRootPointer || expectedRootPointer == rootPointer) {
                this.metadata.getStreamSegmentMetadata(segmentId).updateAttributes(
                        ImmutableMap.<AttributeId, Long>builder()
                                .put(Attributes.ATTRIBUTE_SEGMENT_ROOT_POINTER, rootPointer)
                                .put(Attributes.ATTRIBUTE_SEGMENT_PERSIST_SEQ_NO, lastSequenceNumber)
                                .build());
            } else {
                throw new AssertionError(String.format("Root pointer mismatch. Expected %s, Given %s.", expectedRootPointer, rootPointer));
            }
        }
    }

    @Override
    public CompletableFuture<Void> sealAttributes(long streamSegmentId, Duration timeout) {
        return CompletableFuture.runAsync(() -> {
            synchronized (this.lock) {
                Map<AttributeId, Long> segmentAttributes = this.attributeData.computeIfAbsent(streamSegmentId, k -> new HashMap<>());
                this.attributeData.put(streamSegmentId, Collections.unmodifiableMap(segmentAttributes));
            }
        }, this.executor);
    }

    @Override
    public CompletableFuture<Void> deleteAllAttributes(SegmentMetadata segmentMetadata, Duration timeout) {
        return CompletableFuture.runAsync(() -> {
            synchronized (this.lock) {
                this.attributeData.put(segmentMetadata.getId(), null);
            }
        }, this.executor);
    }

    @Override
    public CompletableFuture<Queue<Operation>> read(int maxCount, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        ErrorInjector<Exception> asyncErrorInjector;
        synchronized (this.lock) {
            ErrorInjector.throwSyncExceptionIfNeeded(this.readSyncErrorInjector);
            asyncErrorInjector = this.readAsyncErrorInjector;
        }

        return ErrorInjector
                .throwAsyncExceptionIfNeeded(asyncErrorInjector, () -> this.log.take(maxCount, timeout, this.executor));
    }

    @Override
    public void completeMerge(long targetStreamSegmentId, long sourceStreamSegmentId) {
        val callback = getCompleteMergeCallback();
        if (callback != null) {
            callback.accept(targetStreamSegmentId, sourceStreamSegmentId);
        }
    }

    @Override
    public BufferView getAppendData(long streamSegmentId, long startOffset, int length) {
        AppendData ad;
        synchronized (this.lock) {
            ErrorInjector.throwSyncExceptionIfNeeded(this.getAppendDataErrorInjector);

            // Perform the same validation checks as the ReadIndex would do.
            SegmentMetadata sm = this.metadata.getStreamSegmentMetadata(streamSegmentId);
            if (sm.isDeleted()) {
                // StorageWriterFactory.WriterDataSource returns null for inexistent segments.
                return null;
            }

            Preconditions.checkArgument(length >= 0, "length must be a non-negative number");
            Preconditions.checkArgument(startOffset >= sm.getStorageLength(),
                    "startOffset (%s) must refer to an offset beyond the Segment's StorageLength offset(%s).", startOffset, sm.getStorageLength());
            Preconditions.checkArgument(startOffset + length <= sm.getLength(),
                    "startOffset+length must be less than the length of the Segment.");
            Preconditions.checkArgument(startOffset >= Math.min(sm.getStartOffset(), sm.getStorageLength()),
                    "startOffset is before the Segment's StartOffset.");

            ad = this.appendData.getOrDefault(streamSegmentId, null);
        }

        if (ad == null) {
            return null;
        }

        return ad.read(startOffset, length);
    }

    @Override
    public boolean isValidTruncationPoint(long operationSequenceNumber) {
        return this.metadata.isValidTruncationPoint(operationSequenceNumber);
    }

    @Override
    public long getClosestValidTruncationPoint(long operationSequenceNumber) {
        return this.metadata.getClosestValidTruncationPoint(operationSequenceNumber);
    }

    @Override
    public UpdateableSegmentMetadata getStreamSegmentMetadata(long streamSegmentId) {
        Consumer<Long> callback;
        synchronized (this.lock) {
            callback = this.segmentMetadataRequested;
        }

        if (callback != null) {
            Callbacks.invokeSafely(callback, streamSegmentId, null);
        }

        return this.metadata.getStreamSegmentMetadata(streamSegmentId);
    }

    //endregion

    //region Other Properties

    long getAttributeRootPointer(long segmentId) {
        synchronized (this.lock) {
            return this.attributeRootPointers.getOrDefault(segmentId, Attributes.NULL_ATTRIBUTE_VALUE);
        }
    }

    void setSegmentMetadataRequested(Consumer<Long> callback) {
        synchronized (this.lock) {
            this.segmentMetadataRequested = callback;
        }
    }

    void setPersistAttributesErrorInjector(ErrorInjector<Exception> injector) {
        synchronized (this.lock) {
            this.persistAttributesErrorInjector = injector;
        }
    }

    void setReadSyncErrorInjector(ErrorInjector<Exception> injector) {
        synchronized (this.lock) {
            this.readSyncErrorInjector = injector;
        }
    }

    void setReadAsyncErrorInjector(ErrorInjector<Exception> injector) {
        synchronized (this.lock) {
            this.readAsyncErrorInjector = injector;
        }
    }

    void setAckSyncErrorInjector(ErrorInjector<Exception> injector) {
        synchronized (this.lock) {
            this.ackSyncErrorInjector = injector;
        }
    }

    void setAckAsyncErrorInjector(ErrorInjector<Exception> injector) {
        synchronized (this.lock) {
            this.ackAsyncErrorInjector = injector;
        }
    }

    void setGetAppendDataErrorInjector(ErrorInjector<Exception> injector) {
        synchronized (this.lock) {
            this.getAppendDataErrorInjector = injector;
        }
    }

    void setCompleteMergeCallback(BiConsumer<Long, Long> callback) {
        synchronized (this.lock) {
            this.completeMergeCallback = callback;
        }
    }

    void setNotifyAttributesPersistedInterceptor(BiFunction<Long, Long, CompletableFuture<Boolean>> interceptor) {
        synchronized (this.lock) {
            this.notifyAttributesPersistedInterceptor = interceptor;
        }
    }

    BiConsumer<Long, Long> getCompleteMergeCallback() {
        synchronized (this.lock) {
            return this.completeMergeCallback;
        }
    }

    /**
     * Sets whether the acknowledgements have any effect of actually truncating the inner log.
     */
    void setAckEffective(boolean value) {
        this.ackEffective.set(value);
    }

    /**
     * Returns a CompletableFuture that will be completed when the TestWriterDataSource becomes empty.
     */
    CompletableFuture<Void> waitFullyAcked() {
        synchronized (this.lock) {
            if (this.waitFullyAcked == null) {
                // Nobody else is waiting for the DataSource to empty out.
                if (this.ackSeqNo >= this.lastAddedSeqNo.get()) {
                    // We are already empty; return a completed future.
                    return CompletableFuture.completedFuture(null);
                } else {
                    // Not empty yet; create an uncompleted Future and store it.
                    this.waitFullyAcked = new CompletableFuture<>();
                }
            }

            return this.waitFullyAcked;
        }
    }

    /**
     * Gets a copy of all the attributes so far.
     */
    Map<AttributeId, Long> getPersistedAttributes(long segmentId) {
        synchronized (this.lock) {
            val m = this.attributeData.get(segmentId);
            return m == null ? Collections.emptyMap() : Collections.unmodifiableMap(new HashMap<>(m));
        }
    }

    //endregion

    //region Helpers

    private void notifyAddProcessed() {
        CompletableFuture<Void> f;
        synchronized (this.lock) {
            f = this.addProcessed;
            this.addProcessed = null;
        }

        if (f != null) {
            f.complete(null);
        }
    }

    //endregion

    static class DataSourceConfig {
        static final int NO_METADATA_CHECKPOINT = -1;
        int autoInsertCheckpointFrequency;
    }

    private static class TestLogAddress extends LogAddress {
        TestLogAddress(long sequence) {
            super(sequence);
        }
    }

    @ThreadSafe
    private static class AppendData {
        @GuardedBy("this")
        private final TreeMap<Long, byte[]> data = new TreeMap<>();

        synchronized void append(long segmentOffset, byte[] data) {
            this.data.put(segmentOffset, data);
        }

        synchronized BufferView read(final long segmentOffset, final int length) {
            ArrayList<BufferView> result = new ArrayList<>();

            // Locate first entry.
            long currentOffset = segmentOffset;
            Map.Entry<Long, byte[]> entry = this.data.floorEntry(currentOffset);
            if (entry == null || entry.getKey() + entry.getValue().length <= currentOffset) {
                // Requested offset is before first entry or in a "gap".
                return null;
            }

            int entryOffset = (int) (currentOffset - entry.getKey());
            byte[] entryData = entry.getValue();
            int remainingLength = length;
            while (entryData != null && remainingLength > 0) {
                int entryLength = Math.min(remainingLength, entryData.length - entryOffset);
                result.add(new ByteArraySegment(entryData, entryOffset, entryLength));
                currentOffset += entryLength;
                remainingLength -= entryLength;
                entryOffset = 0;
                entryData = this.data.get(currentOffset);
            }

            if (remainingLength > 0) {
                return null;
            }

            return BufferView.wrap(result);
        }
    }
}


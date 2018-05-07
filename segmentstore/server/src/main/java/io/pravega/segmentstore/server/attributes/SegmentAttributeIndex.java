/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.attributes;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.pravega.common.Exceptions;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.Retry;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.contracts.ReadResultEntryType;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.contracts.StreamSegmentTruncatedException;
import io.pravega.segmentstore.server.AttributeIndex;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.OperationLog;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.logs.operations.UpdateAttributesOperation;
import io.pravega.segmentstore.server.reading.AsyncReadResultHandler;
import io.pravega.segmentstore.server.reading.AsyncReadResultProcessor;
import io.pravega.segmentstore.server.reading.StreamSegmentStorageReader;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * Attribute Index for a single Segment.
 */
@Slf4j
class SegmentAttributeIndex implements AttributeIndex {
    //region Members

    /**
     * For Attribute Segment Appends, we want to write conditionally based on the offset, and retry the operation if
     * it failed for that reason. That guarantees that we won't be losing any data if we get concurrent calls to put().
     */
    private static final Retry.RetryAndThrowBase<Exception> APPEND_RETRY = Retry
            .withExpBackoff(10, 2, 10, 1000)
            .retryingOn(BadOffsetException.class)
            .throwingOn(Exception.class);

    /**
     * Calls to get() and put() can execute concurrently, which means we can have concurrent reads and writes from/to the
     * Attribute Segment, which in turn means we can truncate the segment while reading from it. We need to retry reads
     * if we stumble upon a segment truncation.
     */
    private static final Retry.RetryAndThrowBase<Exception> READ_RETRY = Retry
            .withExpBackoff(10, 2, 10, 1000)
            .retryingOn(StreamSegmentTruncatedException.class)
            .throwingOn(Exception.class);

    private final SegmentMetadata segmentMetadata;
    private final AtomicReference<AttributeSegment> attributeSegment;
    private final Storage storage;
    private final OperationLog operationLog;
    private final AttributeIndexConfig config;
    private final ScheduledExecutorService executor;
    private final String traceObjectId;

    //endregion

    //region Constructor & Initialization

    /**
     * Creates a new instance of the SegmentAttributeIndex class.
     *
     * @param segmentMetadata The SegmentMetadata of the Segment whose attributes we want to manage.
     * @param storage         A Storage adapter which can be used to access the Attribute Segment.
     * @param operationLog    An OperationLog that can be used to atomically update attributes for the main Segment.
     * @param config          Attribute Index Configuration.
     * @param executor        An Executor to run async tasks.
     */
    SegmentAttributeIndex(SegmentMetadata segmentMetadata, Storage storage, OperationLog operationLog, AttributeIndexConfig config, ScheduledExecutorService executor) {
        this.segmentMetadata = Preconditions.checkNotNull(segmentMetadata, "segmentMetadata");
        this.storage = Preconditions.checkNotNull(storage, "storage");
        this.operationLog = Preconditions.checkNotNull(operationLog, "operationLog");
        this.config = Preconditions.checkNotNull(config, "config");
        this.executor = Preconditions.checkNotNull(executor, "executor");
        this.attributeSegment = new AtomicReference<>();
        this.traceObjectId = String.format("AttributeIndex[%s]", this.segmentMetadata.getId());
    }

    /**
     * Initializes the SegmentAttributeIndex by inspecting the AttributeSegmentFile and creating it if needed.
     *
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate the operation has succeeded.
     */
    CompletableFuture<Void> initialize(Duration timeout) {
        TimeoutTimer timer = new TimeoutTimer(timeout);
        String attributeSegmentName = StreamSegmentNameUtils.getAttributeSegmentName(this.segmentMetadata.getName());
        Preconditions.checkState(this.attributeSegment.get() == null, "SegmentAttributeIndex is already initialized.");
        // Attempt to open the Attribute Segment; if it does not exist yet then create it.
        return Futures
                .exceptionallyComposeExpecting(
                        this.storage.openWrite(attributeSegmentName)
                                .thenComposeAsync(handle -> this.storage
                                        .getStreamSegmentInfo(attributeSegmentName, timer.getRemaining())
                                        .thenAccept(si -> this.attributeSegment.set(new AttributeSegment(handle, si.getLength()))), this.executor),
                        ex -> ex instanceof StreamSegmentNotExistsException,
                        () -> this.storage.create(attributeSegmentName, this.config.getAttributeSegmentRollingPolicy(), timer.getRemaining())
                                .thenComposeAsync(si -> this.storage.openWrite(attributeSegmentName)
                                        .thenAccept(handle -> this.attributeSegment.set(new AttributeSegment(handle, si.getLength()))), this.executor)
                )
                .thenRun(() -> log.debug("{}: Initialized (Attribute Segment Length = {}).", this.traceObjectId, this.attributeSegment.get().getLength()));
    }

    /**
     * Deletes all the Attribute data associated with the given Segment.
     *
     * @param segmentMetadata The SegmentMetadata for the Segment whose attribute data should be deleted.
     * @param storage         A Storage Adapter to execute the deletion on.
     * @param timeout         Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the operation finished successfully.
     */
    static CompletableFuture<Void> delete(SegmentMetadata segmentMetadata, Storage storage, Duration timeout) {
        TimeoutTimer timer = new TimeoutTimer(timeout);
        String attributeSegmentName = StreamSegmentNameUtils.getAttributeSegmentName(segmentMetadata.getName());
        return Futures.exceptionallyExpecting(
                storage.openWrite(attributeSegmentName)
                       .thenCompose(handle -> storage.delete(handle, timer.getRemaining())),
                ex -> ex instanceof StreamSegmentNotExistsException,
                null);
    }

    //endregion

    //region AttributeIndex implementation

    @Override
    public CompletableFuture<Void> put(UUID key, Long value, Duration timeout) {
        return put(Collections.singletonMap(key, value), timeout);
    }

    @Override
    public CompletableFuture<Void> put(Map<UUID, Long> values, Duration timeout) {
        ensureInitialized();
        Preconditions.checkNotNull(values, "values");
        if (values.size() == 0) {
            // Nothing to do.
            return CompletableFuture.completedFuture(null);
        } else {
            AttributeCollection c = new AttributeCollection(values);
            if (shouldSnapshot()) {
                // We are overdue for a snapshot. Create one while including the new values. No need to also write them
                // separately.
                return createSnapshot(c, false, timeout);
            } else {
                // Write the new values separately, as an atomic append.
                return Futures.toVoid(appendConditionally(() -> CompletableFuture.completedFuture(serialize(c)),
                        new TimeoutTimer(timeout)));
            }
        }
    }

    @Override
    public CompletableFuture<Map<UUID, Long>> get(Collection<UUID> keys, Duration timeout) {
        ensureInitialized();
        if (keys.size() == 0) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }

        return readAllSinceLastSnapshot(timeout)
                .thenApply(c -> {
                    ImmutableMap.Builder<UUID, Long> b = ImmutableMap.builder();
                    keys.forEach(attributeId -> {
                        long value = c.attributes.getOrDefault(attributeId, Attributes.NULL_ATTRIBUTE_VALUE);
                        if (value != Attributes.NULL_ATTRIBUTE_VALUE) {
                            b.put(attributeId, value);
                        }
                    });
                    return b.build();
                });
    }

    @Override
    public CompletableFuture<Long> get(UUID key, Duration timeout) {
        ensureInitialized();
        return readAllSinceLastSnapshot(timeout)
                .thenApply(c -> c.attributes.get(key));
    }

    @Override
    public CompletableFuture<Void> remove(UUID key, Duration timeout) {
        return remove(Collections.singleton(key), timeout);
    }

    @Override
    public CompletableFuture<Void> remove(Collection<UUID> keys, Duration timeout) {
        Preconditions.checkNotNull(keys, "keys");
        return put(keys.stream().collect(Collectors.toMap(key -> key, key -> Attributes.NULL_ATTRIBUTE_VALUE)), timeout);
    }

    @Override
    public CompletableFuture<Void> seal(Duration timeout) {
        ensureInitialized();
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return Futures.exceptionallyExpecting(
                createSnapshot(new AttributeCollection(), true, timer.getRemaining())
                        .thenComposeAsync(v -> this.storage.seal(this.attributeSegment.get().handle, timer.getRemaining()), this.executor)
                        .thenRun(() -> log.info("{}: Sealed (Length = {}).", this.traceObjectId, this.attributeSegment.get().getLength())),
                ex -> ex instanceof StreamSegmentSealedException,
                null);
    }

    //endregion

    //region Operations

    /**
     * Determines if we should generate a Snapshot based on the current state of the Attribute Segment.
     *
     * @return True if we should generate a Snapshot, false otherwise.
     */
    @VisibleForTesting
    boolean shouldSnapshot() {
        long lastSnapshotEndOffset = getLastSnapshotOffset() + getLastSnapshotLength();
        return this.attributeSegment.get().getLength() - lastSnapshotEndOffset >= this.config.getSnapshotTriggerSize();
    }

    /**
     * Reads all attributes beginning with the last recorded snapshot until the current end of the Attribute Segment.
     *
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the Attributes as they should be when reaching the
     * end of the Attribute Segment.
     */
    private CompletableFuture<AttributeCollection> readAllSinceLastSnapshot(Duration timeout) {
        CompletableFuture<AttributeCollection> result = READ_RETRY.runAsync(() -> {
            ensureMainSegmentExists();
            long lastSnapshotOffset = getLastSnapshotOffset();
            int readLength = (int) Math.min(Integer.MAX_VALUE, this.attributeSegment.get().getLength() - lastSnapshotOffset);
            CompletableFuture<AttributeCollection> r = new CompletableFuture<>();
            if (readLength == 0) {
                // Nothing to read.
                r.complete(new AttributeCollection());
            } else {
                AsyncReadResultProcessor.process(
                        StreamSegmentStorageReader.read(this.attributeSegment.get().handle, lastSnapshotOffset, readLength, this.config.getReadBlockSize(), this.storage),
                        new AttributeSegmentReader(r, timeout),
                        this.executor);
            }
            return r;
        }, this.executor);
        return Futures.exceptionallyCompose(result,
                ex -> {
                    ex = Exceptions.unwrap(ex);
                    if (ex instanceof IOException || ex instanceof StreamSegmentTruncatedException) {
                        ex = new DataCorruptionException(
                                String.format("Unable to parse AttributeSegment. LastSnapshot = (Offset=%d, Length=%d), Known Segment Length = %d.",
                                        getLastSnapshotOffset(), getLastSnapshotLength(), this.attributeSegment.get().getLength()), ex);
                    }
                    return Futures.failedFuture(ex);
                });
    }

    /**
     * Creates a new Snapshot, writes it to the AttributeSegment and updates the main Segment's attributes with its location.
     *
     * @param newAttributes Any new Attributes to include in this Snapshot that would not already be part of it.
     * @param mustComplete  Whether the update of the main Segment's attributes must execute successfully in order for this
     *                      operation to be considered complete. For auto-generated Snapshots, since the Snapshot itself is
     *                      already written to the AttributeSegment, it is not mandatory for the OperationLog Add &
     *                      Attribute Segment Truncation operations to complete, as there will be no data loss. However, for
     *                      Snapshots generated as part of Seals, all of those must complete in order for the operation to
     *                      be considered successful.
     * @param timeout       Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the operation has completed successfully.
     */
    private CompletableFuture<Void> createSnapshot(AttributeCollection newAttributes, boolean mustComplete, Duration timeout) {
        TimeoutTimer timer = new TimeoutTimer(timeout);

        // The serialization may be invoked multiple times, based on whether the appendConditionally() requires a retry.
        Supplier<CompletableFuture<ArrayView>> s = () ->
                readAllSinceLastSnapshot(timer.getRemaining())
                        .thenApplyAsync(c -> {
                            c.mergeWith(newAttributes);
                            return serialize(c);
                        }, this.executor);
        return appendConditionally(s, timer)
                .thenComposeAsync(w -> updateStatePostSnapshot(w, mustComplete, timer), this.executor);
    }

    /**
     * Updates the system's state post a successful Snapshot write to the Attribute Segment.
     *
     * @param writeInfo    Information about the data written as part of the Snapshot.
     * @param mustComplete Whether the operation must complete. Refer to createSnapshot's mustComplete for more details.
     * @param timer        Timer for the operation (used for timeouts).
     * @return A CompletableFuture that, when completed, will indicate that the operation has completed successfully.
     */
    private CompletableFuture<Void> updateStatePostSnapshot(WriteInfo writeInfo, boolean mustComplete, TimeoutTimer timer) {
        log.debug("{}: Snapshot serialized to attribute segment ({}).", this.traceObjectId, writeInfo);
        UpdateAttributesOperation op = new UpdateAttributesOperation(this.segmentMetadata.getId(), Arrays.asList(
                new AttributeUpdate(Attributes.LAST_ATTRIBUTE_SNAPSHOT_OFFSET, AttributeUpdateType.ReplaceIfGreater, writeInfo.offset),
                new AttributeUpdate(Attributes.LAST_ATTRIBUTE_SNAPSHOT_LENGTH, AttributeUpdateType.Replace, writeInfo.length)));
        CompletableFuture<Void> result = this.operationLog
                .add(op, timer.getRemaining())
                .thenComposeAsync(v -> {
                    log.debug("{}: Snapshot location updated in main segment's metadata ({}).", this.traceObjectId, writeInfo);
                    ensureMainSegmentExists();
                    return this.storage.truncate(this.attributeSegment.get().handle, writeInfo.offset, timer.getRemaining());
                }, this.executor);

        if (!mustComplete) {
            result = result.exceptionally(ex -> {
                // Failure to update snapshot location in the main segment's metadata or to truncate the Attribute Segment
                // is not a critical failure. The snapshot has been successfully written, and its data can be recovered.
                // If we bubble up this exception, the caller may think the data have not been written and retry, hence
                // unnecessarily write the data multiple times.
                // If we do get here, we'll simply have to process more data when we read and potentially have more
                // data than needed in the attribute segment, both of which will be fixed with the next snapshot attempt.
                ex = Exceptions.unwrap(ex);
                log.warn("{}: Snapshot serialized to attribute segment, but failed to update snapshot location or truncate Attribute Segment.", this.traceObjectId, ex);
                return null;
            });
        }
        return result;
    }

    /**
     * Appends the result of the given serialization function conditionally based on the current length of the Attribute Segment.
     * This method will retry the write subject to the APPEND_RETRY policy.
     *
     * @param getSerialization A Supplier that, when invoked, returns a CompletableFuture whose result will be the requested
     *                         serialization.
     * @param timer            Timer for the operation. Used for timeouts.
     * @return A CompletableFuture that, when completed, will contain information about the data that was just written.
     */
    private CompletableFuture<WriteInfo> appendConditionally(Supplier<CompletableFuture<ArrayView>> getSerialization, TimeoutTimer timer) {
        return APPEND_RETRY.runAsync(() -> appendConditionallyOnce(getSerialization, timer), this.executor);
    }

    /**
     * Appends the result of the given serialization function conditionally based on the current length of the Attribute Segment.
     * This method does not perform any retries.
     *
     * @param getSerialization A Supplier that, when invoked, returns a CompletableFuture whose result will be the requested
     *                         serialization.
     * @param timer            Timer for the operation. Used for timeouts.
     * @return A CompletableFuture that, when completed, will contain information about the data that was just written.
     */
    private CompletableFuture<WriteInfo> appendConditionallyOnce(Supplier<CompletableFuture<ArrayView>> getSerialization, TimeoutTimer timer) {
        // We want to make sure that the serialization we generate is accurate based on the state of the Attribute Segment.
        // This is to protect against potential corruptions due to concurrency: for example we picked data for a Snapshot,
        // then merged it with some other changes, then wrote it back - we want to ensure nobody else wrote anything in the meantime.
        // As such, we need to do a conditional append keyed on the length of the Attribute Segment. Should there be
        // a concurrent change, we will need to re-generate the serialization in order to guarantee that we always write
        // the latest data.
        AttributeSegment as = this.attributeSegment.get();
        ensureMainSegmentExists();

        long offset = as.getLength();
        return getSerialization.get().thenComposeAsync(data ->
                Futures.exceptionallyCompose(
                        this.storage
                                .write(as.handle, offset, data.getReader(), data.getLength(), timer.getRemaining())
                                .thenApply(v -> {
                                    as.increaseLength(data.getLength());
                                    log.debug("{}: Wrote data ({}).", this.traceObjectId, data.getLength());
                                    return new WriteInfo(offset, data.getLength());
                                }),
                        ex -> {
                            if (Exceptions.unwrap(ex) instanceof BadOffsetException) {
                                return this.storage
                                        .getStreamSegmentInfo(this.attributeSegment.get().handle.getSegmentName(), timer.getRemaining())
                                        .thenCompose(si -> {
                                            as.setLength(si.getLength());
                                            return Futures.failedFuture(ex);
                                        });
                            } else {
                                return Futures.failedFuture(ex);
                            }
                        }));
    }

    /**
     * Serializes the given AttributeCollection.
     *
     * @param attributes The AttributeCollection to serialize.
     * @return An ArrayView representing the serialization.
     */
    @SneakyThrows(IOException.class)
    @VisibleForTesting
    ArrayView serialize(AttributeCollection attributes) {
        return AttributeCollection.SERIALIZER.serialize(attributes);
    }

    /**
     * Gets a SegmentHandle for the AttributeSegment.
     */
    @VisibleForTesting
    SegmentHandle getAttributeSegmentHandle() {
        return this.attributeSegment.get().handle;
    }

    /**
     * Gets the Offset of the Last Snapshot, as found in the main Segment's metadata.
     */
    private long getLastSnapshotOffset() {
        return this.segmentMetadata.getAttributes().getOrDefault(Attributes.LAST_ATTRIBUTE_SNAPSHOT_OFFSET, 0L);
    }

    /**
     * Gets the Length of the Last Snapshot, as found in the main Segment's metadata.
     */
    private int getLastSnapshotLength() {
        return (int) (long) this.segmentMetadata.getAttributes().getOrDefault(Attributes.LAST_ATTRIBUTE_SNAPSHOT_LENGTH, 0L);
    }

    /**
     * Verifies that the main Segment still exists (is not deleted). If it doesn't, it aborts the current operation by
     * throwing a StreamSegmentNotExistsException. If the main Segment is deleted, the owning SegmentContainer will be
     * deleting this Attribute Segment soon, so do not bother making any more changes to it.
     */
    @SneakyThrows(StreamSegmentNotExistsException.class)
    private void ensureMainSegmentExists() {
        if (this.segmentMetadata.isDeleted() || this.segmentMetadata.isMerged()) {
            log.info("{}: Main Segment ({}) is Deleted. Aborting operation.", this.traceObjectId, this.segmentMetadata.getName());
            throw new StreamSegmentNotExistsException(this.segmentMetadata.getName());
        }
    }

    private void ensureInitialized() {
        Preconditions.checkState(this.attributeSegment.get() != null, "SegmentAttributeIndex is not initialized.");
    }

    //endregion

    //region AttributeSegmentReader

    /**
     * Async reader for the Attribute Segment from Storage.
     */
    @NotThreadSafe
    private static class AttributeSegmentReader implements AsyncReadResultHandler {
        private final ArrayList<InputStream> inputs = new ArrayList<>();
        private final CompletableFuture<AttributeCollection> result;
        private final AttributeCollection attributeCollection;
        private final TimeoutTimer timer;

        AttributeSegmentReader(CompletableFuture<AttributeCollection> result, Duration timeout) {
            this.attributeCollection = new AttributeCollection(null);
            this.result = result;
            this.timer = new TimeoutTimer(timeout);
        }

        @Override
        public boolean shouldRequestContents(ReadResultEntryType entryType, long streamSegmentOffset) {
            return true;
        }

        @Override
        public boolean processEntry(ReadResultEntry entry) {
            assert entry.getContent().isDone() : "received incomplete ReadResultEntry from reader";
            this.inputs.add(entry.getContent().join().getData());
            return true;
        }

        @Override
        public void processError(Throwable cause) {
            this.result.completeExceptionally(cause);
        }

        @Override
        public void processResultComplete() {
            Enumeration<InputStream> inputEnumeration = Collections.enumeration(inputs);
            try (SequenceInputStream inputStream = new SequenceInputStream(inputEnumeration)) {
                // Loop as long as the current InputStream has more elements or we have more input streams to process.
                // NOTE: SequenceInputStream.available() will return 0 if it is sitting on the current end of a member InputStream
                // so we cannot rely on that alone.
                while (inputEnumeration.hasMoreElements() || inputStream.available() > 0) {
                    AttributeCollection c = AttributeCollection.SERIALIZER.deserialize(inputStream);
                    this.attributeCollection.mergeWith(c);
                }
                this.result.complete(this.attributeCollection);
            } catch (Throwable ex) {
                processError(ex);
            }
        }

        @Override
        public Duration getRequestContentTimeout() {
            return this.timer.getRemaining();
        }
    }

    //endregion

    //region AttributeSegment

    /**
     * Metadata about the Attribute Segment.
     */
    private static class AttributeSegment {
        private final SegmentHandle handle;
        private final AtomicLong length;

        AttributeSegment(SegmentHandle handle, long initialLength) {
            this.handle = handle;
            this.length = new AtomicLong(initialLength);
        }

        long getLength() {
            return this.length.get();
        }

        void setLength(long value) {
            this.length.set(value);
        }

        void increaseLength(int delta) {
            Preconditions.checkArgument(delta >= 0, "increase must be non-negative");
            this.length.addAndGet(delta);
        }
    }

    //endregion

    //region AttributeCollection & Serializer

    /**
     * Collection of Attributes.
     */
    @Builder
    @VisibleForTesting
    static class AttributeCollection {
        private static final AttributeCollectionSerializer SERIALIZER = new AttributeCollectionSerializer();
        private final Map<UUID, Long> attributes;

        private AttributeCollection() {
            this.attributes = new HashMap<>();
        }

        private AttributeCollection(Map<UUID, Long> attributes) {
            this.attributes = attributes == null ? new HashMap<>() : attributes;
        }

        private void mergeWith(AttributeCollection other) {
            other.attributes.forEach((attributeId, value) -> {
                if (value == Attributes.NULL_ATTRIBUTE_VALUE) {
                    this.attributes.remove(attributeId);
                } else {
                    this.attributes.put(attributeId, value);
                }
            });
        }

        static class AttributeCollectionBuilder implements ObjectBuilder<AttributeCollection> {
        }

        static class AttributeCollectionSerializer extends VersionedSerializer.WithBuilder<AttributeCollection, AttributeCollectionBuilder> {
            @Override
            protected AttributeCollectionBuilder newBuilder() {
                return AttributeCollection.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(AttributeCollection c, RevisionDataOutput output) throws IOException {
                if (output.requiresExplicitLength()) {
                    output.length(output.getMapLength(c.attributes.size(), RevisionDataOutput.UUID_BYTES, Long.BYTES));
                }

                output.writeMap(c.attributes, RevisionDataOutput::writeUUID, RevisionDataOutput::writeLong);
            }

            private void read00(RevisionDataInput input, AttributeCollectionBuilder builder) throws IOException {
                builder.attributes(input.readMap(RevisionDataInput::readUUID, RevisionDataInput::readLong, HashMap::new));
            }
        }
    }

    //endregion

    //region WriteInfo

    /**
     * Information about a write to the AttributeSegment.
     */
    @Data
    @VisibleForTesting
    static class WriteInfo {
        private final long offset;
        private final int length;

        @Override
        public String toString() {
            return String.format("Offset=%d, Length=%d", this.offset, this.length);
        }
    }

    //endregion
}

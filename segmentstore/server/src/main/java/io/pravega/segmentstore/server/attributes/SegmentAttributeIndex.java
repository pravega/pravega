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
import io.pravega.segmentstore.contracts.StreamSegmentTruncatedException;
import io.pravega.segmentstore.server.AttributeIndex;
import io.pravega.segmentstore.server.OperationLog;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.logs.operations.UpdateAttributesOperation;
import io.pravega.segmentstore.server.reading.AsyncReadResultHandler;
import io.pravega.segmentstore.server.reading.AsyncReadResultProcessor;
import io.pravega.segmentstore.server.reading.StreamSegmentStorageReader;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
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
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.RequiredArgsConstructor;

class SegmentAttributeIndex implements AttributeIndex {
    private static final int MAX_ATTRIBUTE_COUNT = 100 * 1000;
    private static final int ESTIMATED_ATTRIBUTE_SERIALIZATION_SIZE = RevisionDataOutput.UUID_BYTES + Long.BYTES;
    private static final int SNAPSHOT_TRIGGER_SIZE = MAX_ATTRIBUTE_COUNT / 10 * ESTIMATED_ATTRIBUTE_SERIALIZATION_SIZE;
    private static final int READ_BLOCK_SIZE = 1024 * 1024;
    private static final SegmentRollingPolicy ATTRIBUTE_SEGMENT_ROLLING_POLICY = new SegmentRollingPolicy(SNAPSHOT_TRIGGER_SIZE);
    private static final Retry.RetryAndThrowBase<Exception> APPEND_RETRY = Retry
            .withExpBackoff(10, 2, 10, 1000)
            .retryWhen(ex -> Exceptions.unwrap(ex) instanceof BadOffsetException)
            .throwingOn(Exception.class);

    private static final Retry.RetryAndThrowBase<Exception> READ_RETRY = Retry
            .withExpBackoff(10, 2, 10, 1000)
            .retryWhen(ex -> Exceptions.unwrap(ex) instanceof StreamSegmentTruncatedException)
            .throwingOn(Exception.class);

    private final SegmentMetadata segmentMetadata;
    private final AtomicReference<AttributeSegment> attributeSegment;
    private final Storage storage;
    private final OperationLog operationLog;
    private final ScheduledExecutorService executor;


    SegmentAttributeIndex(SegmentMetadata segmentMetadata, Storage storage, OperationLog operationLog, ScheduledExecutorService executor) {
        this.segmentMetadata = Preconditions.checkNotNull(segmentMetadata, "segmentMetadata");
        this.storage = Preconditions.checkNotNull(storage, "storage");
        this.operationLog = Preconditions.checkNotNull(operationLog, "operationLog");
        this.executor = Preconditions.checkNotNull(executor, "executor");
        this.attributeSegment = new AtomicReference<>();
    }

    public CompletableFuture<Void> initialize(Duration timeout) {
        TimeoutTimer timer = new TimeoutTimer(timeout);
        String attributeSegmentName = StreamSegmentNameUtils.getAttributeSegmentName(this.segmentMetadata.getName());
        Preconditions.checkState(this.attributeSegment.get() == null, "SegmentAttributeIndex is already initialized.");
        return Futures.exceptionallyCompose(
                this.storage.openWrite(attributeSegmentName)
                        .thenCompose(handle -> this.storage.getStreamSegmentInfo(attributeSegmentName, timer.getRemaining())
                                .thenAccept(si -> this.attributeSegment.set(new AttributeSegment(handle, si.getLength())))),
                ex -> {
                    if (Exceptions.unwrap(ex) instanceof StreamSegmentNotExistsException) {
                        // Attribute Segment does not exist yet. Create it now.
                        return this.storage.create(attributeSegmentName, ATTRIBUTE_SEGMENT_ROLLING_POLICY, timer.getRemaining())
                                .thenComposeAsync(si -> this.storage.openWrite(attributeSegmentName)
                                        .thenAccept(handle -> this.attributeSegment.set(new AttributeSegment(handle, si.getLength()))));
                    }

                    // Some other kind of exception.
                    return Futures.failedFuture(ex);
                });

    }

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
                return createSnapshot(c, timeout);
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
                        long value = c.attributes.getOrDefault(attributeId, SegmentMetadata.NULL_ATTRIBUTE_VALUE);
                        if (value != SegmentMetadata.NULL_ATTRIBUTE_VALUE) {
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
        return put(keys.stream().collect(Collectors.toMap(key -> key, key -> SegmentMetadata.NULL_ATTRIBUTE_VALUE)), timeout);
    }

    @Override
    public CompletableFuture<Void> seal(Duration timeout) {
        ensureInitialized();
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return createSnapshot(AttributeCollection.EMPTY, timer.getRemaining())
                .thenComposeAsync(v -> this.storage.seal(this.attributeSegment.get().handle, timer.getRemaining()), this.executor);
    }

    private boolean shouldSnapshot() {
        long lastSnapshotEndOffset = this.segmentMetadata.getAttributes().getOrDefault(Attributes.LAST_ATTRIBUTE_SNAPSHOT_OFFSET, 0L)
                + this.segmentMetadata.getAttributes().getOrDefault(Attributes.LAST_ATTRIBUTE_SNAPSHOT_LENGTH, 0L);
        return this.attributeSegment.get().getLength() - lastSnapshotEndOffset >= SNAPSHOT_TRIGGER_SIZE;
    }

    private CompletableFuture<AttributeCollection> readAllSinceLastSnapshot(Duration timeout) {
        return READ_RETRY.runAsync(() -> {
            // TODO: handle the case when this value is invalid (points to wrong offset, or out of segment bounds)
            long lastSnapshotOffset = this.segmentMetadata.getAttributes().getOrDefault(Attributes.LAST_ATTRIBUTE_SNAPSHOT_OFFSET, 0L);
            int readLength = (int) Math.min(Integer.MAX_VALUE, this.attributeSegment.get().getLength() - lastSnapshotOffset);
            CompletableFuture<AttributeCollection> result = new CompletableFuture<>();
            if (readLength == 0) {
                // Nothing to read.
                result.complete(AttributeCollection.EMPTY);
            } else {
                AsyncReadResultProcessor.process(
                        StreamSegmentStorageReader.read(this.attributeSegment.get().handle, lastSnapshotOffset, readLength, READ_BLOCK_SIZE, this.storage),
                        new AttributeSegmentReader(result, timeout),
                        this.executor);
            }
            return result;
        }, this.executor);
    }

    private CompletableFuture<Void> createSnapshot(AttributeCollection newAttributes, Duration timeout) {
        // 1. Get Last snapshot
        // 2. Read all updates since that and incorporate into the snapshot
        // 3. Retry if we encounter StreamSegmentTruncatedException.
        // 4. conditionally append new snapshot. With steps 1-3 as source.
        TimeoutTimer timer = new TimeoutTimer(timeout);
        Supplier<CompletableFuture<ArrayView>> s = () ->
                readAllSinceLastSnapshot(timer.getRemaining())
                        .thenApplyAsync(c -> {
                            c.mergeWith(newAttributes);
                            return serialize(c);
                        }, this.executor);
        return appendConditionally(s, timer)
                .thenComposeAsync(w -> updateStatePostSnapshot(w, timer), this.executor);
    }

    private CompletableFuture<Void> updateStatePostSnapshot(WriteInfo w, TimeoutTimer timer) {
        UpdateAttributesOperation op = new UpdateAttributesOperation(this.segmentMetadata.getId(), Arrays.asList(
                new AttributeUpdate(Attributes.LAST_ATTRIBUTE_SNAPSHOT_OFFSET, AttributeUpdateType.ReplaceIfGreater, w.offset),
                new AttributeUpdate(Attributes.LAST_ATTRIBUTE_SNAPSHOT_LENGTH, AttributeUpdateType.Replace, w.length)));
        return this.operationLog.add(op, timer.getRemaining())
                .thenComposeAsync(v -> this.storage.truncate(this.attributeSegment.get().handle, w.offset, timer.getRemaining()), this.executor);
    }

    private CompletableFuture<WriteInfo> appendConditionally(Supplier<CompletableFuture<ArrayView>> getSerialization, TimeoutTimer timer) {
        // We want to make sure that the serialization we generate is accurate based on the state of the Attribute Segment.
        // This is to protect against potential corruptions due to concurrency: for example we picked data for a Snapshot,
        // then merged it with some other changes, then wrote it back - we want to ensure nobody else wrote anything in the meantime.
        // As such, we need to do a conditional append keyed on the length of the Attribute Segment. Should there be
        // a concurrent change, we will need to re-generate the serialization in order to guarantee that we always write
        // the latest data.
        AtomicBoolean canRetry = new AtomicBoolean(true);
        AttributeSegment as = this.attributeSegment.get();
        return APPEND_RETRY.runAsync(() -> {
                    long offset = as.getLength();
                    return getSerialization.get().thenComposeAsync(data ->
                            this.storage.write(as.handle, offset, data.getReader(), data.getLength(), timer.getRemaining())
                                    .thenApply(v -> {
                                        as.increaseLength(data.getLength());
                                        canRetry.set(false);
                                        return new WriteInfo(offset, data.getLength());
                                    })
                                    .exceptionally(ex -> {
                                        // Check if a conditional append exception; if so, we can retry.
                                        canRetry.set(Exceptions.unwrap(ex) instanceof BadOffsetException);
                                        if (!canRetry.get()) {
                                            throw new CompletionException(ex);
                                        }
                                        return null;
                                    }));
                },
                this.executor);

    }

    private ArrayView serialize(AttributeCollection attributes) {
        try {
            // TODO: split up large sets
            // TODO: sort for snapshots
            // TODO: compression
            return AttributeCollection.SERIALIZER.serialize(attributes);
        } catch (IOException ex) {
            throw new CompletionException(ex);
        }
    }

    private void ensureInitialized() {
        Preconditions.checkState(this.attributeSegment.get() != null, "SegmentAttributeIndex is not initialized.");
    }

    //region AttributeSegmentReader

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

        void increaseLength(int delta) {
            Preconditions.checkArgument(delta >= 0, "increase must be non-negative");
            this.length.addAndGet(delta);
        }
    }

    //endregion

    //region AttributeCollection & Serializer

    @Builder
    private static class AttributeCollection {
        static final AttributeCollection EMPTY = new AttributeCollection(Collections.emptyMap());
        private static final AttributeCollectionSerializer SERIALIZER = new AttributeCollectionSerializer();
        private final Map<UUID, Long> attributes;

        AttributeCollection(Map<UUID, Long> attributes) {
            this.attributes = attributes == null ? new HashMap<>() : attributes;
        }

        void mergeWith(AttributeCollection other) {
            other.attributes.forEach((attributeId, value) -> {
                if (value == SegmentMetadata.NULL_ATTRIBUTE_VALUE) {
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

                // TODO Do we need to differentiate between a regular update and a snapshot? Snapshots (big) may be split into multiple writes, so not all may go in.
                output.writeMap(c.attributes, RevisionDataOutput::writeUUID, RevisionDataOutput::writeLong);
            }

            private void read00(RevisionDataInput input, AttributeCollectionBuilder builder) throws IOException {
                builder.attributes(input.readMap(RevisionDataInput::readUUID, RevisionDataInput::readLong, HashMap::new));
            }
        }
    }

    //endregion

    @RequiredArgsConstructor
    @VisibleForTesting
    static class WriteInfo {
        private final long offset;
        private final int length;
    }
}

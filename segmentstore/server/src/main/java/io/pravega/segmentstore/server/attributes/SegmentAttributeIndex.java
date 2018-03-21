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

import com.google.common.base.Preconditions;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.segmentstore.server.AttributeIndex;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

class SegmentAttributeIndex implements AttributeIndex {
    private final UpdateableSegmentMetadata segmentMetadata;
    private final SegmentHandle attributeSegmentHandle;
    private final Storage storage;
    private final ScheduledExecutorService executor;

    SegmentAttributeIndex(UpdateableSegmentMetadata segmentMetadata, SegmentHandle attributeSegmentHandle, Storage storage,
                          ScheduledExecutorService executor) {
        this.segmentMetadata = Preconditions.checkNotNull(segmentMetadata, "segmentMetadata");
        this.attributeSegmentHandle = Preconditions.checkNotNull(attributeSegmentHandle, "attributeSegmentHandle");
        Preconditions.checkArgument(!this.attributeSegmentHandle.isReadOnly(), "attributeSegmentHandle must allow writing.");
        this.storage = Preconditions.checkNotNull(storage, "storage");
        this.executor = Preconditions.checkNotNull(executor, "executor");
    }

    @Override
    public CompletableFuture<Void> put(UUID key, Long value, Duration timeout) {
        return put(Collections.singletonMap(key, value), timeout);
    }

    @Override
    public CompletableFuture<Void> put(Map<UUID, Long> values, Duration timeout) {
        // TODO: implement.
        return null;
    }

    @Override
    public CompletableFuture<Long> get(UUID key, Duration timeout) {
        // TODO: implement
        return null;
    }

    @Override
    public CompletableFuture<Void> remove(UUID key, Duration timeout) {
        return remove(Collections.singleton(key), timeout);
    }

    @Override
    public CompletableFuture<Void> remove(Collection<UUID> keys, Duration timeout) {
        return put(keys.stream().collect(Collectors.toMap(key -> key, key -> SegmentMetadata.NULL_ATTRIBUTE_VALUE)), timeout);
    }

    @Override
    public CompletableFuture<Void> seal(Duration timeout) {
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return snapshot(timer.getRemaining())
                .thenComposeAsync(v -> this.storage.seal(this.attributeSegmentHandle, timer.getRemaining()), this.executor);
    }

    private CompletableFuture<Void> snapshot(Duration timeout) {
        // TODO: implement
        return null;
    }

    private static class AttributeMapSerializer extends VersionedSerializer.WithBuilder<Map<UUID, Long>, AttributeMapSerializer.MapBuilder> {
        @Override
        protected MapBuilder newBuilder() {
            return new MapBuilder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(Map<UUID, Long> batch, RevisionDataOutput output) throws IOException {
            if (output.requiresExplicitLength()) {
                output.length(output.getMapLength(batch == null ? 0 : batch.size(), RevisionDataOutput.UUID_BYTES, Long.BYTES));
            }

            output.writeMap(batch, RevisionDataOutput::writeUUID, RevisionDataOutput::writeLong);

            // TODO need to differentiate between a regular update and a snapshot. Snapshots (big) may be split into multiple writes, so not all may go in.
        }

        private void read00(RevisionDataInput input, AttributeMapSerializer.MapBuilder batchBuilder) throws IOException {
            batchBuilder.map = input.readMap(RevisionDataInput::readUUID, RevisionDataInput::readLong, HashMap::new);
        }

        static class MapBuilder implements ObjectBuilder<Map<UUID, Long>> {
            private Map<UUID, Long> map;

            @Override
            public Map<UUID, Long> build() {
                return this.map;
            }
        }
    }
}

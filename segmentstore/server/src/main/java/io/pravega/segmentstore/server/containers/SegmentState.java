/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.containers;

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.segmentstore.contracts.SegmentProperties;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import lombok.Builder;
import lombok.Getter;

/**
 * Current state of a segment. Objects of this class can be serialized/deserialized to/from a State Store.
 */
class SegmentState {
    //region Members

    static final VersionedSerializer.WithBuilder<SegmentState, SegmentState.SegmentStateBuilder> SERIALIZER = new Serializer();
    @Getter
    private final String segmentName;
    @Getter
    private final long segmentId;
    @Getter
    private final long startOffset;
    @Getter
    private final Map<UUID, Long> attributes;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the SegmentState class.
     *
     * @param segmentId         The Id of the Segment.
     * @param segmentProperties The SegmentProperties to create from.
     */
    SegmentState(long segmentId, SegmentProperties segmentProperties) {
        this(segmentId, segmentProperties.getName(), segmentProperties.getStartOffset(), segmentProperties.getAttributes());
    }

    @Builder
    private SegmentState(long segmentId, String segmentName, long startOffset, Map<UUID, Long> attributes) {
        this.segmentId = segmentId;
        this.segmentName = segmentName;
        this.startOffset = startOffset;
        this.attributes = attributes;
    }

    //endregion

    //region Serialization

    static class SegmentStateBuilder implements ObjectBuilder<SegmentState> {
    }

    private static class Serializer extends VersionedSerializer.WithBuilder<SegmentState, SegmentState.SegmentStateBuilder> {
        @Override
        protected SegmentStateBuilder newBuilder() {
            return SegmentState.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(SegmentState s, RevisionDataOutput output) throws IOException {
            output.writeLong(s.segmentId);
            output.writeUTF(s.segmentName);
            output.writeLong(s.startOffset);
            output.writeMap(s.attributes, RevisionDataOutput::writeUUID, RevisionDataOutput::writeLong);
        }

        private void read00(RevisionDataInput input, SegmentState.SegmentStateBuilder builder) throws IOException {
            builder.segmentId(input.readLong());
            builder.segmentName(input.readUTF());
            builder.startOffset(input.readLong());
            builder.attributes(input.readMap(RevisionDataInput::readUUID, RevisionDataInput::readLong));
        }
    }

    //endregion
}

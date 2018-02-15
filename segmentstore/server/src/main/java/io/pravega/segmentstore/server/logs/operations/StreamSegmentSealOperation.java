/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.logs.operations;

import io.pravega.common.Exceptions;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import java.io.IOException;

/**
 * Log Operation that indicates a StreamSegment has been sealed.
 */
public class StreamSegmentSealOperation extends StorageOperation {
    //region Members

    private long streamSegmentOffset;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentSealOperation class.
     *
     * @param streamSegmentId The Id of the StreamSegment to seal.
     */
    public StreamSegmentSealOperation(long streamSegmentId) {
        super(streamSegmentId);
        this.streamSegmentOffset = -1;
    }

    /**
     * Deserialization constructor.
     */
    private StreamSegmentSealOperation() {
    }

    //endregion

    //region StreamSegmentSealOperation Implementation

    /**
     * Sets the length of the StreamSegment at the time of sealing.
     *
     * @param value The length.
     */
    public void setStreamSegmentOffset(long value) {
        Exceptions.checkArgument(value >= 0, "value", "StreamSegment Offset must be a non-negative number.");
        this.streamSegmentOffset = value;
    }

    //endregion

    //region Operation Implementation

    @Override
    public long getStreamSegmentOffset() {
        return this.streamSegmentOffset;
    }

    @Override
    public long getLength() {
        return 0;
    }

    @Override
    protected void ensureSerializationConditions() {
        super.ensureSerializationConditions();
        ensureSerializationCondition(this.streamSegmentOffset >= 0, "StreamSegment Offset has not been assigned.");
    }

    @Override
    public String toString() {
        return String.format("%s, Length = %s", super.toString(), toString(this.streamSegmentOffset, -1));
    }

    //endregion

    static class Serializer extends VersionedSerializer.WithBuilder<StreamSegmentSealOperation, OperationBuilder<StreamSegmentSealOperation>> {
        private static final int SERIALIZATION_LENGTH = 3 * Long.BYTES;

        @Override
        protected OperationBuilder<StreamSegmentSealOperation> newBuilder() {
            return new OperationBuilder<>(new StreamSegmentSealOperation());
        }

        @Override
        protected byte writeVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(StreamSegmentSealOperation o, RevisionDataOutput target) throws IOException {
            o.ensureSerializationConditions();
            target.length(SERIALIZATION_LENGTH);
            target.writeLong(o.getSequenceNumber());
            target.writeLong(o.getStreamSegmentId());
            target.writeLong(o.streamSegmentOffset);
        }

        private void read00(RevisionDataInput source, OperationBuilder<StreamSegmentSealOperation> b) throws IOException {
            b.instance.setSequenceNumber(source.readLong());
            b.instance.setStreamSegmentId(source.readLong());
            b.instance.streamSegmentOffset = source.readLong();
        }
    }
}

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

import com.google.common.base.Preconditions;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.server.ContainerMetadata;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 * Log Operation that represents a mapping of StreamSegment Name to a StreamSegment Id.
 */
public class StreamSegmentMapOperation extends MetadataOperation {
    //region Members

    private long streamSegmentId;
    private long parentStreamSegmentId;
    private String streamSegmentName;
    private long startOffset;
    private long length;
    private boolean sealed;
    private Map<UUID, Long> attributes;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentMapOperation class for a non-transaction Segment.
     *
     * @param streamSegmentProperties Information about the StreamSegment.
     */
    public StreamSegmentMapOperation(SegmentProperties streamSegmentProperties) {
        this(ContainerMetadata.NO_STREAM_SEGMENT_ID, streamSegmentProperties);
    }

    /**
     * Creates a new instance of the StreamSegmentMapOperation class.
     *
     * @param parentStreamSegmentId   The Id of the Parent StreamSegment. If this is not a transaction, this should be set
     *                                to ContainerMetadata.NO_STREAM_SEGMENT_ID.
     * @param streamSegmentProperties Information about the StreamSegment.
     */
    public StreamSegmentMapOperation(long parentStreamSegmentId, SegmentProperties streamSegmentProperties) {
        this.streamSegmentId = ContainerMetadata.NO_STREAM_SEGMENT_ID;
        this.parentStreamSegmentId = parentStreamSegmentId;
        this.streamSegmentName = streamSegmentProperties.getName();
        this.startOffset = isTransaction() ? 0 : streamSegmentProperties.getStartOffset();
        this.length = streamSegmentProperties.getLength();
        this.sealed = streamSegmentProperties.isSealed();
        this.attributes = streamSegmentProperties.getAttributes();
    }

    /**
     * Deserialization constructor.
     */
    private StreamSegmentMapOperation() {
    }

    //endregion

    //region MappingOperation implementation.

    /**
     * Gets a value indicating the Id of the Parent StreamSegment.
     */
    public long getParentStreamSegmentId() {
        return this.parentStreamSegmentId;
    }

    /**
     * Gets a value indicating the Name of the StreamSegment.
     */
    public String getStreamSegmentName() {
        return this.streamSegmentName;
    }

    /**
     * Gets a value indicating the Id of the StreamSegment.
     */
    public long getStreamSegmentId() {
        return this.streamSegmentId;
    }

    /**
     * Sets the StreamSegmentId for this operation.
     *
     * @param value The Id of the segment to set.
     */
    public void setStreamSegmentId(long value) {
        Preconditions.checkState(this.streamSegmentId == ContainerMetadata.NO_STREAM_SEGMENT_ID, "StreamSegmentId has already been assigned for this operation.");
        Preconditions.checkArgument(value != ContainerMetadata.NO_STREAM_SEGMENT_ID, "Invalid StreamSegmentId");
        this.streamSegmentId = value;
    }

    /**
     * Gets a value indicating the first offset within the StreamSegment available for reading.
     */
    public long getStartOffset() {
        return this.startOffset;
    }

    /**
     * Gets a value indicating the Length of the StreamSegment at the time of the mapping.
     */
    public long getLength() {
        return this.length;
    }

    /**
     * Gets a value indicating whether the StreamSegment is currently sealed at the time of the mapping.
     */
    public boolean isSealed() {
        return this.sealed;
    }

    /**
     * Gets the Attributes for the StreamSegment at the time of the mapping.
     */
    public Map<UUID, Long> getAttributes() {
        return this.attributes;
    }

    /**
     * Gets a value indicating whether this MappingOperation is for a Transaction StreamSegment.
     */
    public boolean isTransaction() {
        return this.parentStreamSegmentId != ContainerMetadata.NO_STREAM_SEGMENT_ID;
    }

    @Override
    public String toString() {
        return String.format(
                "%s, Id = %s%s, Name = %s, Start = %d, Length = %d, Sealed = %s",
                super.toString(),
                toString(getStreamSegmentId(), ContainerMetadata.NO_STREAM_SEGMENT_ID),
                isTransaction() ? String.format(", ParentId = %s", getParentStreamSegmentId()) : "",
                getStreamSegmentName(),
                getStartOffset(),
                getLength(),
                isSealed());
    }

    //endregion

    static class Serializer extends OperationSerializer<StreamSegmentMapOperation> {
        private static final int STATIC_LENGTH = 5 * Long.BYTES + Byte.BYTES;

        @Override
        protected OperationBuilder<StreamSegmentMapOperation> newBuilder() {
            return new OperationBuilder<>(new StreamSegmentMapOperation());
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        @Override
        protected void beforeSerialization(StreamSegmentMapOperation o) {
            super.beforeSerialization(o);
            Preconditions.checkState(o.streamSegmentId != ContainerMetadata.NO_STREAM_SEGMENT_ID, "StreamSegment Id has not been assigned.");
        }

        private void write00(StreamSegmentMapOperation o, RevisionDataOutput target) throws IOException {
            target.length(STATIC_LENGTH + target.getUTFLength(o.streamSegmentName)
                    + target.getMapLength(o.attributes.size(), RevisionDataOutput.UUID_BYTES, Long.BYTES));
            target.writeLong(o.getSequenceNumber());
            target.writeLong(o.streamSegmentId);
            target.writeLong(o.parentStreamSegmentId);
            target.writeUTF(o.streamSegmentName);
            target.writeLong(o.startOffset);
            target.writeLong(o.length);
            target.writeBoolean(o.sealed);
            target.writeMap(o.attributes, RevisionDataOutput::writeUUID, RevisionDataOutput::writeLong);
        }

        private void read00(RevisionDataInput source, OperationBuilder<StreamSegmentMapOperation> b) throws IOException {
            b.instance.setSequenceNumber(source.readLong());
            b.instance.streamSegmentId = source.readLong();
            b.instance.parentStreamSegmentId = source.readLong();
            b.instance.streamSegmentName = source.readUTF();
            b.instance.startOffset = source.readLong();
            b.instance.length = source.readLong();
            b.instance.sealed = source.readBoolean();
            b.instance.attributes = source.readMap(RevisionDataInput::readUUID, RevisionDataInput::readLong);
        }
    }
}

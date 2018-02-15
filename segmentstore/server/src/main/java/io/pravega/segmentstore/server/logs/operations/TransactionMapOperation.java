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
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.server.ContainerMetadata;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 * Log Operation that represents a mapping between a Transaction StreamSegment and its Parent StreamSegment.
 */
public class TransactionMapOperation extends MetadataOperation implements StreamSegmentMapping {
    //region Members

    private long parentStreamSegmentId;
    private long streamSegmentId;
    private String streamSegmentName;
    private long length;
    private boolean sealed;
    private Map<UUID, Long> attributes;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the TransactionMapOperation class.
     *
     * @param parentStreamSegmentId The Id of the Parent StreamSegment.
     * @param transSegmentInfo      SegmentProperties for Transaction StreamSegment.
     */
    public TransactionMapOperation(long parentStreamSegmentId, SegmentProperties transSegmentInfo) {
        super();
        Preconditions.checkArgument(parentStreamSegmentId != ContainerMetadata.NO_STREAM_SEGMENT_ID, "parentStreamSegmentId must be defined.");
        this.parentStreamSegmentId = parentStreamSegmentId;
        this.streamSegmentId = ContainerMetadata.NO_STREAM_SEGMENT_ID;
        this.streamSegmentName = transSegmentInfo.getName();
        this.length = transSegmentInfo.getLength();
        this.sealed = transSegmentInfo.isSealed();
        this.attributes = transSegmentInfo.getAttributes();
    }

    /**
     * Deserialization constructor.
     */
    private TransactionMapOperation() {
    }

    //endregion

    //region TransactionMapOperation Properties

    /**
     * Gets a value indicating the Id of the Parent StreamSegment.
     */
    public long getParentStreamSegmentId() {
        return this.parentStreamSegmentId;
    }

    @Override
    public void setStreamSegmentId(long value) {
        Preconditions.checkState(this.streamSegmentId == ContainerMetadata.NO_STREAM_SEGMENT_ID, "StreamSegmentId has already been assigned for this operation.");
        Preconditions.checkArgument(value != ContainerMetadata.NO_STREAM_SEGMENT_ID, "Invalid StreamSegmentId");
        this.streamSegmentId = value;
    }

    //endregion

    //region StreamSegmentMapping Implementation

    @Override
    public long getStreamSegmentId() {
        return this.streamSegmentId;
    }

    @Override
    public String getStreamSegmentName() {
        return this.streamSegmentName;
    }

    @Override
    public long getStartOffset() {
        // Transactions cannot be truncated, so don't bother with StartOffset here.
        return 0;
    }

    @Override
    public long getLength() {
        return this.length;
    }

    @Override
    public boolean isSealed() {
        return this.sealed;
    }

    @Override
    public Map<UUID, Long> getAttributes() {
        return this.attributes;
    }

    //endregion

    //region Operation Implementation

    @Override
    protected void ensureSerializationConditions() {
        super.ensureSerializationConditions();
        ensureSerializationCondition(this.streamSegmentId != ContainerMetadata.NO_STREAM_SEGMENT_ID,
                "TransactionStreamSegment Id has not been assigned.");
    }

    @Override
    public String toString() {
        return String.format(
                "%s, Id = %s, ParentId = %s, Name = %s, Length = %d, Sealed = %s",
                super.toString(),
                toString(getStreamSegmentId(), ContainerMetadata.NO_STREAM_SEGMENT_ID),
                toString(getParentStreamSegmentId(), ContainerMetadata.NO_STREAM_SEGMENT_ID),
                getStreamSegmentName(),
                getLength(),
                isSealed());
    }

    //endregion

    static class Serializer extends VersionedSerializer.WithBuilder<TransactionMapOperation, OperationBuilder<TransactionMapOperation>> {
        private static final int STATIC_LENGTH = 4 * Long.BYTES + Byte.BYTES;

        @Override
        protected OperationBuilder<TransactionMapOperation> newBuilder() {
            return new OperationBuilder<>(new TransactionMapOperation());
        }

        @Override
        protected byte writeVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(TransactionMapOperation o, RevisionDataOutput target) throws IOException {
            o.ensureSerializationConditions();
            target.length(STATIC_LENGTH + target.getUTFLength(o.streamSegmentName)
                    + target.getMapLength(o.attributes.size(), RevisionDataOutput.UUID_BYTES, Long.BYTES));
            target.writeLong(o.getSequenceNumber());
            target.writeLong(o.parentStreamSegmentId);
            target.writeLong(o.streamSegmentId);
            target.writeUTF(o.streamSegmentName);
            target.writeLong(o.length);
            target.writeBoolean(o.sealed);
            target.writeMap(o.attributes, RevisionDataOutput::writeUUID, RevisionDataOutput::writeLong);
        }

        private void read00(RevisionDataInput source, OperationBuilder<TransactionMapOperation> b) throws IOException {
            b.instance.setSequenceNumber(source.readLong());
            b.instance.parentStreamSegmentId = source.readLong();
            b.instance.streamSegmentId = source.readLong();
            b.instance.streamSegmentName = source.readUTF();
            b.instance.length = source.readLong();
            b.instance.sealed = source.readBoolean();
            b.instance.attributes = source.readMap(RevisionDataInput::readUUID, RevisionDataInput::readLong);
        }
    }
}

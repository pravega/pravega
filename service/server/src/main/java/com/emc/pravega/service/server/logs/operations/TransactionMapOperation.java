/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server.logs.operations;

import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.server.ContainerMetadata;
import com.emc.pravega.service.server.logs.SerializationException;
import com.google.common.base.Preconditions;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 * Log Operation that represents a mapping between a Transaction StreamSegment and its Parent StreamSegment.
 */
public class TransactionMapOperation extends MetadataOperation implements StreamSegmentMapping {
    //region Members

    public static final byte OPERATION_TYPE = 5;
    private static final byte CURRENT_VERSION = 0;
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

    protected TransactionMapOperation(OperationHeader header, DataInputStream source) throws SerializationException {
        super(header, source);
    }

    //endregion

    //region TransactionMapOperation Properties

    /**
     * Gets a value indicating the Id of the Parent StreamSegment.
     */
    public long getParentStreamSegmentId() {
        return this.parentStreamSegmentId;
    }

    /**
     * Sets the StreamSegmentId for this operation.
     *
     * @param value The Segment Id to set.
     */
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
    protected byte getOperationType() {
        return OPERATION_TYPE;
    }

    @Override
    protected void serializeContent(DataOutputStream target) throws IOException {
        ensureSerializationCondition(this.streamSegmentId != ContainerMetadata.NO_STREAM_SEGMENT_ID, "TransactionStreamSegment Id has not been assigned for this entry.");
        target.writeByte(CURRENT_VERSION);
        target.writeLong(this.parentStreamSegmentId);
        target.writeLong(this.streamSegmentId);
        target.writeUTF(this.streamSegmentName);
        target.writeLong(this.length);
        target.writeBoolean(this.sealed);
        AttributeSerializer.serialize(this.attributes, target);
    }

    @Override
    protected void deserializeContent(DataInputStream source) throws IOException, SerializationException {
        readVersion(source, CURRENT_VERSION);
        this.parentStreamSegmentId = source.readLong();
        this.streamSegmentId = source.readLong();
        this.streamSegmentName = source.readUTF();
        this.length = source.readLong();
        this.sealed = source.readBoolean();
        this.attributes = AttributeSerializer.deserialize(source);
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
}

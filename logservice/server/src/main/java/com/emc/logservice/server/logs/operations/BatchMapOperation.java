package com.emc.logservice.server.logs.operations;

import com.emc.logservice.contracts.SegmentProperties;
import com.emc.logservice.server.logs.SerializationException;

import java.io.*;

/**
 * Log Operation that represents a mapping between a Batch Stream and its Parent Stream.
 */
public class BatchMapOperation extends MetadataOperation {
    //region Members

    public static final byte OperationType = 5;
    private static final byte CurrentVersion = 0;
    private long parentStreamSegmentId;
    private long batchStreamSegmentId;
    private String batchStreamSegmentName;
    private long batchStreamSegmentLength;
    private boolean batchStreamSegmentSealed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the BatchMapOperation class.
     *
     * @param parentStreamSegmentId The Id of the Parent StreamSegment.
     * @param batchStreamSegmentId  The Id of the Batch StreamSegment.
     * @param batchStreamInfo       SegmentProperties for Batch StreamSegment.
     */
    public BatchMapOperation(long parentStreamSegmentId, long batchStreamSegmentId, SegmentProperties batchStreamInfo) {
        super();
        this.parentStreamSegmentId = parentStreamSegmentId;
        this.batchStreamSegmentId = batchStreamSegmentId;
        this.batchStreamSegmentName = batchStreamInfo.getName();
        this.batchStreamSegmentLength = batchStreamInfo.getLength();
        this.batchStreamSegmentSealed = batchStreamInfo.isSealed();
    }

    protected BatchMapOperation(OperationHeader header, DataInputStream source) throws SerializationException {
        super(header, source);
    }

    //endregion

    //region BatchMapOperation Properties

    /**
     * Gets a value indicating the Id of the Parent StreamSegment.
     *
     * @return
     */
    public long getParentStreamSegmentId() {
        return this.parentStreamSegmentId;
    }

    /**
     * Gets a value indicating the Id of the Batch StreamSegment.
     *
     * @return
     */
    public long getBatchStreamSegmentId() {
        return this.batchStreamSegmentId;
    }

    /**
     * Gets a value indicating the Name of the Batch StreamSegment.
     *
     * @return
     */
    public String getBatchStreamSegmentName() {
        return this.batchStreamSegmentName;
    }

    /**
     * Gets a value indicating the Length of the Batch StreamSegment.
     *
     * @return
     */
    public long getBatchStreamSegmentLength() {
        return this.batchStreamSegmentLength;
    }

    /**
     * Gets a value indicating whether the Batch StreamSegment is currently sealed.
     *
     * @return
     */
    public boolean isBatchSealed() {
        return this.batchStreamSegmentSealed;
    }

    //endregion

    //region Operation Implementation

    @Override
    protected byte getOperationType() {
        return OperationType;
    }

    @Override
    protected void serializeContent(DataOutputStream target) throws IOException {
        target.writeByte(CurrentVersion);
        target.writeLong(this.parentStreamSegmentId);
        target.writeLong(this.batchStreamSegmentId);
        target.writeUTF(this.batchStreamSegmentName);
        target.writeLong(this.batchStreamSegmentLength);
        target.writeBoolean(this.batchStreamSegmentSealed);
    }

    @Override
    protected void deserializeContent(DataInputStream source) throws IOException, SerializationException {
        byte version = readVersion(source, CurrentVersion);
        this.parentStreamSegmentId = source.readLong();
        this.batchStreamSegmentId = source.readLong();
        this.batchStreamSegmentName = source.readUTF();
        this.batchStreamSegmentLength = source.readLong();
        this.batchStreamSegmentSealed = source.readBoolean();
    }

    @Override
    public String toString() {
        return String.format("%s, ParentStreamId = %d, BatchStreamId = %d, BatchName = %s, BatchLength = %d, BatchSealed = %s", super.toString(), getParentStreamSegmentId(), getBatchStreamSegmentId(), getBatchStreamSegmentName(), getBatchStreamSegmentLength(), isBatchSealed());
    }

    //endregion
}

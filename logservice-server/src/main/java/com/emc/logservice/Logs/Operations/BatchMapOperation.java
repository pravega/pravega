package com.emc.logservice.logs.operations;

import com.emc.logservice.logs.SerializationException;

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

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the BatchMapOperation class.
     *
     * @param parentStreamSegmentId  The Id of the Parent StreamSegment.
     * @param batchStreamSegmentId   The Id of the Batch StreamSegment.
     * @param batchStreamSegmentName The name of the Batch StreamSegment.
     */
    public BatchMapOperation(long parentStreamSegmentId, long batchStreamSegmentId, String batchStreamSegmentName) {
        super();
        this.parentStreamSegmentId = parentStreamSegmentId;
        this.batchStreamSegmentId = batchStreamSegmentId;
        this.batchStreamSegmentName = batchStreamSegmentName;
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
    }

    @Override
    protected void deserializeContent(DataInputStream source) throws IOException, SerializationException {
        byte version = readVersion(source, CurrentVersion);
        this.parentStreamSegmentId = source.readLong();
        this.batchStreamSegmentId = source.readLong();
        this.batchStreamSegmentName = source.readUTF();
    }

    @Override
    public String toString() {
        return String.format("%s, ParentStreamId = %d, BatchStreamId = %d, BatchStreamName = %s", super.toString(), getParentStreamSegmentId(), getBatchStreamSegmentId(), getBatchStreamSegmentName());
    }

    //endregion
}

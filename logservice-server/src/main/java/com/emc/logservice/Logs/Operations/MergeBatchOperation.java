package com.emc.logservice.Logs.Operations;

import com.emc.logservice.Logs.SerializationException;

import java.io.*;

/**
 * Log Operation that indicates a Batch StreamSegment is merged into its parent StreamSegment.
 */
public class MergeBatchOperation extends StorageOperation {
    //region Members

    public static final byte OperationType = 3;
    private static final byte Version = 0;
    private long targetStreamSegmentOffset;
    private long batchStreamSegmentLength;
    private long batchStreamSegmentId;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the MergeBatchOperation class.
     *
     * @param streamSegmentId      The Id of the Parent StreamSegment (the StreamSegment to merge into).
     * @param batchStreamSegmentId The Id of the Batch StreamSegment (the StreamSegment to be merged).
     */
    public MergeBatchOperation(long streamSegmentId, long batchStreamSegmentId) {
        super(streamSegmentId);
        this.batchStreamSegmentId = batchStreamSegmentId;
        this.batchStreamSegmentLength = -1;
        this.targetStreamSegmentOffset = -1;
    }

    protected MergeBatchOperation(OperationHeader header, DataInputStream source) throws SerializationException {
        super(header, source);
    }

    //endregion

    //region MergeBatchOperation Properties

    /**
     * Gets a value indicating the Id of the Batch StreamSegment (the StreamSegment to be merged).
     *
     * @return The Id.
     */
    public long getBatchStreamSegmentId() {
        return this.batchStreamSegmentId;
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
     * Sets the length of the Batch StreamSegment.
     *
     * @param value The length.
     */
    public void setBatchStreamSegmentLength(long value) {
        // No need for parameter validation here. We will check for them upon serialization.
        this.batchStreamSegmentLength = value;
    }

    /**
     * Gets a value indicating the Offset in the Target StreamSegment to merge at.
     *
     * @return The offset.
     */
    public long getTargetStreamSegmentOffset() {
        return this.targetStreamSegmentOffset;
    }

    /**
     * Sets the offset of the Target StreamSegment to merge at.
     *
     * @param value The offset.
     */
    public void setTargetStreamSegmentOffset(long value) {
        // No need for parameter validation here. We will check for them upon serialization.
        this.targetStreamSegmentOffset = value;
    }

    //endregion

    //region Operation Implementation

    @Override
    protected byte getOperationType() {
        return OperationType;
    }

    @Override
    protected void serializeContent(DataOutputStream target) throws IOException {
        ensureSerializationCondition(this.batchStreamSegmentLength >= 0, "Batch Stream Length has not been assigned for this entry.");
        ensureSerializationCondition(this.targetStreamSegmentOffset >= 0, "Target Stream Offset has not been assigned for this entry.");

        target.writeByte(Version);
        target.writeLong(getStreamSegmentId());
        target.writeLong(this.batchStreamSegmentId);
        target.writeLong(this.batchStreamSegmentLength);
        target.writeLong(this.targetStreamSegmentOffset);
    }

    @Override
    protected void deserializeContent(DataInputStream source) throws IOException, SerializationException {
        byte version = readVersion(source, Version);
        setStreamSegmentId(source.readLong());
        this.batchStreamSegmentId = source.readLong();
        this.batchStreamSegmentLength = source.readLong();
        this.targetStreamSegmentOffset = source.readLong();
    }

    @Override
    public String toString() {
        return String.format("%s, BatchStreamId = %d, BatchLength = %d, TargetStreamOffset = %d", super.toString(), getBatchStreamSegmentId(), getBatchStreamSegmentLength(), getTargetStreamSegmentOffset());
    }

    //endregion
}

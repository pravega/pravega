package com.emc.logservice.Logs.Operations;

import com.emc.logservice.Logs.SerializationException;

import java.io.*;

/**
 * Log Operation that indicates a StreamSegment has been sealed.
 */
public class StreamSegmentSealOperation extends StorageOperation {
    //region Members

    public static final byte OperationType = 2;
    private static final byte Version = 0;
    private long streamSegmentLength;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentSealOperation class.
     *
     * @param streamSegmentId The Id of the StreamSegment to seal.
     */
    public StreamSegmentSealOperation(long streamSegmentId) {
        super(streamSegmentId);
        this.streamSegmentLength = -1;
    }

    protected StreamSegmentSealOperation(OperationHeader header, DataInputStream source) throws SerializationException {
        super(header, source);
    }

    //endregion

    //region StreamSegmentSealOperation Implementation

    /**
     * Gets a value indicating the length of the StreamSegment at the time of sealing.
     *
     * @return The length.
     */
    public long getStreamSegmentLength() {
        return this.streamSegmentLength;
    }

    /**
     * Sets the length of the StreamSegment at the time of sealing.
     *
     * @param value The length.
     */
    public void setStreamSegmentLength(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("Stream Segment Length must be a non-negative number.");
        }

        this.streamSegmentLength = value;
    }

    //endregion

    //region Operation Implementation

    @Override
    protected byte getOperationType() {
        return OperationType;
    }

    @Override
    protected void serializeContent(DataOutputStream target) throws IOException {
        ensureSerializationCondition(this.streamSegmentLength >= 0, "Stream Length has not been assigned for this entry.");
        target.writeByte(Version);
        target.writeLong(getStreamSegmentId());
        target.writeLong(this.streamSegmentLength);
    }

    @Override
    protected void deserializeContent(DataInputStream source) throws IOException, SerializationException {
        byte version = readVersion(source, Version);
        setStreamSegmentId(source.readLong());
        this.streamSegmentLength = source.readLong();
    }

    @Override
    public String toString() {
        return String.format("%s, StreamLength = %d", super.toString(), this.streamSegmentLength);
    }

    //endregion
}

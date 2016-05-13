package com.emc.logservice.Logs.Operations;

import com.emc.logservice.Core.StreamHelpers;
import com.emc.logservice.Logs.SerializationException;

import java.io.*;

/**
 * Log Operation that represents a StreamSegment Append.
 */
public class StreamSegmentAppendOperation extends StorageOperation {
    //region Members

    private static final byte Version = 0;
    public static final byte OperationType = 1;
    private long streamSegmentOffset;
    private byte[] data;

    //endregion

    // region Constructor

    /**
     * Creates a new instance of the StreamSegmentAppendOperation class.
     *
     * @param streamSegmentId The Id of the StreamSegment to add to.
     * @param data            The payload to add.
     */
    public StreamSegmentAppendOperation(long streamSegmentId, byte[] data) {
        super(streamSegmentId);
        this.data = data;
        this.streamSegmentOffset = -1;
    }

    protected StreamSegmentAppendOperation(OperationHeader header, DataInputStream source) throws SerializationException {
        super(header, source);
    }

    //endregion

    //region StreamSegmentAppendOperation Properties

    /**
     * Gets a value indicating the Offset in the StreamSegment to add at.
     *
     * @return The offset.
     */
    public long getStreamSegmentOffset() {
        return this.streamSegmentOffset;
    }

    /**
     * Sets the Offset in the StreamSegment to add at.
     *
     * @param value The offset.
     */
    public void setStreamSegmentOffset(long value) {
        // No need for parameter validation here. We allow even invalid offsets now - we will check for them upon serialization.
        this.streamSegmentOffset = value;
    }

    /**
     * Gets the data buffer for this add.
     *
     * @return The data buffer.
     */
    public byte[] getData() {
        return this.data;
    }

    //endregion

    //region Operation Implementation

    @Override
    protected byte getOperationType() {
        return OperationType;
    }

    @Override
    protected void serializeContent(DataOutputStream target) throws IOException {
        ensureSerializationCondition(this.streamSegmentOffset >= 0, "Stream Offset has not been assigned for this entry.");

        target.writeByte(Version);
        target.writeLong(getStreamSegmentId());
        target.writeLong(this.streamSegmentOffset);
        target.writeInt(data.length);
        target.write(data, 0, data.length);
    }

    @Override
    protected void deserializeContent(DataInputStream source) throws IOException, SerializationException {
        byte version = readVersion(source, Version);
        setStreamSegmentId(source.readLong());
        this.streamSegmentOffset = source.readLong();
        int dataLength = source.readInt();
        this.data = new byte[dataLength];
        StreamHelpers.readAll(source, this.data, 0, this.data.length);
    }

    @Override
    public String toString() {
        return String.format("%s, Offset = %d, Length = %d", super.toString(), this.streamSegmentOffset, this.data.length);
    }

    //endregion
}

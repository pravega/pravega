package com.emc.logservice.logs.operations;

import com.emc.logservice.logs.SerializationException;
import com.emc.logservice.StreamSegmentInformation;

import java.io.*;

/**
 * Log Operation that represents a mapping of StreamSegment Name to a StreamSegment Id.
 */
public class StreamSegmentMapOperation extends MetadataOperation {
    //region Members

    public static final byte OperationType = 4;
    private static final byte Version = 0;
    private long streamSegmentId;
    private String streamSegmentName;
    private long streamSegmentLength;
    private boolean sealed;
    private long lastModifiedTime;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentMapOperation class.
     *
     * @param streamSegmentId          The Id of the StreamSegment.
     * @param streamSegmentInformation Information about the StreamSegment.
     */
    public StreamSegmentMapOperation(long streamSegmentId, StreamSegmentInformation streamSegmentInformation) {
        super();
        this.streamSegmentId = streamSegmentId;
        this.streamSegmentName = streamSegmentInformation.getStreamSegmentName();
        this.streamSegmentLength = streamSegmentInformation.getLength();
        this.sealed = streamSegmentInformation.isSealed();
        this.lastModifiedTime = streamSegmentInformation.getLastModified().getTime();
    }

    protected StreamSegmentMapOperation(OperationHeader header, DataInputStream source) throws SerializationException {
        super(header, source);
    }

    //endregion

    //region StreamSegmentMapOperation Properties

    /**
     * Gets a value indicating the Name of the StreamSegment.
     *
     * @return
     */
    public String getStreamSegmentName() {
        return this.streamSegmentName;
    }

    /**
     * Gets a value indicating the Id of the StreamSegment.
     *
     * @return
     */
    public long getStreamSegmentId() {
        return this.streamSegmentId;
    }

    /**
     * Gets a value indicating the Length of the StreamSegment.
     *
     * @return
     */
    public long getStreamSegmentLength() {
        return this.streamSegmentLength;
    }

    /**
     * Gets a value indicating whether the StreamSegment is currently sealed.
     *
     * @return
     */
    public boolean isSealed() {
        return this.sealed;
    }

    /**
     * Gets a value indicating the Last Modified Time of the StreamSegment. TODO: figure out what data type this should be.
     *
     * @return
     */
    public long getLastModifiedTime() {
        return this.lastModifiedTime;
    }

    //endregion

    //region Operation Implementation

    @Override
    protected byte getOperationType() {
        return OperationType;
    }

    @Override
    protected void serializeContent(DataOutputStream target) throws IOException {
        target.writeByte(Version);
        target.writeLong(this.streamSegmentId);
        target.writeUTF(this.streamSegmentName);
        target.writeLong(this.streamSegmentLength);
        target.writeBoolean(this.sealed);
        target.writeLong(this.lastModifiedTime);
    }

    @Override
    protected void deserializeContent(DataInputStream source) throws IOException, SerializationException {
        byte version = readVersion(source, Version);
        this.streamSegmentId = source.readLong();
        this.streamSegmentName = source.readUTF();
        this.streamSegmentLength = source.readLong();
        this.sealed = source.readBoolean();
        this.lastModifiedTime = source.readLong();
    }

    @Override
    public String toString() {
        return String.format("%s, StreamId = %d, StreamName = %s, StreamLength = %d, Sealed = %s", super.toString(), getStreamSegmentId(), getStreamSegmentName(), getStreamSegmentLength(), isSealed());
    }

    //endregion
}

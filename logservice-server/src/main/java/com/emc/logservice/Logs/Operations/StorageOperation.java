package com.emc.logservice.Logs.Operations;

import com.emc.logservice.Logs.SerializationException;

import java.io.DataInputStream;

/**
 * Log Operation that deals with Storage Operations. This is generally the direct result of an external operation.
 */
public abstract class StorageOperation extends Operation {
    //region Members

    private long streamSegmentId;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StorageOperation class.
     *
     * @param streamSegmentId The Id of the StreamSegment this operation relates to.
     */
    public StorageOperation(long streamSegmentId) {
        super();
        setStreamSegmentId(streamSegmentId);
    }

    protected StorageOperation(OperationHeader header, DataInputStream source) throws SerializationException {
        super(header, source);
    }

    //endregion

    //region StorageOperation Properties

    /**
     * Gets a value indicating the Id of the StreamSegment this operation relates to.
     *
     * @return
     */
    public long getStreamSegmentId() {
        return this.streamSegmentId;
    }

    /**
     * Sets the Id of the StreamSegment.
     *
     * @param streamSegmentId The id to set.
     */
    protected void setStreamSegmentId(long streamSegmentId) {
        this.streamSegmentId = streamSegmentId;
    }

    @Override
    public String toString() {
        return String.format("%s, StreamId = %d", super.toString(), getStreamSegmentId());
    }

    //endregion
}

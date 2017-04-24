/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.service.server.logs.operations;

import io.pravega.service.server.logs.SerializationException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Log Operation that indicates a Transaction StreamSegment is merged into its parent StreamSegment.
 */
public class MergeTransactionOperation extends StorageOperation {
    //region Members

    private static final byte VERSION = 0;
    private long streamSegmentOffset;
    private long length;
    private long transactionSegmentId;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the MergeTransactionOperation class.
     *
     * @param streamSegmentId      The Id of the Parent StreamSegment (the StreamSegment to merge into).
     * @param transactionSegmentId The Id of the Transaction StreamSegment (the StreamSegment to be merged).
     */
    public MergeTransactionOperation(long streamSegmentId, long transactionSegmentId) {
        super(streamSegmentId);
        this.transactionSegmentId = transactionSegmentId;
        this.length = -1;
        this.streamSegmentOffset = -1;
    }

    protected MergeTransactionOperation(OperationHeader header, DataInputStream source) throws SerializationException {
        super(header, source);
    }

    //endregion

    //region MergeTransactionOperation Properties

    /**
     * Gets a value indicating the Id of the Transaction StreamSegment (the StreamSegment to be merged).
     *
     * @return The Id.
     */
    public long getTransactionSegmentId() {
        return this.transactionSegmentId;
    }

    /**
     * Sets the length of the Transaction StreamSegment.
     *
     * @param value The length.
     */
    public void setLength(long value) {
        // No need for parameter validation here. We will check for them upon serialization.
        this.length = value;
    }

    /**
     * Sets the offset of the Target StreamSegment to merge at.
     *
     * @param value The offset.
     */
    public void setStreamSegmentOffset(long value) {
        // No need for parameter validation here. We will check for them upon serialization.
        this.streamSegmentOffset = value;
    }

    //endregion

    //region Operation Implementation

    /**
     * Gets a value indicating the Offset in the Target StreamSegment to merge at.
     *
     * @return The offset.
     */
    @Override
    public long getStreamSegmentOffset() {
        return this.streamSegmentOffset;
    }

    /**
     * Gets a value indicating the Length of the Transaction StreamSegment.
     *
     * @return The length.
     */
    @Override
    public long getLength() {
        return this.length;
    }

    @Override
    protected OperationType getOperationType() {
        return OperationType.Merge;
    }

    @Override
    protected void serializeContent(DataOutputStream target) throws IOException {
        ensureSerializationCondition(this.length >= 0, "Transaction StreamSegment Length has not been assigned for this entry.");
        ensureSerializationCondition(this.streamSegmentOffset >= 0, "Target StreamSegment Offset has not been assigned for this entry.");

        target.writeByte(VERSION);
        target.writeLong(getStreamSegmentId());
        target.writeLong(this.transactionSegmentId);
        target.writeLong(this.length);
        target.writeLong(this.streamSegmentOffset);
    }

    @Override
    protected void deserializeContent(DataInputStream source) throws IOException, SerializationException {
        readVersion(source, VERSION);
        setStreamSegmentId(source.readLong());
        this.transactionSegmentId = source.readLong();
        this.length = source.readLong();
        this.streamSegmentOffset = source.readLong();
    }

    @Override
    public String toString() {
        return String.format(
                "%s, StreamSegmentId = %d, Length = %s, ParentOffset = %s",
                super.toString(),
                getTransactionSegmentId(),
                toString(getLength(), -1),
                toString(getStreamSegmentOffset(), -1));
    }

    //endregion
}

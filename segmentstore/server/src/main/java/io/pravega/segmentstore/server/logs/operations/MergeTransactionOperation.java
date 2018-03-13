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
import java.io.IOException;

/**
 * Log Operation that indicates a Transaction StreamSegment is merged into its parent StreamSegment.
 */
public class MergeTransactionOperation extends StorageOperation {
    //region Members

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

    /**
     * Deserialization constructor.
     */
    private MergeTransactionOperation() {
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
    public String toString() {
        return String.format(
                "%s, TransactionSegmentId = %d, Length = %s, ParentOffset = %s",
                super.toString(),
                getTransactionSegmentId(),
                toString(getLength(), -1),
                toString(getStreamSegmentOffset(), -1));
    }

    //endregion

    static class Serializer extends OperationSerializer<MergeTransactionOperation> {
        private static final int SERIALIZATION_LENGTH = 5 * Long.BYTES;

        @Override
        protected OperationBuilder<MergeTransactionOperation> newBuilder() {
            return new OperationBuilder<>(new MergeTransactionOperation());
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        @Override
        protected void beforeSerialization(MergeTransactionOperation o) {
            super.beforeSerialization(o);
            Preconditions.checkState(o.length >= 0, "Transaction StreamSegment Length has not been assigned.");
            Preconditions.checkState(o.streamSegmentOffset >= 0, "Target StreamSegment Offset has not been assigned.");
        }

        private void write00(MergeTransactionOperation o, RevisionDataOutput target) throws IOException {
            target.length(SERIALIZATION_LENGTH);
            target.writeLong(o.getSequenceNumber());
            target.writeLong(o.getStreamSegmentId());
            target.writeLong(o.transactionSegmentId);
            target.writeLong(o.length);
            target.writeLong(o.streamSegmentOffset);
        }

        private void read00(RevisionDataInput source, OperationBuilder<MergeTransactionOperation> b) throws IOException {
            b.instance.setSequenceNumber(source.readLong());
            b.instance.setStreamSegmentId(source.readLong());
            b.instance.transactionSegmentId = source.readLong();
            b.instance.length = source.readLong();
            b.instance.streamSegmentOffset = source.readLong();
        }
    }
}

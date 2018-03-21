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
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import java.io.IOException;
import java.util.Collection;

/**
 * Log Operation that indicates a Transaction StreamSegment is merged into its parent StreamSegment.
 */
public class MergeTransactionOperation extends StorageOperation {
    //region Members

    private long streamSegmentOffset;
    private long length;
    private long transactionSegmentId;
    private Collection<AttributeUpdate> attributeUpdates;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the MergeTransactionOperation class.
     *
     * @param streamSegmentId      The Id of the Parent StreamSegment (the StreamSegment to merge into).
     * @param transactionSegmentId The Id of the Transaction StreamSegment (the StreamSegment to be merged).
     * @param attributeUpdates     (Optional) The attributeUpdates to update on the Parent StreamSegment with this operation.
     */
    public MergeTransactionOperation(long streamSegmentId, long transactionSegmentId, Collection<AttributeUpdate> attributeUpdates) {
        super(streamSegmentId);
        this.transactionSegmentId = transactionSegmentId;
        this.attributeUpdates = attributeUpdates;
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

    /**
     * Gets the Attribute updates to be applied to the Parent StreamSegment for this MergeTransactionOperation, if any.
     *
     * @return A Collection of Attribute updates, or null if no updates are available.
     */
    public Collection<AttributeUpdate> getAttributeUpdates() {
        return this.attributeUpdates;
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
        private static final int STATIC_LENGTH = 5 * Long.BYTES;
        private static final int ATTRIBUTE_UPDATE_LENGTH = RevisionDataOutput.UUID_BYTES + Byte.BYTES + 2 * Long.BYTES;

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
            int attributesLength = o.attributeUpdates == null
                    ? target.getCompactIntLength(0)
                    : target.getCollectionLength(o.attributeUpdates.size(), ATTRIBUTE_UPDATE_LENGTH);
            target.length(STATIC_LENGTH + attributesLength);
            target.writeLong(o.getSequenceNumber());
            target.writeLong(o.getStreamSegmentId());
            target.writeLong(o.transactionSegmentId);
            target.writeLong(o.length);
            target.writeLong(o.streamSegmentOffset);
            target.writeCollection(o.attributeUpdates, this::writeAttributeUpdate00);
        }

        private void read00(RevisionDataInput source, OperationBuilder<MergeTransactionOperation> b) throws IOException {
            b.instance.setSequenceNumber(source.readLong());
            b.instance.setStreamSegmentId(source.readLong());
            b.instance.transactionSegmentId = source.readLong();
            b.instance.length = source.readLong();
            b.instance.streamSegmentOffset = source.readLong();
            b.instance.attributeUpdates = source.readCollection(this::readAttributeUpdate00);
        }

        private void writeAttributeUpdate00(RevisionDataOutput target, AttributeUpdate au) throws IOException {
            target.writeUUID(au.getAttributeId());
            target.writeByte(au.getUpdateType().getTypeId());
            target.writeLong(au.getValue());
            target.writeLong(au.getComparisonValue());
        }

        private AttributeUpdate readAttributeUpdate00(RevisionDataInput source) throws IOException {
            return new AttributeUpdate(
                    source.readUUID(),
                    AttributeUpdateType.get(source.readByte()),
                    source.readLong(),
                    source.readLong());
        }
    }
}

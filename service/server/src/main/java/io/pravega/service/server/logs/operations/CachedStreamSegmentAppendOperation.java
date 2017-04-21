/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.service.server.logs.operations;

import io.pravega.service.contracts.AttributeUpdate;
import io.pravega.service.server.logs.SerializationException;
import com.google.common.base.Preconditions;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;

/**
 * Log Operation that represents a StreamSegment Append. As opposed from StreamSegmentAppendOperation, this operation cannot
 * be serialized to a DurableLog. Its purpose is to be added to the In-Memory Transaction Log, where it binds a StreamSegmentAppendOperation
 * to its corresponding Cache entry.
 */
public class CachedStreamSegmentAppendOperation extends StorageOperation {
    //region Members

    private final int length;
    private final long streamSegmentOffset;
    private final Collection<AttributeUpdate> attributeUpdates;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the CachedStreamSegmentAppendOperation based on the given StreamSegmentAppendOperation.
     * The created operation will have the same SequenceNumber, StreamSegmentId, Offset and Length as the base operation,
     * but it will not directly store the data (the contents of the Append is stored in the Cache, and will have to be
     * retrieved using properties of this object).
     *
     * @param baseOperation The StreamSegmentAppendOperation to use.
     */
    public CachedStreamSegmentAppendOperation(StreamSegmentAppendOperation baseOperation) {
        super(baseOperation.getStreamSegmentId());
        Preconditions.checkArgument(baseOperation.getStreamSegmentOffset() >= 0, "given baseOperation does not have an assigned StreamSegment Offset.");

        this.streamSegmentOffset = baseOperation.getStreamSegmentOffset();
        this.length = baseOperation.getData().length;
        if (baseOperation.getSequenceNumber() >= 0) {
            setSequenceNumber(baseOperation.getSequenceNumber());
        }

        this.attributeUpdates = baseOperation.getAttributeUpdates();
    }

    //endregion

    //region Properties

    /**
     * Gets the Attribute updates for this StreamSegmentAppendOperation, if any.
     *
     * @return A Collection of Attribute updates, or null if no updates are available.
     */
    public Collection<AttributeUpdate> getAttributeUpdates() {
        return this.attributeUpdates;
    }

    @Override
    public String toString() {
        return String.format(
                "%s, Offset = %d, Length = %d, Attributes = %d",
                super.toString(),
                this.streamSegmentOffset,
                this.length,
                this.attributeUpdates == null ? 0 : this.attributeUpdates.size());
    }

    //endregion

    //region Operation Implementation

    @Override
    public long getStreamSegmentOffset() {
        return this.streamSegmentOffset;
    }

    @Override
    public long getLength() {
        return this.length;
    }

    @Override
    protected OperationType getOperationType() {
        throw new UnsupportedOperationException(this.getClass().getSimpleName() + " cannot be serialized, thus it does not have an Operation Type.");
    }

    @Override
    protected void serializeContent(DataOutputStream target) throws IOException {
        throw new UnsupportedOperationException(this.getClass().getSimpleName() + " cannot be serialized.");
    }

    @Override
    protected void deserializeContent(DataInputStream source) throws IOException, SerializationException {
        throw new UnsupportedOperationException(this.getClass().getSimpleName() + " cannot be deserialized.");
    }

    //endregion
}

/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package io.pravega.service.server.logs.operations;

import io.pravega.service.contracts.AttributeUpdate;
import io.pravega.service.server.AttributeSerializer;
import io.pravega.service.server.logs.SerializationException;
import com.google.common.base.Preconditions;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;

import lombok.Getter;

/**
 * Log Operation that represents an Update to a Segment's Attribute collection.
 */
public class UpdateAttributesOperation extends MetadataOperation implements SegmentOperation {
    //region Members

    private static final byte CURRENT_VERSION = 0;
    @Getter
    private long streamSegmentId;
    @Getter
    private Collection<AttributeUpdate> attributeUpdates;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the UpdateAttributesOperation class.
     *
     * @param streamSegmentId  The Id of the StreamSegment for which to update attributes.
     * @param attributeUpdates A Collection of AttributeUpdates to apply.
     */
    public UpdateAttributesOperation(long streamSegmentId, Collection<AttributeUpdate> attributeUpdates) {
        super();
        Preconditions.checkNotNull(attributeUpdates, "attributeUpdates");

        this.streamSegmentId = streamSegmentId;
        this.attributeUpdates = attributeUpdates;
    }

    protected UpdateAttributesOperation(OperationHeader header, DataInputStream source) throws SerializationException {
        super(header, source);
    }

    //endregion

    //region Operation Implementation

    @Override
    protected OperationType getOperationType() {
        return OperationType.UpdateAttributes;
    }

    @Override
    protected void serializeContent(DataOutputStream target) throws IOException {
        target.writeByte(CURRENT_VERSION);
        target.writeLong(this.streamSegmentId);
        AttributeSerializer.serializeUpdates(this.attributeUpdates, target);
    }

    @Override
    protected void deserializeContent(DataInputStream source) throws IOException, SerializationException {
        readVersion(source, CURRENT_VERSION);
        this.streamSegmentId = source.readLong();
        this.attributeUpdates = AttributeSerializer.deserializeUpdates(source);
    }

    @Override
    public String toString() {
        return String.format("%s, SegmentId = %d, Attributes = %d", super.toString(), this.streamSegmentId, this.attributeUpdates.size());
    }

    //endregion
}

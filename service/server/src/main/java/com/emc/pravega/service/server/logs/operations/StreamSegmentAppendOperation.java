/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server.logs.operations;

import com.emc.pravega.common.io.StreamHelpers;
import com.emc.pravega.service.contracts.AttributeUpdate;
import com.emc.pravega.service.server.AttributeSerializer;
import com.emc.pravega.service.server.logs.SerializationException;
import com.google.common.base.Preconditions;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;

/**
 * Log Operation that represents a StreamSegment Append. This operation, as opposed from CachedStreamSegmentAppendOperation,
 * can be serialized to a DurableDataLog. This operation (although possible), should not be directly added to the In-Memory Transaction Log.
 */
public class StreamSegmentAppendOperation extends StorageOperation {
    //region Members

    private static final long NO_OFFSET = -1;
    private static final byte CURRENT_VERSION = 0;
    private long streamSegmentOffset;
    private byte[] data;
    private Collection<AttributeUpdate> attributeUpdates;

    //endregion

    // region Constructor

    /**
     * Creates a new instance of the StreamSegmentAppendOperation class.
     *
     * @param streamSegmentId  The Id of the StreamSegment to append to.
     * @param data             The payload to append.
     * @param attributeUpdates (Optional) The attributeUpdates to update with this append.
     */
    public StreamSegmentAppendOperation(long streamSegmentId, byte[] data, Collection<AttributeUpdate> attributeUpdates) {
        this(streamSegmentId, NO_OFFSET, data, attributeUpdates);
    }

    /**
     * Creates a new instance of the StreamSegmentAppendOperation class.
     *
     * @param streamSegmentId  The Id of the StreamSegment to append to.
     * @param offset           The offset to append at.
     * @param data             The payload to append.
     * @param attributeUpdates (Optional) The attributeUpdates to update with this append.
     */
    public StreamSegmentAppendOperation(long streamSegmentId, long offset, byte[] data, Collection<AttributeUpdate> attributeUpdates) {
        super(streamSegmentId);
        Preconditions.checkNotNull(data, "data");

        this.data = data;
        this.streamSegmentOffset = offset;
        this.attributeUpdates = attributeUpdates;
    }

    protected StreamSegmentAppendOperation(OperationHeader header, DataInputStream source) throws SerializationException {
        super(header, source);
    }

    //endregion

    //region StreamSegmentAppendOperation Properties

    /**
     * Sets the Offset in the StreamSegment to append at.
     *
     * @param value The offset.
     */
    public void setStreamSegmentOffset(long value) {
        // No need for parameter validation here. We allow even invalid offsets now - we will check for them upon serialization.
        this.streamSegmentOffset = value;
    }

    /**
     * Gets the data buffer for this append.
     *
     * @return The data buffer.
     */
    public byte[] getData() {
        return this.data;
    }

    /**
     * Gets the Attribute updates for this StreamSegmentAppendOperation, if any.
     *
     * @return A Collection of Attribute updates, or null if no updates are available.
     */
    public Collection<AttributeUpdate> getAttributeUpdates() {
        return this.attributeUpdates;
    }

    //endregion

    //region Operation Implementation

    @Override
    public long getStreamSegmentOffset() {
        return this.streamSegmentOffset;
    }

    @Override
    public long getLength() {
        return this.data.length;
    }

    @Override
    protected OperationType getOperationType() {
        return OperationType.Append;
    }

    @Override
    protected void serializeContent(DataOutputStream target) throws IOException {
        ensureSerializationCondition(this.streamSegmentOffset >= 0, "StreamSegment Offset has not been assigned for this entry.");

        target.writeByte(CURRENT_VERSION);
        target.writeLong(getStreamSegmentId());
        target.writeLong(this.streamSegmentOffset);
        target.writeInt(data.length);
        target.write(data, 0, data.length);
        AttributeSerializer.serializeUpdates(this.attributeUpdates, target);
    }

    @Override
    protected void deserializeContent(DataInputStream source) throws IOException, SerializationException {
        readVersion(source, CURRENT_VERSION);
        setStreamSegmentId(source.readLong());
        this.streamSegmentOffset = source.readLong();
        int dataLength = source.readInt();
        this.data = new byte[dataLength];
        int bytesRead = StreamHelpers.readAll(source, this.data, 0, this.data.length);
        assert bytesRead == this.data.length : "StreamHelpers.readAll did not read all the bytes requested.";
        this.attributeUpdates = AttributeSerializer.deserializeUpdates(source);
    }

    @Override
    public String toString() {
        return String.format(
                "%s, Offset = %s, Length = %d, Attributes = %d",
                super.toString(),
                toString(this.streamSegmentOffset, -1),
                this.data.length,
                this.attributeUpdates == null ? 0 : this.attributeUpdates.size());
    }

    //endregion
}

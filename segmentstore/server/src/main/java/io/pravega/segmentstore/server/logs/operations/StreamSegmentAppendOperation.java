/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.server.logs.operations;

import com.google.common.base.Preconditions;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import java.io.IOException;
import java.util.Collection;

/**
 * Log Operation that represents a StreamSegment Append. This operation, as opposed from CachedStreamSegmentAppendOperation,
 * can be serialized to a DurableDataLog. This operation (although possible), should not be directly added to the In-Memory Transaction Log.
 */
public class StreamSegmentAppendOperation extends StorageOperation implements AttributeUpdaterOperation, AutoCloseable {
    //region Members

    private static final long NO_OFFSET = -1;
    private long streamSegmentOffset;
    private BufferView data;
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
    public StreamSegmentAppendOperation(long streamSegmentId, BufferView data, Collection<AttributeUpdate> attributeUpdates) {
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
    public StreamSegmentAppendOperation(long streamSegmentId, long offset, BufferView data, Collection<AttributeUpdate> attributeUpdates) {
        super(streamSegmentId);
        this.data = Preconditions.checkNotNull(data, "data");
        this.data.retain(); // Hold this buffer in memory until we are done with it.
        this.streamSegmentOffset = offset;
        this.attributeUpdates = attributeUpdates;
    }

    /**
     * Deserialization constructor.
     */
    private StreamSegmentAppendOperation() {
    }

    @Override
    public void close() {
        this.data.release();
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
    public BufferView getData() {
        return this.data;
    }

    /**
     * Gets the Attribute updates for this StreamSegmentAppendOperation, if any.
     *
     * @return A Collection of Attribute updates, or null if no updates are available.
     */
    @Override
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
        return this.data.getLength();
    }

    @Override
    public long getCacheLength() {
        return this.data.getLength();
    }

    @Override
    public String toString() {
        return String.format(
                "%s, Offset = %s, Length = %d, Attributes = %d",
                super.toString(),
                toString(this.streamSegmentOffset, -1),
                this.data.getLength(),
                this.attributeUpdates == null ? 0 : this.attributeUpdates.size());
    }

    //endregion

    static class Serializer extends OperationSerializer<StreamSegmentAppendOperation> {
        private static final int STATIC_LENGTH = 3 * Long.BYTES;
        private static final int ATTRIBUTE_UPDATE_LENGTH = RevisionDataOutput.UUID_BYTES + Byte.BYTES + 2 * Long.BYTES;

        @Override
        protected OperationBuilder<StreamSegmentAppendOperation> newBuilder() {
            return new OperationBuilder<>(new StreamSegmentAppendOperation());
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
        protected void beforeSerialization(StreamSegmentAppendOperation o) {
            super.beforeSerialization(o);
            Preconditions.checkState(o.streamSegmentOffset >= 0, "StreamSegment Offset has not been assigned.");
        }

        private void write00(StreamSegmentAppendOperation o, RevisionDataOutput target) throws IOException {
            int attributesLength = o.attributeUpdates == null ? target.getCompactIntLength(0) : target.getCollectionLength(o.attributeUpdates.size(), ATTRIBUTE_UPDATE_LENGTH);
            int dataLength = o.getData().getLength();
            target.length(STATIC_LENGTH + target.getCompactIntLength(dataLength) + dataLength + attributesLength);
            target.writeLong(o.getSequenceNumber());
            target.writeLong(o.getStreamSegmentId());
            target.writeLong(o.streamSegmentOffset);
            target.writeBuffer(o.data);
            target.writeCollection(o.attributeUpdates, this::writeAttributeUpdate00);
        }

        private void read00(RevisionDataInput source, OperationBuilder<StreamSegmentAppendOperation> b) throws IOException {
            b.instance.setSequenceNumber(source.readLong());
            b.instance.setStreamSegmentId(source.readLong());
            b.instance.streamSegmentOffset = source.readLong();
            b.instance.data = new ByteArraySegment(source.readArray()); // No need to invoke BufferView.retain() here.
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

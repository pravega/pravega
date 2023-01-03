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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;

/**
 * Log Operation that represents a StreamSegment Append. This operation, as opposed from CachedStreamSegmentAppendOperation,
 * can be serialized to a DurableDataLog. This operation (although possible), should not be directly added to the In-Memory Transaction Log.
 */
public class StreamSegmentAppendOperation extends StorageOperation implements AttributeUpdaterOperation, AutoCloseable {
    //region Members

    // Represents that no hash has been computed for a given Append contents.
    public static final long NO_HASH = Long.MIN_VALUE;

    private static final long NO_OFFSET = -1;
    protected long streamSegmentOffset;
    protected BufferView data;
    protected AttributeUpdateCollection attributeUpdates;

    // Hash of the Append contents. Note that this is only used internally within the Segment Store to
    // check data integrity and should not be considered for serialization (i.e., transient field).
    @Getter
    @Setter
    private transient long contentHash = NO_HASH;

    //endregion

    // region Constructor

    /**
     * Creates a new instance of the StreamSegmentAppendOperation class.
     *
     * @param streamSegmentId  The Id of the StreamSegment to append to.
     * @param data             The payload to append.
     * @param attributeUpdates (Optional) The attributeUpdates to update with this append.
     */
    public StreamSegmentAppendOperation(long streamSegmentId, BufferView data, AttributeUpdateCollection attributeUpdates) {
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
    public StreamSegmentAppendOperation(long streamSegmentId, long offset, BufferView data, AttributeUpdateCollection attributeUpdates) {
        super(streamSegmentId);
        this.data = Preconditions.checkNotNull(data, "data");
        this.data.retain(); // Hold this buffer in memory until we are done with it.
        this.streamSegmentOffset = offset;
        this.attributeUpdates = attributeUpdates;
    }

    /**
     * Deserialization constructor.
     */
    protected StreamSegmentAppendOperation() {
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
    public AttributeUpdateCollection getAttributeUpdates() {
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

    /**
     * Version 0 Serializer. Retired in Release 0.10.0. Should keep around until at least 2 releases afterwards.
     * Currently "frozen" to validate backwards compatibility.
     * See {@link Serializer} for more details on current version.
     */
    @VisibleForTesting
    static class SerializerV0 extends OperationSerializer<StreamSegmentAppendOperation> {
        protected static final int STATIC_LENGTH = 3 * Long.BYTES;
        protected static final int ATTRIBUTE_UPDATE_LENGTH_V0 = 2 * Long.BYTES + Byte.BYTES + 2 * Long.BYTES;

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

        protected void write00(StreamSegmentAppendOperation o, RevisionDataOutput target) throws IOException {
            int attributesLength = o.attributeUpdates == null ? target.getCompactIntLength(0) : target.getCollectionLength(o.attributeUpdates.size(), ATTRIBUTE_UPDATE_LENGTH_V0);
            int dataLength = o.getData().getLength();
            target.length(STATIC_LENGTH + target.getCompactIntLength(dataLength) + dataLength + attributesLength);
            target.writeLong(o.getSequenceNumber());
            target.writeLong(o.getStreamSegmentId());
            target.writeLong(o.streamSegmentOffset);
            target.writeBuffer(o.data);
            target.writeCollection(o.attributeUpdates, this::writeAttributeUpdate00);
        }

        protected void read00(RevisionDataInput source, OperationBuilder<StreamSegmentAppendOperation> b) throws IOException {
            b.instance.setSequenceNumber(source.readLong());
            b.instance.setStreamSegmentId(source.readLong());
            b.instance.streamSegmentOffset = source.readLong();
            b.instance.data = new ByteArraySegment(source.readArray()); // No need to invoke BufferView.retain() here.
            b.instance.attributeUpdates = source.readCollection(this::readAttributeUpdate00, AttributeUpdateCollection::new);
        }

        protected void writeAttributeUpdate00(RevisionDataOutput target, AttributeUpdate au) throws IOException {
            assert au.getAttributeId().isUUID(); // This will fail any tests but not force a check in production code.
            target.writeLong(au.getAttributeId().getBitGroup(0));
            target.writeLong(au.getAttributeId().getBitGroup(1));
            target.writeByte(au.getUpdateType().getTypeId());
            target.writeLong(au.getValue());
            target.writeLong(au.getComparisonValue());
        }

        protected AttributeUpdate readAttributeUpdate00(RevisionDataInput source) throws IOException {
            return new AttributeUpdate(
                    AttributeId.uuid(source.readLong(), source.readLong()),
                    AttributeUpdateType.get(source.readByte()),
                    source.readLong(),
                    source.readLong());
        }
    }

    static class Serializer extends SerializerV0 {
        /**
         * Fixed portion:
         * - AttributeUpdateType: 1 byte
         * - Value: 8 bytes
         * - We do not encode the compare value anymore. There is no need post-serialization/validation.
         */
        private static final int ATTRIBUTE_UPDATE_LENGTH_FIXED = Byte.BYTES + Long.BYTES;
        /**
         * Attribute Update Encoding Layout:
         * - UUID MSB (8 bytes)
         * - UUID LSB (8 bytes)
         * - Update Type + Value: {@link #ATTRIBUTE_UPDATE_LENGTH_FIXED}.
         * - We do not encode CompareValue as we have already validated it at this point so no need for it again.
         */
        private static final int ATTRIBUTE_UPDATE_LENGTH_UUID_V1 = 2 * Long.BYTES + ATTRIBUTE_UPDATE_LENGTH_FIXED;

        @Override
        protected OperationBuilder<StreamSegmentAppendOperation> newBuilder() {
            return super.newBuilder();
        }

        @Override
        protected byte getWriteVersion() {
            // We still encode in version 0 for backwards compatibility; we'll switch to version 1 in a future release.
            return super.getWriteVersion();
        }

        @Override
        protected void declareVersions() {
            super.declareVersions();
            version(1).revision(0, this::write10, this::read10);
        }

        @Override
        protected void beforeSerialization(StreamSegmentAppendOperation o) {
            super.beforeSerialization(o);
        }

        @Override
        protected void write00(StreamSegmentAppendOperation o, RevisionDataOutput target) throws IOException {
            // Overriding version 0 for writes in a manner to encode variable attribute ids as well.
            // Keeping here for backwards compatibility only.
            write(o, target, ATTRIBUTE_UPDATE_LENGTH_V0, this::writeAttributeUpdate00);
        }

        private void write10(StreamSegmentAppendOperation o, RevisionDataOutput target) throws IOException {
            write(o, target, ATTRIBUTE_UPDATE_LENGTH_UUID_V1, this::writeAttributeUpdateUUID10);
        }

        private void write(StreamSegmentAppendOperation o, RevisionDataOutput target, int attributeUUIDLength,
                           RevisionDataOutput.ElementSerializer<AttributeUpdate> serializeAttributeUUID) throws IOException {
            int attributesLength;
            if (o.attributeUpdates == null) {
                attributesLength = 2 * target.getCompactIntLength(0); // UUIDs and Variables are stored separately.
            } else {
                attributesLength = target.getCollectionLength(o.attributeUpdates.getUUIDAttributeUpdates().size(), attributeUUIDLength);
                attributesLength += o.attributeUpdates.hasVariableAttributeIds()
                        ? target.getCollectionLength(o.attributeUpdates.getVariableAttributeUpdates(), au -> getAttributeUpdateLengthVariable(target, au))
                        : target.getCompactIntLength(0);
            }
            int dataLength = o.getData().getLength();
            target.length(STATIC_LENGTH + target.getCompactIntLength(dataLength) + dataLength + attributesLength);
            target.writeLong(o.getSequenceNumber());
            target.writeLong(o.getStreamSegmentId());
            target.writeLong(o.streamSegmentOffset);
            target.writeBuffer(o.data);
            if (o.attributeUpdates == null) {
                target.writeCollection(null, serializeAttributeUUID);
                target.writeCollection(null, this::writeAttributeUpdateVariable10);
            } else {
                target.writeCollection(o.attributeUpdates.getUUIDAttributeUpdates(), serializeAttributeUUID);
                target.writeCollection(o.attributeUpdates.getVariableAttributeUpdates(), this::writeAttributeUpdateVariable10);
            }
        }

        private int getAttributeUpdateLengthVariable(RevisionDataOutput target, AttributeUpdate au) {
            return target.getCompactIntLength(au.getAttributeId().byteCount()) + au.getAttributeId().byteCount() + ATTRIBUTE_UPDATE_LENGTH_FIXED;
        }

        @Override
        protected void read00(RevisionDataInput source, OperationBuilder<StreamSegmentAppendOperation> b) throws IOException {
            read(source, b, this::readAttributeUpdate00);
        }

        private void read10(RevisionDataInput source, OperationBuilder<StreamSegmentAppendOperation> b) throws IOException {
            read(source, b, this::readAttributeUpdateUUID10);
        }

        private void read(RevisionDataInput source, OperationBuilder<StreamSegmentAppendOperation> b,
                          RevisionDataInput.ElementDeserializer<AttributeUpdate> readAttributeUUID) throws IOException {
            b.instance.setSequenceNumber(source.readLong());
            b.instance.setStreamSegmentId(source.readLong());
            b.instance.streamSegmentOffset = source.readLong();
            b.instance.data = new ByteArraySegment(source.readArray()); // No need to invoke BufferView.retain() here.
            b.instance.attributeUpdates = source.readCollection(readAttributeUUID, AttributeUpdateCollection::new);
            if (source.getRemaining() > 0) {
                // Backwards compatibility with Version 0. This check (but not its contents) can be removed once we stop
                // supporting Version 0.
                source.readCollection(this::readAttributeUpdateVariable10, b.instance::getAttributeUpdates);
            }
        }

        private void writeAttributeUpdateUUID10(RevisionDataOutput target, AttributeUpdate au) throws IOException {
            assert au.getAttributeId().isUUID(); // This will fail any tests but not force a check in production code.
            target.writeLong(au.getAttributeId().getBitGroup(0));
            target.writeLong(au.getAttributeId().getBitGroup(1));
            target.writeByte(au.getUpdateType().getTypeId());
            target.writeLong(au.getValue());
        }

        private void writeAttributeUpdateVariable10(RevisionDataOutput target, AttributeUpdate au) throws IOException {
            target.writeBuffer(au.getAttributeId().toBuffer());
            target.writeByte(au.getUpdateType().getTypeId());
            target.writeLong(au.getValue());
        }

        private AttributeUpdate readAttributeUpdateUUID10(RevisionDataInput source) throws IOException {
            return new AttributeUpdate(
                    AttributeId.uuid(source.readLong(), source.readLong()),
                    AttributeUpdateType.get(source.readByte()),
                    source.readLong(),
                    Long.MIN_VALUE); // We do not encode this for serialization, so we don't care about it upon deserialization.
        }

        private AttributeUpdate readAttributeUpdateVariable10(RevisionDataInput source) throws IOException {
            return new AttributeUpdate(
                    AttributeId.from(source.readArray()),
                    AttributeUpdateType.get(source.readByte()),
                    source.readLong(),
                    Long.MIN_VALUE); // We do not encode this for serialization, so we don't care about it upon deserialization.
        }

    }
}

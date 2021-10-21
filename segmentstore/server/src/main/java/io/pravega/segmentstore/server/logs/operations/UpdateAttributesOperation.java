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
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import java.io.IOException;
import java.util.Collection;
import lombok.Getter;
import lombok.Setter;

/**
 * Log Operation that represents an Update to a Segment's Attribute collection.
 */
public class UpdateAttributesOperation extends MetadataOperation implements AttributeUpdaterOperation {
    //region Members

    @Getter
    private long streamSegmentId;
    @Getter
    private AttributeUpdateCollection attributeUpdates;
    /**
     * Whether this operation is internally generated.
     * This field is not serialized; upon deserialization (recovery) all instances will be marked as Internal (true).
     */
    @Getter
    @Setter
    private boolean internal;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the UpdateAttributesOperation class.
     *
     * @param streamSegmentId  The Id of the StreamSegment for which to update attributes.
     * @param attributeUpdates A Collection of AttributeUpdates to apply.
     */
    public UpdateAttributesOperation(long streamSegmentId, AttributeUpdateCollection attributeUpdates) {
        super();
        Preconditions.checkNotNull(attributeUpdates, "attributeUpdates");

        this.streamSegmentId = streamSegmentId;
        this.attributeUpdates = attributeUpdates;
    }

    /**
     * Deserialization constructor.
     */
    private UpdateAttributesOperation() {
    }

    //endregion

    //region Operation Implementation

    @Override
    public String toString() {
        return String.format("%s, SegmentId = %d, Attributes = %d", super.toString(), this.streamSegmentId, this.attributeUpdates.size());
    }

    //endregion

    static class Serializer extends OperationSerializer<UpdateAttributesOperation> {
        private static final int STATIC_LENGTH = 2 * Long.BYTES;
        private static final int ATTRIBUTE_UUID_UPDATE_LENGTH = RevisionDataOutput.UUID_BYTES + Byte.BYTES + 2 * Long.BYTES;
        /**
         * Fixed portion:
         * - AttributeUpdateType: 1 byte
         * - Value: 8 bytes
         * - We do not encode the compare value anymore. There is no need post-serialization/validation.
         */
        private static final int ATTRIBUTE_NON_UUID_UPDATE_LENGTH_FIXED = Byte.BYTES + Long.BYTES;

        @Override
        protected OperationBuilder<UpdateAttributesOperation> newBuilder() {
            return new OperationBuilder<>(new UpdateAttributesOperation());
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00) // Backwards compatible with UUID attributes.
                    .revision(1, this::write01, this::read01);  // Stores non-UUID attribute updates.
        }

        private void write00(UpdateAttributesOperation o, RevisionDataOutput target) throws IOException {
            Collection<AttributeUpdate> updates = o.attributeUpdates == null ? null : o.attributeUpdates.getUUIDAttributeUpdates();

            int attributesLength = updates == null ? target.getCompactIntLength(0) : target.getCollectionLength(updates.size(), ATTRIBUTE_UUID_UPDATE_LENGTH);
            target.length(STATIC_LENGTH + attributesLength);
            target.writeLong(o.getSequenceNumber());
            target.writeLong(o.streamSegmentId);
            target.writeCollection(updates, this::writeAttributeUpdateUUID00);
        }

        private void read00(RevisionDataInput source, OperationBuilder<UpdateAttributesOperation> b) throws IOException {
            b.instance.setSequenceNumber(source.readLong());
            b.instance.streamSegmentId = source.readLong();
            b.instance.attributeUpdates = source.readCollection(this::readAttributeUpdateUUID00, AttributeUpdateCollection::new);
            b.instance.internal = true;
        }

        private void writeAttributeUpdateUUID00(RevisionDataOutput target, AttributeUpdate au) throws IOException {
            target.writeLong(au.getAttributeId().getBitGroup(0));
            target.writeLong(au.getAttributeId().getBitGroup(1));
            target.writeByte(au.getUpdateType().getTypeId());
            target.writeLong(au.getValue());
            target.writeLong(au.getComparisonValue());
        }

        private AttributeUpdate readAttributeUpdateUUID00(RevisionDataInput source) throws IOException {
            return new AttributeUpdate(
                    AttributeId.uuid(source.readLong(), source.readLong()),
                    AttributeUpdateType.get(source.readByte()),
                    source.readLong(),
                    source.readLong());
        }

        private void write01(UpdateAttributesOperation o, RevisionDataOutput target) throws IOException {
            if (o.attributeUpdates == null) {
                target.length(target.getCompactIntLength(0));
                return;
            }

            Collection<AttributeUpdate> updates = o.attributeUpdates.getVariableAttributeUpdates();
            int attributesLength = target.getCollectionLength(updates, au -> getAttributeUpdateNonUUIDLength(target, au));
            target.length(attributesLength);
            target.writeCollection(updates, this::writeAttributeUpdateVariable01);
        }

        private void read01(RevisionDataInput source, OperationBuilder<UpdateAttributesOperation> b) throws IOException {
            source.readCollection(this::readAttributeUpdateVariable01, b.instance::getAttributeUpdates);
        }

        private void writeAttributeUpdateVariable01(RevisionDataOutput target, AttributeUpdate au) throws IOException {
            target.writeBuffer(au.getAttributeId().toBuffer());
            target.writeByte(au.getUpdateType().getTypeId());
            target.writeLong(au.getValue());
        }

        private AttributeUpdate readAttributeUpdateVariable01(RevisionDataInput source) throws IOException {
            return new AttributeUpdate(
                    AttributeId.from(source.readArray()),
                    AttributeUpdateType.get(source.readByte()),
                    source.readLong(),
                    Long.MIN_VALUE); // We do not encode this for serialization, so we don't care about it upon deserialization.
        }

        private int getAttributeUpdateNonUUIDLength(RevisionDataOutput target, AttributeUpdate au) {
            return target.getCompactIntLength(au.getAttributeId().byteCount()) + au.getAttributeId().byteCount() + ATTRIBUTE_NON_UUID_UPDATE_LENGTH_FIXED;
        }
    }
}

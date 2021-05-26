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
import io.pravega.segmentstore.contracts.AttributeUpdate;
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
    private Collection<AttributeUpdate> attributeUpdates;
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
    public UpdateAttributesOperation(long streamSegmentId, Collection<AttributeUpdate> attributeUpdates) {
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
        private static final int ATTRIBUTE_UPDATE_LENGTH = RevisionDataOutput.UUID_BYTES + Byte.BYTES + 2 * Long.BYTES;

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
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(UpdateAttributesOperation o, RevisionDataOutput target) throws IOException {
            int attributesLength = o.attributeUpdates == null ? target.getCompactIntLength(0) : target.getCollectionLength(o.attributeUpdates.size(), ATTRIBUTE_UPDATE_LENGTH);
            target.length(STATIC_LENGTH + attributesLength);
            target.writeLong(o.getSequenceNumber());
            target.writeLong(o.streamSegmentId);
            target.writeCollection(o.attributeUpdates, this::writeAttributeUpdate00);
        }

        private void read00(RevisionDataInput source, OperationBuilder<UpdateAttributesOperation> b) throws IOException {
            b.instance.setSequenceNumber(source.readLong());
            b.instance.streamSegmentId = source.readLong();
            b.instance.attributeUpdates = source.readCollection(this::readAttributeUpdate00);
            b.instance.internal = true;
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

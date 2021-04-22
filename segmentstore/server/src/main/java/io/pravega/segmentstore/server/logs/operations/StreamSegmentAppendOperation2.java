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
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import java.io.IOException;
import java.util.Collection;

public class StreamSegmentAppendOperation2 extends StreamSegmentAppendOperation {
    public StreamSegmentAppendOperation2(long streamSegmentId, BufferView data, Collection<AttributeUpdate> attributeUpdates) {
        super(streamSegmentId, data, attributeUpdates);
    }

    public StreamSegmentAppendOperation2(long streamSegmentId, long offset, BufferView data, Collection<AttributeUpdate> attributeUpdates) {
        super(streamSegmentId, offset, data, attributeUpdates);
    }

    /**
     * Deserialization constructor.
     */
    private StreamSegmentAppendOperation2() {
    }

    static class Serializer extends OperationSerializer<StreamSegmentAppendOperation2> {
        private static final int STATIC_LENGTH = 3 * Long.BYTES;
        /**
         * Fixed portion:
         * - AttributeUpdateType: 1 byte
         * - Value: 8 bytes
         * - We do not encode the compare value anymore. There is no need post-serialization/validation.
         */
        private static final int ATTRIBUTE_UPDATE_LENGTH_FIXED = Byte.BYTES + Long.BYTES;

        @Override
        protected OperationBuilder<StreamSegmentAppendOperation2> newBuilder() {
            return new OperationBuilder<>(new StreamSegmentAppendOperation2());
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
        protected void beforeSerialization(StreamSegmentAppendOperation2 o) {
            super.beforeSerialization(o);
            Preconditions.checkState(o.streamSegmentOffset >= 0, "StreamSegment Offset has not been assigned.");
        }

        private void write00(StreamSegmentAppendOperation2 o, RevisionDataOutput target) throws IOException {
            int attributesLength = o.attributeUpdates == null
                    ? target.getCompactIntLength(0)
                    : target.getCollectionLength(o.attributeUpdates, au -> getAttributeUpdateLength(target, au));
            int dataLength = o.getData().getLength();
            target.length(STATIC_LENGTH + target.getCompactIntLength(dataLength) + dataLength + attributesLength);
            target.writeLong(o.getSequenceNumber());
            target.writeLong(o.getStreamSegmentId());
            target.writeLong(o.streamSegmentOffset);
            target.writeBuffer(o.data);
            target.writeCollection(o.attributeUpdates, this::writeAttributeUpdate00);
        }

        private int getAttributeUpdateLength(RevisionDataOutput target, AttributeUpdate au) {
            return target.getCompactIntLength(au.getAttributeId().byteCount()) + au.getAttributeId().byteCount() + ATTRIBUTE_UPDATE_LENGTH_FIXED;
        }

        private void read00(RevisionDataInput source, OperationBuilder<StreamSegmentAppendOperation2> b) throws IOException {
            b.instance.setSequenceNumber(source.readLong());
            b.instance.setStreamSegmentId(source.readLong());
            b.instance.streamSegmentOffset = source.readLong();
            b.instance.data = new ByteArraySegment(source.readArray()); // No need to invoke BufferView.retain() here.
            b.instance.attributeUpdates = source.readCollection(this::readAttributeUpdate00);
        }

        private void writeAttributeUpdate00(RevisionDataOutput target, AttributeUpdate au) throws IOException {
            target.writeBuffer(au.getAttributeId().toBuffer());
            target.writeByte(au.getUpdateType().getTypeId());
            target.writeLong(au.getValue());
        }

        private AttributeUpdate readAttributeUpdate00(RevisionDataInput source) throws IOException {
            return new AttributeUpdate(
                    AttributeId.from(source.readArray()),
                    AttributeUpdateType.get(source.readByte()),
                    source.readLong(),
                    Long.MIN_VALUE); // We do not encode this for serialization, so we don't care about it upon deserialization.
        }
    }
}
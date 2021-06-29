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

import io.pravega.common.MathHelpers;
import io.pravega.common.hash.RandomFactory;
import io.pravega.common.io.SerializationException;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.test.common.AssertExtensions;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for StreamSegmentAppendOperation class.
 */
public class StreamSegmentAppendOperationTests extends OperationTestsBase<StreamSegmentAppendOperation> {
    private static final int MIN_LENGTH = 1;
    private static final int MAX_LENGTH = 1024 * 1024;

    @Override
    protected StreamSegmentAppendOperation createOperation(Random random) {
        byte[] data = new byte[random.nextInt(MAX_LENGTH - MIN_LENGTH) + MIN_LENGTH];
        random.nextBytes(data);
        val attributes = createAttributes();
        return new StreamSegmentAppendOperation(random.nextLong(), new ByteArraySegment(data), attributes);
    }

    @Override
    protected boolean isPreSerializationConfigRequired(StreamSegmentAppendOperation operation) {
        return operation.getStreamSegmentOffset() < 0;
    }

    @Override
    protected void configurePreSerialization(StreamSegmentAppendOperation operation, Random random) {
        if (operation.getStreamSegmentOffset() < 0) {
            operation.setStreamSegmentOffset(MathHelpers.abs(random.nextLong()));
        } else if (isPreSerializationConfigRequired(operation)) {
            Assert.fail("isPreSerializationConfigRequired returned true but there is nothing to be done.");
        }
    }

    static AttributeUpdateCollection createAttributes() {
        val result = new AttributeUpdateCollection();
        long currentValue = 0;

        for (AttributeUpdateType ut : AttributeUpdateType.values()) {
            result.add(new AttributeUpdate(AttributeId.uuid(Attributes.CORE_ATTRIBUTE_ID_PREFIX, ut.getTypeId()), ut, ++currentValue, currentValue));
            result.add(new AttributeUpdate(AttributeId.random(AttributeId.Variable.MAX_LENGTH), ut, ++currentValue, currentValue));
        }

        return result;
    }

    /**
     * Tests the following scenarios:
     * - Serialization and deserialization using V1.
     * - Serialization using V1 and deserialization using V0.
     * - Serialization using V0 and deserialization using V1.
     * <p>
     * NOTE: {@link #testSerialization()} validates that {@link StreamSegmentAppendOperation.Serializer} works well
     * with default settings (for normal scenarios). This method validates backwards compatibility.
     */
    @Test
    public void testSerializationV0V1() throws IOException {
        val defaultSerializer = new StreamSegmentAppendOperation.Serializer(); // Writes V0, reads both V0, V1.
        val serializerV0 = new StreamSegmentAppendOperation.SerializerV0();    // Writes and reads V0 only.
        val serializerV1 = new SerializerV1();                                 // Writes V1, reads both V0, V1.
        val random = RandomFactory.create();
        val operations = Arrays.asList(
                createOperation(random),
                new StreamSegmentAppendOperation(1, new ByteArraySegment(new byte[1]), null));
        for (val baseOp : operations) {
            val baseOpUUIDOnly = new StreamSegmentAppendOperation(baseOp.getStreamSegmentId(), baseOp.getData(),
                    baseOp.getAttributeUpdates() == null ? null : AttributeUpdateCollection.from(baseOp.getAttributeUpdates().getUUIDAttributeUpdates()));
            baseOp.setSequenceNumber(MathHelpers.abs(random.nextLong()));
            baseOpUUIDOnly.setSequenceNumber(baseOp.getSequenceNumber());
            configurePreSerialization(baseOp, random);
            baseOpUUIDOnly.setStreamSegmentOffset(baseOp.getStreamSegmentOffset());

            // 1. Deserialize the V0 serialization using ALL serializers. This simulates an upgrade scenario from old code.
            val v0Serialized = serializerV0.serialize(baseOpUUIDOnly); // This can only do UUIDs.
            val v0v0Deserialized = serializerV0.deserialize(v0Serialized);
            OperationComparer.DEFAULT.assertEquals(baseOpUUIDOnly, v0v0Deserialized);
            val v0v1Deserialized = serializerV1.deserialize(v0Serialized);
            OperationComparer.DEFAULT.assertEquals(baseOpUUIDOnly, v0v1Deserialized);
            val v0vdDeserialized = defaultSerializer.deserialize(v0Serialized);
            OperationComparer.DEFAULT.assertEquals(baseOpUUIDOnly, v0vdDeserialized);

            // 2. Deserialize the V1 serializer using V1 and Default serializer. This simulates normal operations.
            val v1Serialized = serializerV1.serialize(baseOp);
            val v1v1Deserialized = serializerV1.deserialize(v1Serialized);
            OperationComparer.DEFAULT.assertEquals(baseOp, v1v1Deserialized);
            val v1vdDeserialized = defaultSerializer.deserialize(v1Serialized);
            OperationComparer.DEFAULT.assertEquals(baseOp, v1vdDeserialized);

            AssertExtensions.assertThrows("V0 should not be able to deserialize V1",
                    () -> serializerV0.deserialize(v1Serialized), ex -> ex instanceof SerializationException);

            // 3. Deserialize the Default serializer using ALL serializers. This simulates normal operations and "rollback".
            val dSerialized = defaultSerializer.serialize(baseOp);
            val vdv0Deserialized = serializerV0.deserialize(dSerialized);
            OperationComparer.DEFAULT.assertEquals(baseOpUUIDOnly, vdv0Deserialized); // V0 doesn't know about Variable IDs.
            val vdv1Deserialized = serializerV1.deserialize(dSerialized);
            OperationComparer.DEFAULT.assertEquals(baseOp, vdv1Deserialized);
            val vdvdDeserialized = defaultSerializer.deserialize(dSerialized);
            OperationComparer.DEFAULT.assertEquals(baseOp, vdvdDeserialized);
        }
    }

    @Test
    public void testReferences() {
        val buf = new RefCountByteArraySegment(new byte[1]);
        val op = new StreamSegmentAppendOperation(1L, buf, null);
        Assert.assertEquals("Expected an invocation to BufferView.retain().", 1, buf.refCount);
        op.close();
        Assert.assertEquals("Expected an invocation to BufferView.release().", 0, buf.refCount);
    }

    private static class RefCountByteArraySegment extends ByteArraySegment {
        private int refCount = 0;

        RefCountByteArraySegment(byte[] array) {
            super(array);
        }

        @Override
        public void retain() {
            this.refCount++;
        }

        @Override
        public void release() {
            this.refCount--;
        }
    }

    private static class SerializerV1 extends StreamSegmentAppendOperation.Serializer {
        @Override
        protected byte getWriteVersion() {
            return 1; // Base class still writes with version 0.
        }
    }
}

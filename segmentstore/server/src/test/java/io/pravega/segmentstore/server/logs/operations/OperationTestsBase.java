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
import io.pravega.common.io.ByteBufferOutputStream;
import io.pravega.test.common.AssertExtensions;
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;

/**
 * Base class for all Log Operation test.
 */
@SuppressWarnings("checkstyle:JavadocMethod")
public abstract class OperationTestsBase<T extends Operation> {
    private static final int MAX_CONFIG_ITERATIONS = 10;
    private static final OperationSerializer OPERATION_SERIALIZER = new OperationSerializer();

    /**
     * Tests the ability of an Operation to serialize/deserialize itself.
     */
    @Test
    public void testSerialization() throws Exception {
        Random random = RandomFactory.create();
        T baseOp = createOperation(random);

        // Verify we cannot serialize without a valid Sequence Number.
        trySerialize(baseOp, "Serialization was possible without a valid Sequence Number.");
        baseOp.setSequenceNumber(MathHelpers.abs(random.nextLong()));

        // Verify that whatever Pre-Serialization requirements are needed will actually prevent serialization.
        int iteration = 0;
        while (iteration < MAX_CONFIG_ITERATIONS && isPreSerializationConfigRequired(baseOp)) {
            iteration++;
            trySerialize(baseOp, "Serialization was possible without completing all necessary pre-serialization steps.");
            configurePreSerialization(baseOp, random);
        }

        // Serialize.
        ByteBufferOutputStream outputStream = new ByteBufferOutputStream();
        OPERATION_SERIALIZER.serialize(outputStream, baseOp);

        // Deserialize.
        Operation newOp = OPERATION_SERIALIZER.deserialize(outputStream.getData().getReader());

        // Verify operations are the same.
        OperationComparer.DEFAULT.assertEquals(baseOp, newOp);
    }

    @Test
    public void testOperationType() {
        Random random = RandomFactory.create();
        T op = createOperation(random);
        Assert.assertEquals(getExpectedOperationType(), op.getType());
    }

    @Test
    public void testOperationResetSequenceNumber() {
        Random random = RandomFactory.create();
        T op = createOperation(random);
        op.resetSequenceNumber(1234);
        Assert.assertEquals(1234, op.getSequenceNumber());
        Assert.assertNotNull(op.toString());
    }

    /**
     * Creates a new operation of a given type.
     */
    protected abstract T createOperation(Random random);

    /**
     * Gets a value indicating whether we need to do anything special (i.e., assign offsets) before serializing.
     */
    protected boolean isPreSerializationConfigRequired(T operation) {
        return false;
    }

    /**
     * Performs any necessary pre-serialization configuration (one step at a time - as long as isPreSerializationConfigRequired returns true).
     */
    protected void configurePreSerialization(T operation, Random random) {
        // Base method intentionally left blank.
    }

    protected OperationType getExpectedOperationType() {
        return OperationType.Normal;
    }

    String getStreamSegmentName(long id) {
        return "StreamSegment_" + id;
    }

    private void trySerialize(T op, String message) {
        AssertExtensions.assertThrows(message,
                () -> OPERATION_SERIALIZER.serialize(new ByteBufferOutputStream(), op),
                ex -> ex instanceof IllegalStateException);
    }
}

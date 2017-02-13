/**
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.service.server.logs.operations;

import com.emc.pravega.common.MathHelpers;
import com.emc.pravega.testcommon.AssertExtensions;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Random;

/**
 * Base class for all Log Operation test.
 */
@SuppressWarnings("checkstyle:JavadocMethod")
public abstract class OperationTestsBase<T extends Operation> {
    private static final int MAX_CONFIG_ITERATIONS = 10;
    private static final OperationFactory OPERATION_FACTORY = new OperationFactory();

    /**
     * Tests the ability of an Operation to serialize/deserialize itself.
     */
    @Test
    public void testSerialization() throws Exception {
        Random random = new Random();
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
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        baseOp.serialize(outputStream);

        //Deserialize.
        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        Operation newOp = OPERATION_FACTORY.deserialize(inputStream);

        // Verify operations are the same.
        OperationComparer.DEFAULT.assertEquals(baseOp, newOp);
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

    String getStreamSegmentName(long id) {
        return "StreamSegment_" + id;
    }

    private void trySerialize(T op, String message) {
        AssertExtensions.assertThrows(message,
                () -> op.serialize(new ByteArrayOutputStream()),
                ex -> ex instanceof IllegalStateException);
    }
}

package com.emc.logservice.logs.operations;

import com.emc.logservice.AssertExtensions;
import org.junit.Test;

import java.io.*;
import java.util.Random;
import java.util.concurrent.CompletionException;

/**
 * Base class for all Log Operation test.
 */
public abstract class OperationTestsBase<T extends Operation> {
    protected static final int MaxConfigIterations = 10;

    @Test
    public void testSerialization() throws Exception {
        Random random = new Random();
        T baseOp = createOperation(random);

        // Verify we cannot serialize without a valid Sequence Number.
        trySerialize(baseOp, "Serialization was possible without a valid Sequence Number.");
        baseOp.setSequenceNumber(Math.abs(random.nextLong()));

        // Verify that whatever Pre-Serialization requirements are needed will actually prevent serialization.
        int configIter = 0;
        while (configIter < MaxConfigIterations && isPreSerializationConfigRequired(baseOp)) {
            configIter++;
            trySerialize(baseOp, "Serialization was possible without completing all necessary pre-serialization steps.");
            configurePreSerialization(baseOp, random);
        }

        // Serialize.
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        baseOp.serialize(outputStream);

        //Deserialize.
        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        Operation newOp = Operation.deserialize(inputStream);

        // Verify operations are the same.
        OperationHelpers.assertEquals(baseOp, newOp);
    }

    /**
     * Creates a new operation of a given type.
     *
     * @return
     */
    protected abstract T createOperation(Random random);

    /**
     * Gets a value indicating whether we need to do anything special (i.e., assign offsets) before serializing.
     *
     * @param operation
     * @return
     */
    protected boolean isPreSerializationConfigRequired(T operation) {
        return false;
    }

    /**
     * Performs any necessary pre-serialization configuration (one step at a time - as long as isPreSerializationConfigRequired returns true).
     *
     * @param operation
     * @param random
     */
    protected void configurePreSerialization(T operation, Random random) {
        // Base method intentionally left blank.
    }

    protected String getStreamSegmentName(long id) {
        return "StreamSegment_" + id;
    }

    private void trySerialize(T op, String message) {
        AssertExtensions.assertThrows(message,
                () -> {
                    try {
                        op.serialize(new ByteArrayOutputStream());
                    }
                    catch (IOException ex) {
                        throw new CompletionException(ex);
                    }
                },
                ex -> ex instanceof IllegalStateException);
    }
}

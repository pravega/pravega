package com.emc.logservice.server.logs.operations;

import org.junit.Assert;

import java.util.Random;

/**
 * Unit tests for StreamSegmentSealOperation class.
 */
public class StreamSegmentSealOperationTests extends OperationTestsBase<StreamSegmentSealOperation> {
    @Override
    protected StreamSegmentSealOperation createOperation(Random random) {
        return new StreamSegmentSealOperation(random.nextLong());
    }

    @Override
    protected boolean isPreSerializationConfigRequired(StreamSegmentSealOperation operation) {
        return operation.getStreamSegmentLength() < 0;
    }

    @Override
    protected void configurePreSerialization(StreamSegmentSealOperation operation, Random random) {
        if (operation.getStreamSegmentLength() < 0) {
            operation.setStreamSegmentLength(Math.abs(random.nextLong()));
        }
        else if (isPreSerializationConfigRequired(operation)) {
            Assert.fail("isPreSerializationConfigRequired returned true but there is nothing to be done.");
        }
    }
}

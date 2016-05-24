package com.emc.logservice.logs.operations;

import org.junit.Assert;

import java.util.Random;

/**
 * Unit tests for StreamSegmentAppendOperation class.
 */
public class StreamSegmentAppendOperationTests extends OperationTestsBase<StreamSegmentAppendOperation> {
    private static final int MinAppendData = 1;
    private static final int MaxAppendData = 1024 * 1024;

    @Override
    protected StreamSegmentAppendOperation createOperation(Random random) {
        byte[] data = new byte[random.nextInt(MaxAppendData - MinAppendData) + MinAppendData];
        random.nextBytes(data);
        return new StreamSegmentAppendOperation(random.nextLong(), data);
    }

    @Override
    protected boolean isPreSerializationConfigRequired(StreamSegmentAppendOperation operation) {
        return operation.getStreamSegmentOffset() < 0;
    }

    @Override
    protected void configurePreSerialization(StreamSegmentAppendOperation operation, Random random) {
        if (operation.getStreamSegmentOffset() < 0) {
            operation.setStreamSegmentOffset(Math.abs(random.nextLong()));
        }
        else if (isPreSerializationConfigRequired(operation)) {
            Assert.fail("isPreSerializationConfigRequired returned true but there is nothing to be done.");
        }
    }
}

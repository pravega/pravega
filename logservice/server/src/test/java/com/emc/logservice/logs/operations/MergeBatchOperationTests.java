package com.emc.logservice.logs.operations;

import org.junit.Assert;

import java.util.Random;

/**
 * Unit tests for MergeBatchOperation class.
 */
public class MergeBatchOperationTests extends OperationTestsBase<MergeBatchOperation> {
    @Override
    protected MergeBatchOperation createOperation(Random random) {
        return new MergeBatchOperation(random.nextLong(), random.nextLong());
    }

    @Override
    protected boolean isPreSerializationConfigRequired(MergeBatchOperation operation) {
        return operation.getBatchStreamSegmentLength() < 0
                || operation.getTargetStreamSegmentOffset() < 0;
    }

    @Override
    protected void configurePreSerialization(MergeBatchOperation operation, Random random) {
        if (operation.getBatchStreamSegmentLength() < 0) {
            operation.setBatchStreamSegmentLength(Math.abs(random.nextLong()));
        }
        else if (operation.getTargetStreamSegmentOffset() < 0) {
            operation.setTargetStreamSegmentOffset(Math.abs(random.nextLong()));
        }
        else if (isPreSerializationConfigRequired(operation)) {
            Assert.fail("isPreSerializationConfigRequired returned true but there is nothing to be done.");
        }
    }
}


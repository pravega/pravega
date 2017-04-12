/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server.logs.operations;

import java.util.Random;

import org.junit.Assert;

import com.emc.pravega.shared.MathHelpers;

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
        return operation.getStreamSegmentOffset() < 0;
    }

    @Override
    protected void configurePreSerialization(StreamSegmentSealOperation operation, Random random) {
        if (operation.getStreamSegmentOffset() < 0) {
            operation.setStreamSegmentOffset(MathHelpers.abs(random.nextLong()));
        } else if (isPreSerializationConfigRequired(operation)) {
            Assert.fail("isPreSerializationConfigRequired returned true but there is nothing to be done.");
        }
    }
}

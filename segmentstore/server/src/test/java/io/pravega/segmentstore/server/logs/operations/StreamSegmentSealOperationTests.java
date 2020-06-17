/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.logs.operations;

import java.util.Random;

import org.junit.Assert;

import io.pravega.common.MathHelpers;

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

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

import io.pravega.common.MathHelpers;
import org.junit.Assert;

import java.util.Random;

/**
 * Unit tests for MergeSegmentOperation class.
 */
public class MergeSegmentOperationTests extends OperationTestsBase<MergeSegmentOperation> {
    @Override
    protected MergeSegmentOperation createOperation(Random random) {
        return new MergeSegmentOperation(random.nextLong(), random.nextLong());
    }

    @Override
    protected boolean isPreSerializationConfigRequired(MergeSegmentOperation operation) {
        return operation.getLength() < 0
                || operation.getStreamSegmentOffset() < 0;
    }

    @Override
    protected void configurePreSerialization(MergeSegmentOperation operation, Random random) {
        if (operation.getLength() < 0) {
            operation.setLength(MathHelpers.abs(random.nextLong()));
        } else if (operation.getStreamSegmentOffset() < 0) {
            operation.setStreamSegmentOffset(MathHelpers.abs(random.nextLong()));
        } else if (isPreSerializationConfigRequired(operation)) {
            Assert.fail("isPreSerializationConfigRequired returned true but there is nothing to be done.");
        }
    }
}


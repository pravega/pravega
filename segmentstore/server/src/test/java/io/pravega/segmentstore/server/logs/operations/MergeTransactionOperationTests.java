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
 * Unit tests for MergeTransactionOperation class.
 */
public class MergeTransactionOperationTests extends OperationTestsBase<MergeTransactionOperation> {
    @Override
    protected MergeTransactionOperation createOperation(Random random) {
        return new MergeTransactionOperation(random.nextLong(), random.nextLong());
    }

    @Override
    protected boolean isPreSerializationConfigRequired(MergeTransactionOperation operation) {
        return operation.getLength() < 0
                || operation.getStreamSegmentOffset() < 0;
    }

    @Override
    protected void configurePreSerialization(MergeTransactionOperation operation, Random random) {
        if (operation.getLength() < 0) {
            operation.setLength(MathHelpers.abs(random.nextLong()));
        } else if (operation.getStreamSegmentOffset() < 0) {
            operation.setStreamSegmentOffset(MathHelpers.abs(random.nextLong()));
        } else if (isPreSerializationConfigRequired(operation)) {
            Assert.fail("isPreSerializationConfigRequired returned true but there is nothing to be done.");
        }
    }
}


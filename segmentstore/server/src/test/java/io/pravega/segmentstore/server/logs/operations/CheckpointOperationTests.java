/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.logs.operations;

import io.pravega.common.util.ByteArraySegment;
import java.util.Random;
import org.junit.Assert;

/**
 * Unit tests for all classes derived from the CheckpointOperationBase class.
 */
public abstract class CheckpointOperationTests extends OperationTestsBase<CheckpointOperationBase> {

    public static class MetadataCheckpointOperationTests extends CheckpointOperationTests {
        @Override
        protected CheckpointOperationBase createOperation(Random random) {
            return new MetadataCheckpointOperation();
        }
    }

    public static class StorageMetadataCheckpointOperationTests extends CheckpointOperationTests {
        @Override
        protected CheckpointOperationBase createOperation(Random random) {
            return new StorageMetadataCheckpointOperation();
        }
    }

    @Override
    protected boolean isPreSerializationConfigRequired(CheckpointOperationBase operation) {
        return operation.getContents() == null;
    }

    @Override
    protected void configurePreSerialization(CheckpointOperationBase operation, Random random) {
        if (operation.getContents() == null) {
            byte[] data = new byte[10245];
            random.nextBytes(data);
            operation.setContents(new ByteArraySegment(data));
        } else if (isPreSerializationConfigRequired(operation)) {
            Assert.fail("isPreSerializationConfigRequired returned true but there is nothing to be done.");
        }
    }
}

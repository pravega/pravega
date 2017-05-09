/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.service.server.logs.operations;

import io.pravega.common.util.ByteArraySegment;
import org.junit.Assert;

import java.util.Random;

/**
 * Unit tests for MetadataCheckpointOperation class.
 */
public class MetadataCheckpointOperationTests extends OperationTestsBase<MetadataCheckpointOperation> {
    @Override
    protected MetadataCheckpointOperation createOperation(Random random) {
        return new MetadataCheckpointOperation();
    }

    @Override
    protected boolean isPreSerializationConfigRequired(MetadataCheckpointOperation operation) {
        return operation.getContents() == null;
    }

    @Override
    protected void configurePreSerialization(MetadataCheckpointOperation operation, Random random) {
        if (operation.getContents() == null) {
            byte[] data = new byte[10245];
            random.nextBytes(data);
            operation.setContents(new ByteArraySegment(data));
        } else if (isPreSerializationConfigRequired(operation)) {
            Assert.fail("isPreSerializationConfigRequired returned true but there is nothing to be done.");
        }
    }
}

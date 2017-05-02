/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.server.segmentstore.service.logs.operations;

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

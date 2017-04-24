/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.service.server.logs.operations;

import io.pravega.common.util.ImmutableDate;
import io.pravega.service.contracts.StreamSegmentInformation;
import io.pravega.service.server.ContainerMetadata;
import java.util.Random;

import org.junit.Assert;

/**
 * Unit tests for TransactionMapOperation class.
 */
public class TransactionMapOperationTests extends OperationTestsBase<TransactionMapOperation> {
    @Override
    protected TransactionMapOperation createOperation(Random random) {
        return new TransactionMapOperation(random.nextLong(), new StreamSegmentInformation(
                super.getStreamSegmentName(random.nextLong()),
                random.nextLong(),
                random.nextBoolean(),
                random.nextBoolean(),
                StreamSegmentMapOperationTests.createAttributes(10),
                new ImmutableDate()));
    }

    @Override
    protected boolean isPreSerializationConfigRequired(TransactionMapOperation operation) {
        return operation.getStreamSegmentId() == ContainerMetadata.NO_STREAM_SEGMENT_ID;
    }

    @Override
    protected void configurePreSerialization(TransactionMapOperation operation, Random random) {
        if (operation.getStreamSegmentId() == ContainerMetadata.NO_STREAM_SEGMENT_ID) {
            operation.setStreamSegmentId(random.nextLong());
        } else if (isPreSerializationConfigRequired(operation)) {
            Assert.fail("isPreSerializationConfigRequired returned true but there is nothing to be done.");
        }
    }
}

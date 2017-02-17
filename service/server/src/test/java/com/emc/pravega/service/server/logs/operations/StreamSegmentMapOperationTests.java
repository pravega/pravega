/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server.logs.operations;

import com.emc.pravega.common.util.ImmutableDate;
import com.emc.pravega.service.contracts.StreamSegmentInformation;
import com.emc.pravega.service.server.ContainerMetadata;

import java.util.Random;

import org.junit.Assert;

/**
 * Unit tests for StreamSegmentMapOperation class.
 */
public class StreamSegmentMapOperationTests extends OperationTestsBase<StreamSegmentMapOperation> {
    @Override
    protected StreamSegmentMapOperation createOperation(Random random) {
        return new StreamSegmentMapOperation(new StreamSegmentInformation(super.getStreamSegmentName(random.nextLong()), random.nextLong(), random.nextBoolean(), random.nextBoolean(), new ImmutableDate()));
    }

    @Override
    protected boolean isPreSerializationConfigRequired(StreamSegmentMapOperation operation) {
        return operation.getStreamSegmentId() == ContainerMetadata.NO_STREAM_SEGMENT_ID;
    }

    @Override
    protected void configurePreSerialization(StreamSegmentMapOperation operation, Random random) {
        if (operation.getStreamSegmentId() == ContainerMetadata.NO_STREAM_SEGMENT_ID) {
            operation.setStreamSegmentId(random.nextLong());
        } else if (isPreSerializationConfigRequired(operation)) {
            Assert.fail("isPreSerializationConfigRequired returned true but there is nothing to be done.");
        }
    }
}

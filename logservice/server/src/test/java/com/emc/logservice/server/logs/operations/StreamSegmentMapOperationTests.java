package com.emc.logservice.server.logs.operations;

import com.emc.logservice.server.StreamSegmentInformation;

import java.util.Date;
import java.util.Random;

/**
 * Unit tests for StreamSegmentMapOperation class.
 */
public class StreamSegmentMapOperationTests extends com.emc.logservice.server.logs.operations.OperationTestsBase<StreamSegmentMapOperation> {
    @Override
    protected StreamSegmentMapOperation createOperation(Random random) {
        long id = random.nextLong();
        return new StreamSegmentMapOperation(id, new StreamSegmentInformation(super.getStreamSegmentName(id), random.nextLong(), random.nextBoolean(), random.nextBoolean(), new Date()));
    }
}

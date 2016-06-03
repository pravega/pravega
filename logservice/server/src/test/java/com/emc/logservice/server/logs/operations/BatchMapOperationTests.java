package com.emc.logservice.server.logs.operations;

import com.emc.logservice.server.StreamSegmentInformation;

import java.util.Date;
import java.util.Random;

/**
 * Unit tests for BatchMapOperation class.
 */
public class BatchMapOperationTests extends OperationTestsBase<BatchMapOperation> {
    @Override
    protected BatchMapOperation createOperation(Random random) {
        long id = random.nextLong();
        return new BatchMapOperation(random.nextLong(), id, new StreamSegmentInformation(super.getStreamSegmentName(id), random.nextLong(), random.nextBoolean(), random.nextBoolean(), new Date()));
    }
}

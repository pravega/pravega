package com.emc.logservice.logs.operations;

import java.util.Random;

/**
 * Unit tests for BatchMapOperation class.
 */
public class BatchMapOperationTests extends OperationTestsBase<BatchMapOperation> {
    @Override
    protected BatchMapOperation createOperation(Random random) {
        long id = random.nextLong();
        return new BatchMapOperation(random.nextLong(), id, super.getStreamSegmentName(id));
    }
}

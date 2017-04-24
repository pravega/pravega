/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package io.pravega.service.server.logs.operations;

import java.util.Random;
import lombok.val;

/**
 * Unit tests for the UpdateAttributesOperation class.
 */
public class UpdateAttributesOperationTests extends OperationTestsBase<UpdateAttributesOperation> {
    @Override
    protected UpdateAttributesOperation createOperation(Random random) {
        val attributes = StreamSegmentAppendOperationTests.createAttributes();
        return new UpdateAttributesOperation(random.nextLong(), attributes);
    }
}

/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.logs.operations;

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

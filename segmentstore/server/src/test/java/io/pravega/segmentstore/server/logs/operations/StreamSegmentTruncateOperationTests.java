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

import io.pravega.common.MathHelpers;
import java.util.Random;

/**
 * Unit tests for the StreamSegmentTruncateOperation class.
 */
public class StreamSegmentTruncateOperationTests extends OperationTestsBase<StreamSegmentTruncateOperation> {
    @Override
    protected StreamSegmentTruncateOperation createOperation(Random random) {
        return new StreamSegmentTruncateOperation(random.nextLong(), MathHelpers.abs(random.nextLong()));
    }
}

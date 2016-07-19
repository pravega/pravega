/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.logservice.server.logs.operations;

import com.emc.logservice.server.SegmentMetadataCollection;
import com.emc.logservice.server.StreamSegmentInformation;
import org.junit.Assert;

import java.util.Date;
import java.util.Random;

/**
 * Unit tests for BatchMapOperation class.
 */
public class BatchMapOperationTests extends OperationTestsBase<BatchMapOperation> {
    @Override
    protected BatchMapOperation createOperation(Random random) {
        return new BatchMapOperation(random.nextLong(), new StreamSegmentInformation(super.getStreamSegmentName(random.nextLong()), random.nextLong(), random.nextBoolean(), random.nextBoolean(), new Date()));
    }

    @Override
    protected boolean isPreSerializationConfigRequired(BatchMapOperation operation) {
        return operation.getStreamSegmentId() == SegmentMetadataCollection.NO_STREAM_SEGMENT_ID;
    }

    @Override
    protected void configurePreSerialization(BatchMapOperation operation, Random random) {
        if (operation.getStreamSegmentId() == SegmentMetadataCollection.NO_STREAM_SEGMENT_ID) {
            operation.setStreamSegmentId(random.nextLong());
        } else if (isPreSerializationConfigRequired(operation)) {
            Assert.fail("isPreSerializationConfigRequired returned true but there is nothing to be done.");
        }
    }
}

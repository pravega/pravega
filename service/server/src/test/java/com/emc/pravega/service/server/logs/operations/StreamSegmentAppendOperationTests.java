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

package com.emc.pravega.service.server.logs.operations;

import com.emc.pravega.common.MathHelpers;
import com.emc.pravega.service.contracts.AppendContext;
import org.junit.Assert;

import java.util.Random;
import java.util.UUID;

/**
 * Unit tests for StreamSegmentAppendOperation class.
 */
public class StreamSegmentAppendOperationTests extends OperationTestsBase<StreamSegmentAppendOperation> {
    private static final int MIN_LENGTH = 1;
    private static final int MAX_LENGTH = 1024 * 1024;

    @Override
    protected StreamSegmentAppendOperation createOperation(Random random) {
        byte[] data = new byte[random.nextInt(MAX_LENGTH - MIN_LENGTH) + MIN_LENGTH];
        random.nextBytes(data);
        AppendContext context = new AppendContext(new UUID(random.nextLong(), random.nextLong()), random.nextLong());
        return new StreamSegmentAppendOperation(random.nextLong(), data, context);
    }

    @Override
    protected boolean isPreSerializationConfigRequired(StreamSegmentAppendOperation operation) {
        return operation.getStreamSegmentOffset() < 0;
    }

    @Override
    protected void configurePreSerialization(StreamSegmentAppendOperation operation, Random random) {
        if (operation.getStreamSegmentOffset() < 0) {
            operation.setStreamSegmentOffset(MathHelpers.abs(random.nextLong()));
        } else if (isPreSerializationConfigRequired(operation)) {
            Assert.fail("isPreSerializationConfigRequired returned true but there is nothing to be done.");
        }
    }
}

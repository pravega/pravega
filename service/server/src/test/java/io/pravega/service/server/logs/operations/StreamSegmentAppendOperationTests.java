/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pravega.service.server.logs.operations;

import io.pravega.common.MathHelpers;
import io.pravega.service.contracts.AttributeUpdate;
import io.pravega.service.contracts.AttributeUpdateType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;
import java.util.UUID;

import lombok.val;
import org.junit.Assert;

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
        val attributes = createAttributes();
        return new StreamSegmentAppendOperation(random.nextLong(), data, attributes);
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

    static Collection<AttributeUpdate> createAttributes() {
        val result = new ArrayList<AttributeUpdate>();
        long currentValue = 0;
        for (AttributeUpdateType ut : AttributeUpdateType.values()) {
            result.add(new AttributeUpdate(UUID.randomUUID(), ut, ++currentValue));
        }

        return result;
    }
}

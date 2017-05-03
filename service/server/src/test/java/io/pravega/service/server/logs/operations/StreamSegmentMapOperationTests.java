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

import io.pravega.common.util.ImmutableDate;
import io.pravega.service.contracts.StreamSegmentInformation;
import io.pravega.service.server.ContainerMetadata;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import lombok.val;
import org.junit.Assert;

/**
 * Unit tests for StreamSegmentMapOperation class.
 */
public class StreamSegmentMapOperationTests extends OperationTestsBase<StreamSegmentMapOperation> {
    @Override
    protected StreamSegmentMapOperation createOperation(Random random) {
        return new StreamSegmentMapOperation(new StreamSegmentInformation(
                super.getStreamSegmentName(random.nextLong()),
                random.nextLong(),
                random.nextBoolean(),
                random.nextBoolean(),
                createAttributes(10),
                new ImmutableDate()));
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

    static Map<UUID, Long> createAttributes(int count) {
        val result = new HashMap<UUID, Long>();
        for (int i = 0; i < count; i++) {
            result.put(UUID.randomUUID(), (long) i);
        }

        return result;
    }
}

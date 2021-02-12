/**
 * Copyright Pravega Authors.
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
package io.pravega.segmentstore.server.logs.operations;

import io.pravega.common.MathHelpers;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.server.ContainerMetadata;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import lombok.val;
import org.junit.Assert;

/**
 * Unit tests for StreamSegmentMapOperation class.
 */
public class StreamSegmentMapOperationTests extends OperationTestsBase<StreamSegmentMapOperation> {
    @Override
    protected StreamSegmentMapOperation createOperation(Random random) {
        long length = MathHelpers.abs(random.nextLong());
        val op = new StreamSegmentMapOperation(StreamSegmentInformation
                .builder()
                .name(super.getStreamSegmentName(random.nextLong()))
                .startOffset(length / 2)
                .length(length)
                .sealed(random.nextBoolean())
                .deleted(random.nextBoolean())
                .attributes(createAttributes(10))
                .build());
        op.markPinned();
        return op;
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

    static Map<AttributeId, Long> createAttributes(int count) {
        val result = new HashMap<AttributeId, Long>();
        for (int i = 0; i < count; i++) {
            result.put(AttributeId.randomUUID(), (long) i);
        }

        return result;
    }
}

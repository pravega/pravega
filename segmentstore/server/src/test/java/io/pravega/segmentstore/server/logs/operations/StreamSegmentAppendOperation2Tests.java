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
import io.pravega.common.util.BitConverter;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;
import lombok.val;
import org.junit.Assert;

/**
 * Unit test for the {@link StreamSegmentAppendOperation2} class.
 */
public class StreamSegmentAppendOperation2Tests extends OperationTestsBase<StreamSegmentAppendOperation2> {
    private static final int MIN_LENGTH = 1;
    private static final int MAX_LENGTH = 1024 * 1024;
    private static final int[] ATTRIBUTE_ID_LENGTHS = new int[]{8, 16, 32, 64, 128, 256};

    @Override
    protected StreamSegmentAppendOperation2 createOperation(Random random) {
        byte[] data = new byte[random.nextInt(MAX_LENGTH - MIN_LENGTH) + MIN_LENGTH];
        random.nextBytes(data);
        val attributes = createAttributes();
        return new StreamSegmentAppendOperation2(random.nextLong(), new ByteArraySegment(data), attributes);
    }

    @Override
    protected boolean isPreSerializationConfigRequired(StreamSegmentAppendOperation2 operation) {
        return operation.getStreamSegmentOffset() < 0;
    }

    @Override
    protected void configurePreSerialization(StreamSegmentAppendOperation2 operation, Random random) {
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
            for (int length : ATTRIBUTE_ID_LENGTHS) {
                byte[] data = new byte[length];
                int offset = 0;
                while (offset < data.length) {
                    offset += BitConverter.writeLong(data, offset, currentValue + offset);
                }
                result.add(new AttributeUpdate(AttributeId.from(data), ut, ++currentValue, currentValue));
            }
        }

        return result;
    }
}

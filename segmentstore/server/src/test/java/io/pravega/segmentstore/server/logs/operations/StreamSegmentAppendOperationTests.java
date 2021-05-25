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
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Random;
import java.util.UUID;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

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
        return new StreamSegmentAppendOperation(random.nextLong(), new ByteArraySegment(data), attributes);
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
            result.add(new AttributeUpdate(UUID.randomUUID(), ut, ++currentValue, currentValue));
        }

        return result;
    }

    @Test
    public void testReferences() {
        val buf = new RefCountByteArraySegment(new byte[1]);
        val op = new StreamSegmentAppendOperation(1L, buf, Collections.emptyList());
        Assert.assertEquals("Expected an invocation to BufferView.retain().", 1, buf.refCount);
        op.close();
        Assert.assertEquals("Expected an invocation to BufferView.release().", 0, buf.refCount);
    }

    private static class RefCountByteArraySegment extends ByteArraySegment {
        private int refCount = 0;

        RefCountByteArraySegment(byte[] array) {
            super(array);
        }

        @Override
        public void retain() {
            this.refCount++;
        }

        @Override
        public void release() {
            this.refCount--;
        }
    }
}

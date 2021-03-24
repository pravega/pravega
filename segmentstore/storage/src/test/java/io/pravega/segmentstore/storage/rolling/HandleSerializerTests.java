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
package io.pravega.segmentstore.storage.rolling;

import io.pravega.common.MathHelpers;
import io.pravega.common.io.StreamHelpers;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.AssertExtensions;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the HandleSerializer class.
 */
public class HandleSerializerTests {
    private static final String SEGMENT_NAME = "Segment";
    private static final SegmentRollingPolicy TEST_ROLLING_POLICY = new SegmentRollingPolicy(1234);

    /**
     * Tests the basic Serialization-Deserialization for a Handle with no concat executed on it.
     */
    @Test
    public void testNormalSerialization() {
        final int chunkCount = 1000;
        val source = newHandle(chunkCount);
        val serialization = serialize(source);
        val newHandle = HandleSerializer.deserialize(serialization, source.getHeaderHandle());
        assertHandleEquals(source, newHandle, source.getHeaderHandle());
        Assert.assertEquals("getHeaderLength", serialization.length, newHandle.getHeaderLength());
    }

    /**
     * Tests the basic Serialization-Deserialization for a Handle with various successful/unsuccessful concat operations
     * executed on it.
     */
    @Test
    public void testConcat() throws IOException {
        final int chunkCount = 10;
        final int concatCount = 15;
        final int failConcatEvery = 2;
        final int addChunkAfterEveryConcat = 3;
        val source = newHandle(chunkCount);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        os.write(serialize(source));
        for (int i = 0; i < concatCount; i++) {
            os.write(HandleSerializer.serializeConcat(chunkCount, source.length()));
            if (i % failConcatEvery != 0) {

                // Create a handle for the concat segment, and add its info both to the base handle and serialize it.
                val concatHandle = newHandle("concat" + i, chunkCount);
                os.write(serialize(concatHandle));
                source.addChunks(concatHandle.chunks().stream()
                        .map(s -> s.withNewOffset(s.getStartOffset() + source.length())).collect(Collectors.toList()));
            }

            // Every now and then, add a new SegmentChunk to the source, to verify how deserializing Concats (whether successful
            // or not) works with this.
            if (i % addChunkAfterEveryConcat == 0) {
                val chunk = new SegmentChunk(NameUtils.getSegmentChunkName(source.getSegmentName(), source.length()), source.length());
                chunk.setLength(i + 1);
                source.addChunks(Collections.singletonList(chunk));
                os.write(HandleSerializer.serializeChunk(chunk));
            }
        }

        val serialization = os.toByteArray();
        val newHandle = HandleSerializer.deserialize(serialization, source.getHeaderHandle());
        assertHandleEquals(source, newHandle, source.getHeaderHandle());
        Assert.assertEquals("getHeaderLength", serialization.length, newHandle.getHeaderLength());
    }

    private void assertHandleEquals(RollingSegmentHandle expected, RollingSegmentHandle actual, SegmentHandle headerHandle) {
        Assert.assertEquals("getSegmentName", expected.getSegmentName(), actual.getSegmentName());
        AssertExtensions.assertListEquals("chunks", expected.chunks(), actual.chunks(), this::chunkEquals);
        Assert.assertEquals("getRollingPolicy", expected.getRollingPolicy().getMaxLength(), actual.getRollingPolicy().getMaxLength());
        Assert.assertEquals("getHeaderHandle", headerHandle, actual.getHeaderHandle());
        Assert.assertEquals("isReadOnly", headerHandle.isReadOnly(), expected.isReadOnly());
    }

    private boolean chunkEquals(SegmentChunk s1, SegmentChunk s2) {
        return s1.getName().equals(s2.getName())
                && s1.getStartOffset() == s2.getStartOffset();
    }

    @SneakyThrows(IOException.class)
    private byte[] serialize(RollingSegmentHandle handle) {
        val s = HandleSerializer.serialize(handle);
        return StreamHelpers.readAll(s.getReader(), s.getLength());
    }

    private RollingSegmentHandle newHandle(int chunkCount) {
        return newHandle(SEGMENT_NAME, chunkCount);
    }

    private RollingSegmentHandle newHandle(String segmentName, int chunkCount) {
        val chunks = new ArrayList<SegmentChunk>();
        long offset = 0;
        val rnd = new Random(0);
        for (int i = 0; i < chunkCount; i++) {
            val chunk = new SegmentChunk(NameUtils.getSegmentChunkName(segmentName, offset), offset);
            chunk.setLength(MathHelpers.abs(rnd.nextInt()));
            if (i < chunkCount - 1) {
                chunk.markSealed();
            }
            chunks.add(chunk);
            offset += chunk.getLength();
        }

        return new RollingSegmentHandle(new TestHandle(NameUtils.getHeaderSegmentName(segmentName), false),
                TEST_ROLLING_POLICY, chunks);
    }

    @Data
    private static class TestHandle implements SegmentHandle {
        private final String segmentName;
        private final boolean readOnly;
    }
}

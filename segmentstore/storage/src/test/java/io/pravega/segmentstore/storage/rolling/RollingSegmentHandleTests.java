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

import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.AssertExtensions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Data;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the RollingSegmentHandle class.
 */
public class RollingSegmentHandleTests {
    private static final SegmentRollingPolicy DEFAULT_ROLLING_POLICY = new SegmentRollingPolicy(1234);
    private static final String SEGMENT_NAME = "Segment";
    private static final String HEADER_NAME = NameUtils.getHeaderSegmentName(SEGMENT_NAME);

    /**
     * Tests various features of the RollingSegmentHandle.
     */
    @Test
    public void testMainFeatures() {
        val headerHandle = new TestHandle(HEADER_NAME, true);
        val h = new RollingSegmentHandle(headerHandle, DEFAULT_ROLLING_POLICY, new ArrayList<>());
        Assert.assertNull("Unexpected lastChunk for empty handle.", h.lastChunk());
        Assert.assertEquals("Unexpected contents in chunks() for empty handle.", 0, h.chunks().size());
        Assert.assertEquals("Unexpected value for length() for empty handle.", 0, h.length());
        Assert.assertEquals("Unexpected value for getHeaderLength() for empty handle.", 0, h.getHeaderLength());
        Assert.assertEquals("Unexpected value for getHeaderHandle().", headerHandle, h.getHeaderHandle());
        Assert.assertEquals("Unexpected segment name.", SEGMENT_NAME, h.getSegmentName());
        Assert.assertEquals("Unexpected rolling policy.", DEFAULT_ROLLING_POLICY, h.getRollingPolicy());
        Assert.assertTrue("Unexpected value for isReadOnly.", h.isReadOnly());
        Assert.assertFalse("Unexpected value for isSealed.", h.isSealed());
        Assert.assertFalse("Unexpected value for isDeleted.", h.isDeleted());

        // Active handles.
        AssertExtensions.assertThrows(
                "setActiveChunkHandle accepted a handle when no SegmentChunks are registered.",
                () -> h.setActiveChunkHandle(new TestHandle("foo", false)),
                ex -> ex instanceof IllegalStateException);

        val chunkName = "Chunk";
        h.addChunk(new SegmentChunk(chunkName, 0L), new TestHandle(chunkName, false));
        AssertExtensions.assertThrows(
                "setActiveChunkHandle accepted a handle that does not match the last SegmentChunk's name.",
                () -> h.setActiveChunkHandle(new TestHandle("foo", false)),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                "setActiveChunkHandle accepted a read-only handle.",
                () -> h.setActiveChunkHandle(new TestHandle(chunkName, true)),
                ex -> ex instanceof IllegalArgumentException);

        val activeHandle = new TestHandle(chunkName, false);
        h.setActiveChunkHandle(activeHandle);
        Assert.assertEquals("Unexpected value from getActiveChunkHandle.", activeHandle, h.getActiveChunkHandle());

        // Header length.
        h.setHeaderLength(10);
        h.increaseHeaderLength(5);
        Assert.assertEquals("Unexpected value for getHeaderLength when set and then increased.", 15, h.getHeaderLength());

        // Sealed.
        h.markSealed();
        Assert.assertTrue("Unexpected value for isSealed.", h.isSealed());

        // Deleted
        h.markDeleted();
        Assert.assertTrue("Unexpected value for isDeleted.", h.isDeleted());
    }

    /**
     * Tests the ability to add a single SegmentChunk.
     */
    @Test
    public void testAddChunk() {
        val headerHandle = new TestHandle(HEADER_NAME, true);
        val h = new RollingSegmentHandle(headerHandle, DEFAULT_ROLLING_POLICY, new ArrayList<>());

        AssertExtensions.assertThrows(
                "addChunk allowed adding a null ActiveSegmentHandle.",
                () -> h.addChunk(new SegmentChunk("s", 0L), null),
                ex -> ex instanceof NullPointerException);
        AssertExtensions.assertThrows(
                "addChunk allowed adding a read-only ActiveSegmentHandle.",
                () -> h.addChunk(new SegmentChunk("s", 0L), new TestHandle("s", true)),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows(
                "addChunk allowed adding an ActiveSegmentHandle with different name..",
                () -> h.addChunk(new SegmentChunk("s", 0L), new TestHandle("s2", false)),
                ex -> ex instanceof IllegalArgumentException);
        Assert.assertEquals("Not expecting any SegmentChunks to be added.", 0, h.chunks().size());
        Assert.assertNull("Not expecting the Active SegmentChunk handle to be set.", h.getActiveChunkHandle());

        val chunk = new SegmentChunk("s1", 100L);
        h.addChunk(chunk, new TestHandle("s1", false));
        chunk.setLength(123L);
        Assert.assertEquals("Unexpected value for length() after adding one SegmentChunk.",
                chunk.getStartOffset() + chunk.getLength(), h.length());
        AssertExtensions.assertListEquals("Unexpected contents for chunks().",
                Collections.singletonList(chunk), h.chunks(), Object::equals);
        Assert.assertEquals("Unexpected lastChunk.", chunk, h.lastChunk());

        AssertExtensions.assertThrows("addChunk allowed adding a SegmentChunk that is not contiguous.",
                () -> h.addChunk(new SegmentChunk("s2", chunk.getLastOffset() + 1), new TestHandle("s2", false)),
                ex -> ex instanceof IllegalArgumentException);

        chunk.markInexistent();
        val chunk2 = new SegmentChunk("s2", chunk.getLastOffset());
        chunk2.setLength(234L);
        h.addChunk(chunk2, new TestHandle("s2", false));
        Assert.assertEquals("Unexpected number of registered SegmentChunks.", 2, h.chunks().size());
        Assert.assertEquals("Unexpected value for length() after adding two SegmentChunk.",
                chunk2.getStartOffset() + chunk2.getLength(), h.length());
        Assert.assertEquals("Unexpected lastChunk.", chunk2, h.lastChunk());
        h.lastChunk().markInexistent();
        Assert.assertFalse("Unexpected value from isDeleted after last SegmentChunk marked as inexistent.", h.isDeleted());
    }


    /**
     * Tests the ability to add multiple SegmentChunks at once, atomically.
     */
    @Test
    public void testAddChunks() {
        val headerHandle = new TestHandle(HEADER_NAME, true);
        val h = new RollingSegmentHandle(headerHandle, DEFAULT_ROLLING_POLICY, new ArrayList<>());

        val firstBadList = Arrays.asList(
                new SegmentChunk("s1", 0L),
                new SegmentChunk("s2", 10L));
        firstBadList.get(0).setLength(9);
        AssertExtensions.assertThrows(
                "addChunks allowed an incontiguous list of SegmentChunks to be added.",
                () -> h.addChunks(firstBadList),
                ex -> ex instanceof IllegalArgumentException);
        Assert.assertEquals("Not expecting any SegmentChunks to be added.", 0, h.chunks().size());
        Assert.assertEquals("Unexpected length().", 0, h.length());

        val validList = Arrays.asList(
                new SegmentChunk("s1", 0L),
                new SegmentChunk("s2", 10L));
        validList.get(0).setLength(10);
        validList.get(1).setLength(5);
        h.addChunks(validList);
        AssertExtensions.assertListEquals("Unexpected list of SegmentChunks.", validList, h.chunks(), Object::equals);
        Assert.assertEquals("Unexpected length.", 15, h.length());

        val secondBadList = Arrays.asList(
                new SegmentChunk("s3", h.length() - 1),
                new SegmentChunk("s4", h.length() + 1));
        secondBadList.get(0).setLength(2);
        AssertExtensions.assertThrows(
                "addChunks allowed an incontiguous list of SegmentChunks to be added.",
                () -> h.addChunks(secondBadList),
                ex -> ex instanceof IllegalArgumentException);
    }

    @Test
    public void testExcludeInexistentChunks() {
        val initialChunkCount = 10;
        val headerHandle = new TestHandle(HEADER_NAME, true);
        val h = new RollingSegmentHandle(headerHandle, DEFAULT_ROLLING_POLICY, new ArrayList<>());

        val chunkList = IntStream.range(0, initialChunkCount)
                .mapToObj(i -> new SegmentChunk(String.format("s%s", i), i * 10L))
                .collect(Collectors.toList());
        chunkList.forEach(c -> c.setLength(10L));
        h.addChunks(chunkList);

        // We mark the first 4 chunks as inexistent ...
        val truncateCount = 4;
        for (int i = 0; i < truncateCount; i++) {
            chunkList.get(i).markInexistent();
        }

        // And the 6th one.
        chunkList.get(truncateCount + 1).markInexistent();

        // We trim away the chunks.
        h.excludeInexistentChunks();

        // ... and expect that only the Chunks up to truncateIndex are gone.
        Assert.assertEquals(initialChunkCount - truncateCount, h.chunks().size());
        for (int i = truncateCount; i < initialChunkCount; i++) {
            Assert.assertEquals(chunkList.get(i), h.chunks().get(i - truncateCount));
        }
        val serialization = HandleSerializer.serialize(h);
        h.setHeaderLength(serialization.getLength());

        // Test serialization/deserialization.
        val h2 = HandleSerializer.deserialize(serialization.getCopy(), h.getHeaderHandle());
        Assert.assertEquals(h.getHeaderLength(), h2.getHeaderLength());
        AssertExtensions.assertListEquals("", h.chunks(), h2.chunks(),
                (c1, c2) -> c1.getName().equals(c2.getName()) && c1.getStartOffset() == c2.getStartOffset());
    }

    /**
     * Tests the ability of the Handle to refresh based on information from another similar handle.
     */
    @Test
    public void testRefresh() {
        val headerHandle = new TestHandle(HEADER_NAME, true);
        val target = new RollingSegmentHandle(headerHandle, DEFAULT_ROLLING_POLICY,
                Collections.singletonList(new SegmentChunk("s1", 0L)));

        val source = new RollingSegmentHandle(headerHandle, DEFAULT_ROLLING_POLICY, Arrays.asList(
                new SegmentChunk("s1", 0L),
                new SegmentChunk("s2", 100L)));
        source.chunks().get(0).setLength(100);
        source.markSealed();
        source.setHeaderLength(1000);
        source.setActiveChunkHandle(new TestHandle(source.lastChunk().getName(), false));

        target.refresh(source);
        Assert.assertEquals("Unexpected getHeaderLength()", source.getHeaderLength(), target.getHeaderLength());
        AssertExtensions.assertListEquals("Unexpected chunks()", source.chunks(), target.chunks(), Object::equals);
        Assert.assertTrue("Unexpected isSealed.", target.isSealed());
        Assert.assertNull("Not expecting any ActiveSegmentHandle to be copied.", target.getActiveChunkHandle());
    }

    @Data
    private static class TestHandle implements SegmentHandle {
        private final String segmentName;
        private final boolean readOnly;
    }
}

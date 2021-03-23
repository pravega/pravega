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

package io.pravega.segmentstore.storage.chunklayer;

import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalNotification;
import io.pravega.common.concurrent.Futures;
import io.pravega.test.common.AssertExtensions;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;

/**
 * Tests for {@link ReadIndexCache}.
 */
public class ReadIndexCacheTests {
    @Test
    public void testSimpleChunkEviction() {
        String segmentName = "testSegment";
        ReadIndexCache cache = new ReadIndexCache(10, 10);

        // Add some entries.
        for (int i = 0; i < 10; i++) {
            cache.addIndexEntry(segmentName, "chunk" + i, i);
        }

        // Add check floor function works.
        for (int i = 0; i < 10; i++) {
            val floor = cache.findFloor(segmentName, i);
            Assert.assertEquals(i, floor.getOffset());
            Assert.assertEquals("chunk" + i, floor.getChunkName());
        }

        // Add more entries than cache capacity
        ArrayList<ChunkNameOffsetPair> entries = new ArrayList<>();
        for (int i = 10; i < 20; i++) {
            entries.add(new ChunkNameOffsetPair(i, "chunk" + i));
        }
        cache.addIndexEntries(segmentName, entries);

        // Check that cache is evicted as required.
        cache.cleanUp();

        // Check that chunks are really evicted.
        for (int i = 0; i < 10; i++) {
            val floor = cache.findFloor(segmentName, i);
            Assert.assertNull(floor);
        }

        // make sure new entries are still there.
        for (int i = 10; i < 20; i++) {
            val floor = cache.findFloor(segmentName, i);
            Assert.assertEquals(i, floor.getOffset());
            Assert.assertEquals("chunk" + i, floor.getChunkName());
        }

        // Truncate
        cache.truncateReadIndex(segmentName, 15);
        cache.cleanUp();
        // Check that chunks are really evicted.
        for (int i = 10; i < 15; i++) {
            val floor = cache.findFloor(segmentName, i);
            Assert.assertNull(floor);
        }

        // make sure new entries are still there.
        for (int i = 15; i < 20; i++) {
            val floor = cache.findFloor(segmentName, i);
            Assert.assertEquals(i, floor.getOffset());
            Assert.assertEquals("chunk" + i, floor.getChunkName());
        }
    }

    @Test
    public void testSimpleSegmentEviction() {

        ReadIndexCache cache = new ReadIndexCache(10, 10);

        // Add some entries.
        for (int i = 0; i < 10; i++) {
            cache.addIndexEntry("testSegment" + i, "chunk0", 123);
        }

        // Add check floor function works.
        for (int i = 0; i < 10; i++) {
            val floor = cache.findFloor("testSegment" + i, 123);
            Assert.assertEquals(123, floor.getOffset());
            Assert.assertEquals("chunk0", floor.getChunkName());
        }
    }

    @Test
    public void testSimpleChunkEvictionForMultipleSegments() {
        ReadIndexCache cache = new ReadIndexCache(10, 30);

        // Add some entries.
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 5; j++) {
                cache.addIndexEntry("testSegment" + i, "chunk" + j, j);
            }
        }
        cache.cleanUp();
        // Check that floor function works.
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 5; j++) {
                val floor = cache.findFloor("testSegment" + i, j);
                Assert.assertEquals(j, floor.getOffset());
                Assert.assertEquals("chunk" + j, floor.getChunkName());
            }
        }
        cache.cleanUp();

        // Add some more data so that earlier entries are evicted.
        for (int i = 0; i < 5; i++) {
            for (int j = 5; j < 20; j++) {
                cache.addIndexEntry("testSegment" + i, "chunk" + j, j);
            }
        }
        cache.cleanUp();

        // Verify that old entries are evicted.
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 5; j++) {
                val floor = cache.findFloor("testSegment" + i, j);
                Assert.assertNull(floor);
            }
        }
        Assert.assertEquals(5, cache.getSegmentsReadIndexCache().asMap().size());
        Assert.assertEquals(30, cache.getIndexEntryCache().asMap().size());
    }

    @Test
    public void testSimpleAddRemove() {
        String segmentName = "testSegment";
        ReadIndexCache cache = new ReadIndexCache(10, 10);
        // Add some entries.
        cache.addIndexEntry(segmentName, "chunk0", 0);

        // Add check floor function works.
        val floor = cache.findFloor(segmentName, 0);
        Assert.assertEquals(0, floor.getOffset());
        Assert.assertEquals("chunk0", floor.getChunkName());

        // Remove
        cache.remove(segmentName);

        // Add more entries than cache capacity
        ArrayList<ChunkNameOffsetPair> entries = new ArrayList<>();
        entries.add(new ChunkNameOffsetPair(0, "chunk0"));
        cache.addIndexEntries(segmentName, entries);
        Assert.assertEquals(0, floor.getOffset());
        Assert.assertEquals("chunk0", floor.getChunkName());
        Assert.assertEquals(1, cache.getSegmentsReadIndexCache().asMap().size());
        Assert.assertEquals(1, cache.getIndexEntryCache().asMap().size());
    }

    @Test
    public void testRemoveNonExistent() {
        String segmentName = "testSegment";
        ReadIndexCache cache = new ReadIndexCache(10, 10);

        // Remove non-existent entities
        cache.remove(segmentName);

        cache.removeSegment( RemovalNotification.create(segmentName, new ReadIndexCache.SegmentReadIndex(), RemovalCause.EXPIRED));
        cache.removeChunk(RemovalNotification.create(
                ReadIndexCache.IndexEntry.builder()
                    .chunkName("foo")
                    .streamSegmentName(segmentName)
                    .startOffset(111)
                    .build(),
                true,
                RemovalCause.EXPIRED));
    }

    @Test
    public void testMultiGenerationAddRemove() {
        ReadIndexCache cache = new ReadIndexCache(5, 5);

        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10; j++) {
                cache.addIndexEntry("testSegment" + i, "chunk" + j, j);
            }
        }
        cache.cleanUp();
        for (int j = 5; j < 10; j++) {
            val index = cache.getSegmentsReadIndexCache().getIfPresent("testSegment" + j);
            Assert.assertNotNull(index);
        }
        Assert.assertEquals(5, cache.getSegmentsReadIndexCache().asMap().size());
        Assert.assertEquals(5, cache.getIndexEntryCache().asMap().size());
    }

    @Test
    public void testParallelMultiGenerationAddRemove() {
        ReadIndexCache cache = new ReadIndexCache(5, 5);
        ArrayList<CompletableFuture<Void>> futures = new ArrayList<CompletableFuture<Void>>();
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 100; j++) {
                val m = i;
                val n = j;
                futures.add( CompletableFuture.runAsync(() -> cache.addIndexEntry("testSegment" + m, "chunk" + n, n)));
            }
        }
        Futures.allOf(futures).join();
        cache.cleanUp();
        Assert.assertEquals(5, cache.getSegmentsReadIndexCache().asMap().size());
        Assert.assertEquals(5, cache.getIndexEntryCache().asMap().size());
    }

    @Test
    public void testTruncateInvalidParameters() throws Exception {
        String segmentName = "testSegment";

        AssertExtensions.assertThrows("ReadIndexCache() allowed for invalid parameters",
                () -> new ReadIndexCache(-1, 10),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows("ReadIndexCache() allowed for invalid parameters",
                () -> new ReadIndexCache(10, -1),
                ex -> ex instanceof IllegalArgumentException);

        ReadIndexCache cache = new ReadIndexCache(10, 10);
        // Invalid parameters
        AssertExtensions.assertThrows("addIndexEntry() allowed for invalid parameters",
                () -> cache.addIndexEntry(null, "chunk", 0),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows("addIndexEntry() allowed for invalid parameters",
                () -> cache.addIndexEntry("segment", null, 0),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows("addIndexEntry() allowed for invalid parameters",
                () -> cache.addIndexEntry("segment", "chunk", -1),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows("addIndexEntries() allowed for invalid parameters",
                () -> cache.addIndexEntries(null, new ArrayList<>()),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows("addIndexEntries() allowed for invalid parameters",
                () -> cache.addIndexEntries("segment", null),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows("truncateReadIndex() allowed for invalid parameters",
                () -> cache.truncateReadIndex(null, 0),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows("truncateReadIndex() allowed for invalid parameters",
                () -> cache.truncateReadIndex("segment", -1),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows("remove() allowed for invalid parameters",
                () -> cache.remove(null),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows("findFloor() allowed for invalid parameters",
                () -> cache.findFloor("segment", -1),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows("findFloor() allowed for invalid parameters",
                () -> cache.findFloor(null, 1),
                ex -> ex instanceof IllegalArgumentException);
    }

}

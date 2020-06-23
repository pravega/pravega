/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.segmentstore.storage.chunklayer;

import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

/**
 * Tests for {@link ReadIndexCache}.
 */
public class ReadIndexCacheTests {
    @Test
    public void testSimpleChunkEviction() throws Exception {
        String segmentName = "testSegment";
        ReadIndexCache cache = new ReadIndexCache(10, 10, 10);

        Assert.assertEquals(0, cache.getOldestGeneration());
        Assert.assertEquals(0, cache.getCurrentGeneration());
        Assert.assertEquals(0, cache.getTotalChunksCount());
        Assert.assertEquals(0, cache.getTotalSegmentCount());

        // Add some entries.
        for (int i = 0; i < 10; i++) {
            cache.addIndexEntry(segmentName, "chunk" + i, i);
        }

        Assert.assertEquals(0, cache.getOldestGeneration());
        Assert.assertEquals(0, cache.getCurrentGeneration());
        Assert.assertEquals(10, cache.getTotalChunksCount());
        Assert.assertEquals(1, cache.getTotalSegmentCount());

        // Add check floor function works.
        for (int i = 0; i < 10; i++) {
            val floor = cache.findFloor(segmentName, i);
            Assert.assertEquals(i, floor.getOffset());
            Assert.assertEquals("chunk" + i, floor.getChunkName());
        }

        // Add more entries than cache capacity
        ArrayList<ChunkNameOffsetPair> entries = new ArrayList<ChunkNameOffsetPair>();
        for (int i = 10; i < 20; i++) {
            entries.add(new ChunkNameOffsetPair(i, "chunk" + i));
        }
        cache.addIndexEntries(segmentName, entries);

        // Check that cache is evicted as required.
        Assert.assertEquals(1, cache.getCurrentGeneration());
        Assert.assertEquals(10, cache.getTotalChunksCount());
        Assert.assertEquals(1, cache.getTotalSegmentCount());
        Assert.assertEquals(1, cache.getOldestGeneration());

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
        Assert.assertEquals(1, cache.getOldestGeneration());
        Assert.assertEquals(1, cache.getCurrentGeneration());
        Assert.assertEquals(5, cache.getTotalChunksCount());
        Assert.assertEquals(1, cache.getTotalSegmentCount());

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
    public void testSimpleSegmentEviction() throws Exception {

        ReadIndexCache cache = new ReadIndexCache(10, 10, 10);

        Assert.assertEquals(0, cache.getOldestGeneration());
        Assert.assertEquals(0, cache.getCurrentGeneration());
        Assert.assertEquals(0, cache.getTotalChunksCount());
        Assert.assertEquals(0, cache.getTotalSegmentCount());

        // Add some entries.
        for (int i = 0; i < 10; i++) {
            String segmentName = "testSegment";
            cache.addIndexEntry("testSegment" + i, "chunk0", 123);
        }

        Assert.assertEquals(0, cache.getOldestGeneration());
        Assert.assertEquals(0, cache.getCurrentGeneration());
        Assert.assertEquals(10, cache.getTotalChunksCount());
        Assert.assertEquals(10, cache.getTotalSegmentCount());

        // Add check floor function works.
        for (int i = 0; i < 10; i++) {
            val floor = cache.findFloor("testSegment" + i, 133);
            Assert.assertEquals(123, floor.getOffset());
            Assert.assertEquals("chunk0", floor.getChunkName());
        }

        int gen = 1;
        long oldGen = cache.getOldestGeneration();
        // Add more entries than cache capacity
        for (int i = 10; i < 20; i++) {
            String segmentName = "testSegment";
            cache.addIndexEntry("testSegment" + i, "chunk0", 123);
            Assert.assertEquals(++oldGen, cache.getOldestGeneration());
            Assert.assertEquals(gen++, cache.getCurrentGeneration());
            Assert.assertEquals(10, cache.getTotalChunksCount());
            Assert.assertEquals(10, cache.getTotalSegmentCount());
        }
    }

    @Test
    public void testSimpleChunkEvictionForMultipleSegments() throws Exception {
        ReadIndexCache cache = new ReadIndexCache(10, 10, 25);

        Assert.assertEquals(0, cache.getOldestGeneration());
        Assert.assertEquals(0, cache.getCurrentGeneration());
        Assert.assertEquals(0, cache.getTotalChunksCount());
        Assert.assertEquals(0, cache.getTotalSegmentCount());

        // Add some entries.
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 5; j++) {
                cache.addIndexEntry("testSegment" + i, "chunk" + j, j);
            }
        }

        Assert.assertEquals(0, cache.getOldestGeneration());
        Assert.assertEquals(0, cache.getCurrentGeneration());
        Assert.assertEquals(25, cache.getTotalChunksCount());
        Assert.assertEquals(5, cache.getTotalSegmentCount());

        // Add check floor function works.
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 5; j++) {
                val floor = cache.findFloor("testSegment" + i, j);
                Assert.assertEquals(j, floor.getOffset());
                Assert.assertEquals("chunk" + j, floor.getChunkName());
            }
        }

        long gen = 0;
        for (int i = 0; i < 5; i++) {
            for (int j = 5; j < 10; j++) {
                cache.addIndexEntry("testSegment" + i, "chunk" + j, j);
                //Assert.assertEquals(gen++, cache.getOldestGeneration());
                //Assert.assertEquals(gen, cache.getCurrentGeneration());
                Assert.assertEquals(25, cache.getTotalChunksCount());
                Assert.assertEquals(5, cache.getTotalSegmentCount());
            }
        }

        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 5; j++) {
                val floor = cache.findFloor("testSegment" + i, j);
                Assert.assertNull(floor);
            }
        }

        for (int i = 0; i < 5; i++) {
            for (int j = 5; j < 10; j++) {
                val floor = cache.findFloor("testSegment" + i, j);
                Assert.assertEquals(j, floor.getOffset());
                Assert.assertEquals("chunk" + j, floor.getChunkName());
            }
        }
    }

    @Test
    public void testSimpleAddRemove() throws Exception {
        String segmentName = "testSegment";
        ReadIndexCache cache = new ReadIndexCache(10, 10, 10);

        Assert.assertEquals(0, cache.getOldestGeneration());
        Assert.assertEquals(0, cache.getCurrentGeneration());
        Assert.assertEquals(0, cache.getTotalChunksCount());
        Assert.assertEquals(0, cache.getTotalSegmentCount());

        // Add some entries.
        cache.addIndexEntry(segmentName, "chunk0", 0);

        Assert.assertEquals(0, cache.getOldestGeneration());
        Assert.assertEquals(0, cache.getCurrentGeneration());
        Assert.assertEquals(1, cache.getTotalChunksCount());
        Assert.assertEquals(1, cache.getTotalSegmentCount());

        // Add check floor function works.
        val floor = cache.findFloor(segmentName, 0);
        Assert.assertEquals(0, floor.getOffset());
        Assert.assertEquals("chunk0", floor.getChunkName());

        // Remove
        cache.remove(segmentName);
        Assert.assertEquals(0, cache.getOldestGeneration());
        Assert.assertEquals(0, cache.getCurrentGeneration());
        Assert.assertEquals(0, cache.getTotalChunksCount());
        Assert.assertEquals(0, cache.getTotalSegmentCount());

        // Add more entries than cache capacity
        ArrayList<ChunkNameOffsetPair> entries = new ArrayList<ChunkNameOffsetPair>();
        entries.add(new ChunkNameOffsetPair(0, "chunk0"));
        cache.addIndexEntries(segmentName, entries);
        Assert.assertEquals(0, cache.getOldestGeneration());
        Assert.assertEquals(0, cache.getCurrentGeneration());
        Assert.assertEquals(1, cache.getTotalChunksCount());
        Assert.assertEquals(1, cache.getTotalSegmentCount());
    }

    @Test
    public void testMultiGenerationAddRemove() throws Exception {
        String segmentName = "testSegment";
        ReadIndexCache cache = new ReadIndexCache(5, 5, 5);

        Assert.assertEquals(0, cache.getOldestGeneration());
        Assert.assertEquals(0, cache.getCurrentGeneration());
        Assert.assertEquals(0, cache.getTotalChunksCount());
        Assert.assertEquals(0, cache.getTotalSegmentCount());

        // Add some entries.
        for (int i = 0; i < 5; i++) {
            cache.addIndexEntry(segmentName, "chunk" + i, i);
        }
        // Add some entries.
        for (int i = 5; i < 9; i++) {
            cache.addIndexEntry(segmentName, "chunk" + i, i);
        }

        Assert.assertEquals(0, cache.getOldestGeneration());
        Assert.assertEquals(4, cache.getCurrentGeneration());
        Assert.assertEquals(5, cache.getTotalChunksCount());
        Assert.assertEquals(1, cache.getTotalSegmentCount());
    }
}

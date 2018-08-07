/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import com.google.common.collect.ImmutableList;
import io.pravega.client.segment.impl.Segment;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import io.pravega.shared.segment.StreamSegmentNameUtils;
import org.junit.Test;

import static io.pravega.shared.segment.StreamSegmentNameUtils.computeSegmentId;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class StreamSegmentsTest {

    private final String scope = "scope";
    private final String streamName = "streamName";
    
    @Test
    public void testUsesAllSegments() {
        TreeMap<Double, Segment> segments = new TreeMap<>();
        segments.put(0.25, new Segment(scope, streamName, 0));
        segments.put(0.5, new Segment(scope, streamName, 1));
        segments.put(0.75, new Segment(scope, streamName, 2));
        segments.put(1.0, new Segment(scope, streamName, 3));
        StreamSegments streamSegments = new StreamSegments(segments, "");
        
        int[] counts = new int[4];
        Arrays.fill(counts, 0);
        for (int i = 0; i < 20; i++) {
            Segment segment = streamSegments.getSegmentForKey("" + i);
            assertNotNull(segment);
            counts[StreamSegmentNameUtils.getSegmentNumber(segment.getSegmentId())]++;
        }
        for (int count : counts) {
            assertTrue(count > 1);
        }
        
        Random r = new Random(0);
        Arrays.fill(counts, 0);
        for (int i = 0; i < 20; i++) {
            Segment segment = streamSegments.getSegmentForKey(r.nextDouble());
            assertNotNull(segment);
            counts[StreamSegmentNameUtils.getSegmentNumber(segment.getSegmentId())]++;
        }
        for (int count : counts) {
            assertTrue(count > 1);
        }
    }
    
    @Test
    public void testRangeReplacementSplit() {
        TreeMap<Double, Segment> segments = new TreeMap<>();
        segments.put(0.5, new Segment(scope, streamName, 0));
        segments.put(1.0, new Segment(scope, streamName, 1));
        StreamSegments streamSegments = new StreamSegments(segments, "");
        Map<SegmentWithRange, List<Long>> newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(new Segment(scope, streamName, 2L), 0, 0.25), ImmutableList.of(0L));
        newRange.put(new SegmentWithRange(new Segment(scope, streamName, 3L), 0.25, 0.5), ImmutableList.of(0L));
        streamSegments = streamSegments.withReplacementRange(getSegment(0, 0), new StreamSegmentsWithPredecessors(newRange, ""));
        newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(new Segment(scope, streamName, 4L), 0.5, 0.75), ImmutableList.of(1L));
        newRange.put(new SegmentWithRange(new Segment(scope, streamName, 5L), 0.75, 1.0), ImmutableList.of(1L));
        streamSegments = streamSegments.withReplacementRange(getSegment(1, 0), new StreamSegmentsWithPredecessors(newRange, ""));
        
        int[] counts = new int[6];
        Arrays.fill(counts, 0);
        for (int i = 0; i < 20; i++) {
            Segment segment = streamSegments.getSegmentForKey("" + i);
            assertNotNull(segment);
            counts[StreamSegmentNameUtils.getSegmentNumber(segment.getSegmentId())]++;
        }
        assertEquals(0, counts[0]);
        assertEquals(0, counts[1]);
        assertTrue(counts[2] > 1);
        assertTrue(counts[3] > 1);
        assertTrue(counts[4] > 1);
        assertTrue(counts[5] > 1);
    }
    
    //    @Test
    //    public void testRangeReplacementMerge() {
    //        TreeMap<Double, Segment> segments = new TreeMap<>();
    //        segments.put(0.25, new Segment(scope, streamName, 0));
    //        segments.put(0.5, new Segment(scope, streamName, 1));
    //        segments.put(0.75, new Segment(scope, streamName, 2));
    //        segments.put(1.0, new Segment(scope, streamName, 3));
    //        StreamSegments streamSegments = new StreamSegments(segments, "");
    //        Map<SegmentWithRange, List<Long>> newRange = new HashMap<>();
    //        newRange.put(new SegmentWithRange(new Segment(scope, streamName, 4L), 0, 0.5), ImmutableList.of(0L, 1L));
    //        newRange.put(new SegmentWithRange(new Segment(scope, streamName, 5L), 0.5, 1.0), ImmutableList.of(2L, 3L));
    //        streamSegments = streamSegments.withReplacementRange(new StreamSegmentsWithPredecessors(newRange, ""));
    //        newRange = new HashMap<>();
    //        newRange.put(new SegmentWithRange(new Segment(scope, streamName, 6L), 0.0, 1.0), ImmutableList.of(4L, 5L));
    //        streamSegments = streamSegments.withReplacementRange(new StreamSegmentsWithPredecessors(newRange, ""));
    //
    //        int[] counts = new int[7];
    //        Arrays.fill(counts, 0);
    //        for (int i = 0; i < 20; i++) {
    //            Segment segment = streamSegments.getSegmentForKey("" + i);
    //            assertNotNull(segment);
    //            counts[StreamSegmentNameUtils.getSegmentNumber(segment.getSegmentId())]++;
    //        }
    //        assertEquals(0, counts[0]);
    //        assertEquals(0, counts[1]);
    //        assertEquals(0, counts[2]);
    //        assertEquals(0, counts[3]);
    //        assertEquals(0, counts[4]);
    //        assertEquals(0, counts[5]);
    //        assertEquals(20, counts[6]);
    //    }

    @Test
    public void testSameRoutingKey() {
        TreeMap<Double, Segment> segments = new TreeMap<>();
        segments.put(0.25, new Segment(scope, streamName, 0));
        segments.put(0.5, new Segment(scope, streamName, 1));
        segments.put(0.75, new Segment(scope, streamName, 2));
        segments.put(1.0, new Segment(scope, streamName, 3));
        StreamSegments streamSegments = new StreamSegments(segments, "");

        int[] counts = new int[4];
        Arrays.fill(counts, 0);
        for (int i = 0; i < 20; i++) {
            Segment segment = streamSegments.getSegmentForKey("Foo");
            assertNotNull(segment);
            counts[StreamSegmentNameUtils.getSegmentNumber(segment.getSegmentId())]++;
        }
        assertArrayEquals(new int[] { 20, 0, 0, 0 }, counts);
    }

    /*
  Segment map used by the test.
           ^
           |
    1.0    +---------------------------------------------
           |               |
           |    1          |    5
    0.75   |               +--------+--------------------
           |               |    4   |
           |               |        |
    0.5    +---------+-----+--------+        8
           |         |              |
           |         |     3        |
           |         |              |
   0.25    |   0     +----------+---+--------------------
           |         |          |   7
           |         |     2    +--------------+---------
           |         |          |   6
   0.0  +------------+----------+--------------------------
           |
           0         1     2    3   4
    */
    @Test
    public void testRangeReplacementDuringScale() {
        TreeMap<Double, Segment> segments = new TreeMap<>();
        segments.put(0.5, getSegment(0, 0));
        segments.put(1.0, getSegment(1, 0));
        StreamSegments streamSegments = new StreamSegments(segments, "");

        //simulate fetch successors of segment 0.
        Map<SegmentWithRange, List<Long>> newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(getSegment(2, 1), 0, 0.25),
                ImmutableList.of(computeSegmentId(0, 0)));
        newRange.put(new SegmentWithRange(getSegment(3, 1), 0.25, 0.5),
                ImmutableList.of(computeSegmentId(0, 0)));
        streamSegments = streamSegments.withReplacementRange(getSegment(0, 0), new StreamSegmentsWithPredecessors(newRange, ""));

        assertEquals(getSegment(2, 1), streamSegments.getSegmentForKey(0.1));
        assertEquals(getSegment(3, 1), streamSegments.getSegmentForKey(0.3));
        assertEquals(getSegment(1, 0), streamSegments.getSegmentForKey(0.6));

        //simulate fetch successors for segment 2.
        newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(getSegment(6, 3), 0, 0.125),
                ImmutableList.of(computeSegmentId(2, 1)));
        newRange.put(new SegmentWithRange(getSegment(7, 3), 0.125, 0.25),
                ImmutableList.of(computeSegmentId(2, 1)));
        streamSegments = streamSegments.withReplacementRange(getSegment(2, 1), new StreamSegmentsWithPredecessors(newRange, ""));

        assertEquals(getSegment(6, 3), streamSegments.getSegmentForKey(0.1));
        assertEquals(getSegment(7, 3), streamSegments.getSegmentForKey(0.24));
        assertEquals(getSegment(3, 1), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(1, 0), streamSegments.getSegmentForKey(0.7));

        // simulate fetch successors for segment 3.
        newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(getSegment(8, 4), 0.25, 0.75),
                ImmutableList.of(computeSegmentId(3, 1), computeSegmentId(4, 2)));
        streamSegments = streamSegments.withReplacementRange(getSegment(3, 1), new StreamSegmentsWithPredecessors(newRange, ""));

        assertEquals(getSegment(6, 3), streamSegments.getSegmentForKey(0.1));
        assertEquals(getSegment(7, 3), streamSegments.getSegmentForKey(0.24));
        assertEquals(getSegment(8, 4), streamSegments.getSegmentForKey(0.4));
        //this should not return segment 8 as segment 4 is never updated.
        assertEquals(getSegment(1, 0), streamSegments.getSegmentForKey(0.7));

        // simulate fetch successors for segment 1.
        newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(getSegment(4, 2), 0.5, 0.75),
                ImmutableList.of(computeSegmentId(1, 0)));
        newRange.put(new SegmentWithRange(getSegment(5, 2), 0.75, 1.0),
                ImmutableList.of(computeSegmentId(1, 0)));
        streamSegments = streamSegments.withReplacementRange(getSegment(1, 0), new StreamSegmentsWithPredecessors(newRange, ""));

        assertEquals(getSegment(6, 3), streamSegments.getSegmentForKey(0.1));
        assertEquals(getSegment(7, 3), streamSegments.getSegmentForKey(0.24));
        // this should now return segment 8 as before
        assertEquals(getSegment(8, 4), streamSegments.getSegmentForKey(0.4));
        //this should now return segment 4 and not segment 1 or segment 8
        assertEquals(getSegment(4, 2), streamSegments.getSegmentForKey(0.7));
        //this should now return segment 5 and not segment 1.
        assertEquals(getSegment(5, 2), streamSegments.getSegmentForKey(0.9));

        // simulate fetch successors for segment 4.
        newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(getSegment(8, 4), 0.25, 0.75),
                ImmutableList.of(computeSegmentId(3, 1), computeSegmentId(4, 2)));
        streamSegments = streamSegments.withReplacementRange(getSegment(4, 2), new StreamSegmentsWithPredecessors(newRange, ""));

        assertEquals(getSegment(6, 3), streamSegments.getSegmentForKey(0.1));
        assertEquals(getSegment(7, 3), streamSegments.getSegmentForKey(0.24));
        assertEquals(getSegment(8, 4), streamSegments.getSegmentForKey(0.4));
        // this should now return segment 8 and not segment 4
        assertEquals(getSegment(8, 4), streamSegments.getSegmentForKey(0.7));
        assertEquals(getSegment(5, 2), streamSegments.getSegmentForKey(0.9));

    }

    @Test
    public void testRangeReplacementMultipleSegmentMerge() {
        TreeMap<Double, Segment> segments = new TreeMap<>();
        segments.put(0.33, getSegment(0, 0));
        segments.put(0.66, getSegment(1, 0));
        segments.put(1.0, getSegment(2, 0));
        StreamSegments streamSegments = new StreamSegments(segments, "");

        // All the three segments are merged into a single segment 3.

        // Simulate fetch successors of segment 1.
        Map<SegmentWithRange, List<Long>> newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(getSegment(3, 1), 0, 1.0),
                ImmutableList.of(computeSegmentId(0, 0), computeSegmentId(1, 0), computeSegmentId(2, 0)));
        streamSegments = streamSegments.withReplacementRange(getSegment(1, 0), new StreamSegmentsWithPredecessors(newRange, ""));

        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(3, 1), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(2, 0), streamSegments.getSegmentForKey(0.8));
    }

    private Segment getSegment(int segmentNumber, int epoch) {
        return new Segment(scope, streamName, computeSegmentId(segmentNumber, epoch));
    }

}

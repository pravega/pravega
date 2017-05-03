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
package io.pravega.client.stream.impl;

import com.google.common.collect.ImmutableList;
import io.pravega.client.stream.Segment;
import io.pravega.client.stream.SegmentWithRange;
import io.pravega.client.stream.StreamSegmentsWithPredecessors;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Test;

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
        StreamSegments streamSegments = new StreamSegments(segments);
        
        int[] counts = new int[4];
        Arrays.fill(counts, 0);
        for (int i = 0; i < 20; i++) {
            Segment segment = streamSegments.getSegmentForKey("" + i);
            assertNotNull(segment);
            counts[segment.getSegmentNumber()]++;
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
        StreamSegments streamSegments = new StreamSegments(segments);
        Map<SegmentWithRange, List<Integer>> newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(new Segment(scope, streamName, 2), 0, 0.25), ImmutableList.of(0));
        newRange.put(new SegmentWithRange(new Segment(scope, streamName, 3), 0.25, 0.5), ImmutableList.of(0));
        streamSegments = streamSegments.withReplacementRange(new StreamSegmentsWithPredecessors(newRange));
        newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(new Segment(scope, streamName, 4), 0.5, 0.75), ImmutableList.of(1));
        newRange.put(new SegmentWithRange(new Segment(scope, streamName, 5), 0.75, 1.0), ImmutableList.of(1));
        streamSegments = streamSegments.withReplacementRange(new StreamSegmentsWithPredecessors(newRange));
        
        int[] counts = new int[6];
        Arrays.fill(counts, 0);
        for (int i = 0; i < 20; i++) {
            Segment segment = streamSegments.getSegmentForKey("" + i);
            assertNotNull(segment);
            counts[segment.getSegmentNumber()]++;
        }
        assertEquals(0, counts[0]);
        assertEquals(0, counts[1]);
        assertTrue(counts[2] > 1);
        assertTrue(counts[3] > 1);
        assertTrue(counts[4] > 1);
        assertTrue(counts[5] > 1);
    }
    
    @Test
    public void testRangeReplacementMerge() {
        TreeMap<Double, Segment> segments = new TreeMap<>();
        segments.put(0.25, new Segment(scope, streamName, 0));
        segments.put(0.5, new Segment(scope, streamName, 1));
        segments.put(0.75, new Segment(scope, streamName, 2));
        segments.put(1.0, new Segment(scope, streamName, 3));
        StreamSegments streamSegments = new StreamSegments(segments);
        Map<SegmentWithRange, List<Integer>> newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(new Segment(scope, streamName, 4), 0, 0.5), ImmutableList.of(0, 1));
        newRange.put(new SegmentWithRange(new Segment(scope, streamName, 5), 0.5, 1.0), ImmutableList.of(2, 3));
        streamSegments = streamSegments.withReplacementRange(new StreamSegmentsWithPredecessors(newRange));
        newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(new Segment(scope, streamName, 6), 0.0, 1.0), ImmutableList.of(4, 5));
        streamSegments = streamSegments.withReplacementRange(new StreamSegmentsWithPredecessors(newRange));
        
        int[] counts = new int[7];
        Arrays.fill(counts, 0);
        for (int i = 0; i < 20; i++) {
            Segment segment = streamSegments.getSegmentForKey("" + i);
            assertNotNull(segment);
            counts[segment.getSegmentNumber()]++;
        }
        assertEquals(0, counts[0]);
        assertEquals(0, counts[1]);
        assertEquals(0, counts[2]);
        assertEquals(0, counts[3]);
        assertEquals(0, counts[4]);
        assertEquals(0, counts[5]);
        assertEquals(20, counts[6]);
    }
    
    @Test
    public void testSameRoutingKey() {
        TreeMap<Double, Segment> segments = new TreeMap<>();
        segments.put(0.25, new Segment(scope, streamName, 0));
        segments.put(0.5, new Segment(scope, streamName, 1));
        segments.put(0.75, new Segment(scope, streamName, 2));
        segments.put(1.0, new Segment(scope, streamName, 3));
        StreamSegments streamSegments = new StreamSegments(segments);

        int[] counts = new int[4];
        Arrays.fill(counts, 0);
        for (int i = 0; i < 20; i++) {
            Segment segment = streamSegments.getSegmentForKey("Foo");
            assertNotNull(segment);
            counts[segment.getSegmentNumber()]++;
        }
        assertArrayEquals(new int[] { 20, 0, 0, 0 }, counts);
    }

}

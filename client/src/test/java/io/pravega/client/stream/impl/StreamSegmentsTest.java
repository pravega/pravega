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
package io.pravega.client.stream.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import io.pravega.client.segment.impl.Segment;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.AssertExtensions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import lombok.val;
import org.junit.Test;

import static io.pravega.shared.NameUtils.computeSegmentId;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class StreamSegmentsTest {

    private final String scope = "scope";
    private final String streamName = "streamName";

    @Test
    public void testInvalidInput() {
        val s = new Segment("a", "b", 9L);
        val n1 = new TreeMap<Double, SegmentWithRange>();
        n1.put(0.0, new SegmentWithRange(s, 0.0, 0.1));
        AssertExtensions.assertThrows(
                "",
                () -> new StreamSegments(n1),
                ex -> ex instanceof IllegalArgumentException);
        n1.clear();
        n1.put(0.5, new SegmentWithRange(s, 0.5, 0.6));
        AssertExtensions.assertThrows(
                "",
                () -> new StreamSegments(n1),
                ex -> ex instanceof IllegalArgumentException);
        n1.clear();
        n1.put(2.0, new SegmentWithRange(s, 1.0, 1.0));
        AssertExtensions.assertThrows(
                "",
                () -> new StreamSegments(n1),
                ex -> ex instanceof IllegalArgumentException);
        n1.clear();
        val s1 = new StreamSegments(n1);
        AssertExtensions.assertThrows(
                "",
                () -> s1.getSegmentForKey(-1),
                ex -> ex instanceof IllegalArgumentException);
        AssertExtensions.assertThrows(
                "",
                () -> s1.getSegmentForKey(2),
                ex -> ex instanceof IllegalArgumentException);
    }

    @Test
    public void testUsesAllSegments() {
        StreamSegments streamSegments = initStreamSegments(4);

        int[] counts = new int[4];
        Arrays.fill(counts, 0);
        for (int i = 0; i < 20; i++) {
            Segment segment = streamSegments.getSegmentForKey("" + i);
            assertNotNull(segment);
            counts[NameUtils.getSegmentNumber(segment.getSegmentId())]++;
        }
        for (int count : counts) {
            assertTrue(count > 1);
        }
        
        Random r = new Random(0);
        Arrays.fill(counts, 0);
        for (int i = 0; i < 20; i++) {
            Segment segment = streamSegments.getSegmentForKey(r.nextDouble());
            assertNotNull(segment);
            counts[NameUtils.getSegmentNumber(segment.getSegmentId())]++;
        }
        for (int count : counts) {
            assertTrue(count > 1);
        }
    }
    
    @Test
    public void testRangeReplacementSplit() {
        TreeMap<Double, SegmentWithRange> segments = new TreeMap<>();
        addNewSegment(segments, 0, 0.0, 0.5);
        addNewSegment(segments, 1, 0.5, 1.0);
        StreamSegments streamSegments = new StreamSegments(segments);
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
            counts[NameUtils.getSegmentNumber(segment.getSegmentId())]++;
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
        StreamSegments streamSegments = initStreamSegments(4);

        // simulate successors for 0
        Map<SegmentWithRange, List<Long>> newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(new Segment(scope, streamName, computeSegmentId(4, 1)), 0, 0.5), ImmutableList.of(0L, 1L));
        streamSegments = streamSegments.withReplacementRange(getSegment(0, 0), new StreamSegmentsWithPredecessors(newRange, ""));

        // verify mapping
        assertEquals(getSegment(4, 1), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(1, 0), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(2, 0), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(3, 0), streamSegments.getSegmentForKey(0.8));

        // simulate successors for 1
        streamSegments = streamSegments.withReplacementRange(getSegment(1, 0), new StreamSegmentsWithPredecessors(newRange, ""));
        //verify mapping
        assertEquals(getSegment(4, 1), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(4, 1), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(2, 0), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(3, 0), streamSegments.getSegmentForKey(0.8));

        // simulate successor for 2
        newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(new Segment(scope, streamName, computeSegmentId(5, 1)), 0.5, 1.0),
                     ImmutableList.of(2L, 3L));
        streamSegments = streamSegments.withReplacementRange(getSegment(2, 0), new StreamSegmentsWithPredecessors(newRange, ""));
        //verify mapping
        assertEquals(getSegment(4, 1), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(4, 1), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(5, 1), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(3, 0), streamSegments.getSegmentForKey(0.8));

        // simulate successor for 3
        streamSegments = streamSegments.withReplacementRange(getSegment(3, 0), new StreamSegmentsWithPredecessors(newRange, ""));
        //verify mapping
        assertEquals(getSegment(4, 1), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(4, 1), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(5, 1), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(5, 1), streamSegments.getSegmentForKey(0.8));

        // simulate successor for 4
        newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(new Segment(scope, streamName, computeSegmentId(6, 2)), 0.0, 1.0),
                     ImmutableList.of(computeSegmentId(4, 1), computeSegmentId(5, 1)));
        streamSegments = streamSegments.withReplacementRange(getSegment(4, 1), new StreamSegmentsWithPredecessors(newRange, ""));
        //verify mapping
        assertEquals(getSegment(6, 2), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(6, 2), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(5, 1), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(5, 1), streamSegments.getSegmentForKey(0.8));

        // simulate successor for 5
        newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(new Segment(scope, streamName, computeSegmentId(6, 2)), 0.0, 1.0),
                     ImmutableList.of(computeSegmentId(4, 1), computeSegmentId(5, 1)));
        streamSegments = streamSegments.withReplacementRange(getSegment(5, 1), new StreamSegmentsWithPredecessors(newRange, ""));
        //verify mapping
        assertEquals(getSegment(6, 2), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(6, 2), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(6, 2), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(6, 2), streamSegments.getSegmentForKey(0.8));

        int[] counts = new int[7];
        Arrays.fill(counts, 0);
        for (int i = 0; i < 20; i++) {
            Segment segment = streamSegments.getSegmentForKey("" + i);
            assertNotNull(segment);
            counts[NameUtils.getSegmentNumber(segment.getSegmentId())]++;
        }
        assertEquals(0, counts[0]);
        assertEquals(0, counts[1]);
        assertEquals(0, counts[2]);
        assertEquals(0, counts[3]);
        assertEquals(0, counts[4]);
        assertEquals(0, counts[5]);
        assertEquals(20, counts[6]);
    }

    private StreamSegments initStreamSegments(int num) {
        TreeMap<Double, SegmentWithRange> segments = new TreeMap<>();
        double stride = 1.0 / num;
        for (int i = 0; i < num; i++) {
            addNewSegment(segments, i, i * stride, (i + 1) * stride);
        }
        StreamSegments streamSegments = new StreamSegments(segments);
        return streamSegments;
    }

    @Test
    public void testSameRoutingKey() {
        StreamSegments streamSegments = initStreamSegments(4);

        int[] counts = new int[4];
        Arrays.fill(counts, 0);
        for (int i = 0; i < 20; i++) {
            Segment segment = streamSegments.getSegmentForKey("Foo");
            assertNotNull(segment);
            counts[NameUtils.getSegmentNumber(segment.getSegmentId())]++;
        }
        assertArrayEquals(new int[] { 20, 0, 0, 0 }, counts);
    }

    /**
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
        StreamSegments streamSegments = initStreamSegments(2);

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
        TreeMap<Double, SegmentWithRange> segments = new TreeMap<>();
        addNewSegment(segments, 0, 0, 0.33);
        addNewSegment(segments, 1, 0.33, 0.66);
        addNewSegment(segments, 2, 0.66, 1);
        StreamSegments streamSegments = new StreamSegments(segments);

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

    /**
      Segment Map used by the test.
             ^
             |
         1.0 +---------+---------+-------+
             |         |         |
             |    2    |         |
             |         |         |
         0.66+---------|         |
             |         |         |   5
             |    1    |   3     |
             |         |         |
         0.33+---------|         +--------+
             |         |         |
             |    0    |         |    4
             |         |         |
             +------------+---------+------------------>
             +         1         2
     */
    @Test
    public void testRangeReplacementMultipleSegmentMergeSplitOrder() {
        TreeMap<Double, SegmentWithRange> segments = new TreeMap<>();
        addNewSegment(segments, 0, 0, 0.33);
        addNewSegment(segments, 1, 0.33, 0.66);
        addNewSegment(segments, 2, 0.66, 1);
        StreamSegments streamSegments = new StreamSegments(segments);

        // All the three segments are merged into a single segment 3.
        Map<SegmentWithRange, List<Long>> newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(getSegment(3, 1), 0, 1.0),
                     ImmutableList.of(computeSegmentId(0, 0), computeSegmentId(1, 0), computeSegmentId(2, 0)));

        // Simulate fetch successors for segment 2.
        streamSegments = streamSegments.withReplacementRange(getSegment(2, 0), new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(1, 0), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(3, 1), streamSegments.getSegmentForKey(0.8));

        // Simulate fetch successors for segment 0.
        streamSegments = streamSegments.withReplacementRange(getSegment(0, 0), new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(3, 1), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(1, 0), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(3, 1), streamSegments.getSegmentForKey(0.8));

        // Simulate fetch successor for segment 1.
        streamSegments = streamSegments.withReplacementRange(getSegment(1, 0), new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(3, 1), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(3, 1), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(3, 1), streamSegments.getSegmentForKey(0.8));

        // Simulate fetch successor for segment 3.
        newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(getSegment(4, 2), 0, 0.33), ImmutableList.of(computeSegmentId(3, 1)));
        newRange.put(new SegmentWithRange(getSegment(5, 2), 0.33, 1.0), ImmutableList.of(computeSegmentId(3, 1)));
        streamSegments = streamSegments.withReplacementRange(getSegment(3, 1), new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify
        assertEquals(getSegment(4, 2), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(5, 2), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(5, 2), streamSegments.getSegmentForKey(0.8));
    }

    /**
    Segment Map used by the test.
           ^
           |
       1.0 +---------+---------+-------->
           |         |         |
           |    2    |         |    5
           |         |    3    |
       0.66+---------|         +-------->
           |         |         |
           |    1    |---------+    6
           |         |         |
       0.33+---------|         |-------->
           |         |    4    |
           |    0    |         |    7
           |         |         |
           +-------------------+-------->
           +         1         2
     */
    @Test
    public void testUnevenRangeReplacement() {
        TreeMap<Double, SegmentWithRange> segments = new TreeMap<>();
        addNewSegment(segments, 0, 0, 0.33);
        addNewSegment(segments, 1, 0.33, 0.66);
        addNewSegment(segments, 2, 0.66, 1);
        StreamSegments streamSegments = new StreamSegments(segments);

        // Verify.
        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(1, 0), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(1, 0), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(2, 0), streamSegments.getSegmentForKey(0.8));
        
        // 1 and 2 are merged into 3.
        Map<SegmentWithRange, List<Long>> newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(getSegment(3, 1), 0.5, 1.0),
                     ImmutableList.of(computeSegmentId(1, 0), computeSegmentId(2, 0)));
        // 1 and 0 are merged into 4.
        newRange.put(new SegmentWithRange(getSegment(4, 1), 0.0, 0.5),
                     ImmutableList.of(computeSegmentId(1, 0), computeSegmentId(0, 0)));

        // Simulate fetch successors for segment 1.
        streamSegments = streamSegments.withReplacementRange(getSegment(1, 0),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(4, 1), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(3, 1), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(2, 0), streamSegments.getSegmentForKey(0.8));

        newRange = new HashMap<>();
        // 1 and 0 are merged into 4.
        newRange.put(new SegmentWithRange(getSegment(4, 1), 0.0, 0.5),
                     ImmutableList.of(computeSegmentId(1, 0), computeSegmentId(0, 0)));

        // Simulate fetch successors for segment 0.
        streamSegments = streamSegments.withReplacementRange(getSegment(0, 0),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(4, 1), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(4, 1), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(3, 1), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(2, 0), streamSegments.getSegmentForKey(0.8));

        // 1 and 2 are merged into 3.
        newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(getSegment(3, 1), 0.5, 1.0),
                     ImmutableList.of(computeSegmentId(1, 0), computeSegmentId(2, 0)));

        // Simulate fetch successor for segment 2.
        streamSegments = streamSegments.withReplacementRange(getSegment(2, 0),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(4, 1), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(4, 1), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(3, 1), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(3, 1), streamSegments.getSegmentForKey(0.8));

        // 3 is merged into 5 and 6.
        newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(getSegment(5, 2), 0.66, 1.0), ImmutableList.of(computeSegmentId(3, 1)));
        newRange.put(new SegmentWithRange(getSegment(6, 2), 0.33, 0.66),
                     ImmutableList.of(computeSegmentId(3, 1), computeSegmentId(4, 1)));
        // Simulate fetch successors for segment 3.
        streamSegments = streamSegments.withReplacementRange(getSegment(3, 1),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(4, 1), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(4, 1), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(6, 2), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(5, 2), streamSegments.getSegmentForKey(0.8));

        // 4 is merged into 6 and 7.
        newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(getSegment(6, 2), 0.33, 0.66),
                     ImmutableList.of(computeSegmentId(3, 1), computeSegmentId(4, 1)));
        newRange.put(new SegmentWithRange(getSegment(7, 2), 0.0, 0.33), ImmutableList.of(computeSegmentId(4, 1)));

        // Simulate fetch successors for segment 4.
        streamSegments = streamSegments.withReplacementRange(getSegment(4, 1),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(7, 2), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(6, 2), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(6, 2), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(5, 2), streamSegments.getSegmentForKey(0.8));

    }

    
    /**
    Segment Map used by the test.
           ^
           |
       1.0 +---------------------------->
           |                   
           |    3                  
           |                   
       0.75+---------+---------+-------->
           |         |         |
           |    2    |         |    
           |         |         |
       0.50+---------|    4    |
           |         |         |
           |    1    |         |    5
           |         |         |
       0.25+---------+---------+
           |                   |
           |    0              |
           |                   |
       0.0 +-------------------+--------->
           +         1         2
     */
    @Test
    public void testDoubleMergeLow() {
        StreamSegments streamSegments = initStreamSegments(4);

        // Verify.
        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(1, 0), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(2, 0), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(3, 0), streamSegments.getSegmentForKey(0.8));
        
        // 1 and 2 are merged into 4.
        Map<SegmentWithRange, List<Long>> newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(getSegment(4, 1), 0.25, 0.75),
                     ImmutableList.of(computeSegmentId(1, 0), computeSegmentId(2, 0)));
    
        // Simulate fetch successors for segment 1.
        streamSegments = streamSegments.withReplacementRange(getSegment(1, 0),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(4, 1), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(2, 0), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(3, 0), streamSegments.getSegmentForKey(0.8));

        newRange = new HashMap<>();
        // 1 and 2 are merged into 4.
        newRange.put(new SegmentWithRange(getSegment(4, 1), 0.25, 0.75),
                     ImmutableList.of(computeSegmentId(1, 0), computeSegmentId(2, 0)));

        // Simulate fetch successors for segment 2.
        streamSegments = streamSegments.withReplacementRange(getSegment(2, 0),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(4, 1), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(4, 1), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(3, 0), streamSegments.getSegmentForKey(0.8));
        
        newRange = new HashMap<>();
        // 0 and 4 are merged into 5.
        newRange.put(new SegmentWithRange(getSegment(5, 2), 0.0, 0.75),
                     ImmutableList.of(computeSegmentId(0, 0), computeSegmentId(4, 1)));

        // Simulate fetch successors for segment 4.
        streamSegments = streamSegments.withReplacementRange(getSegment(4, 1),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(5, 2), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(5, 2), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(3, 0), streamSegments.getSegmentForKey(0.8));
        
        // Simulate fetch successors for segment 0.
        streamSegments = streamSegments.withReplacementRange(getSegment(0, 0),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        
        assertEquals(getSegment(5, 2), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(5, 2), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(5, 2), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(3, 0), streamSegments.getSegmentForKey(0.8));

    }
    
    /**
    Segment Map used by the test.
           ^
           |
       1.0 +---------------------------->
           |                   |
           |    3              |    
           |                   |
       0.75+---------+---------+
           |         |         |
           |    2    |         |    5
           |         |         |
       0.50+---------|    4    |
           |         |         |
           |    1    |         |    
           |         |         |
       0.25+---------+---------+-------->
           |                   
           |    0              
           |                   
       0.0 +-------------------+--------->
           +         1         2
     */
    @Test
    public void testDoubleMergeHigh() {
        StreamSegments streamSegments = initStreamSegments(4);

        // Verify.
        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(1, 0), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(2, 0), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(3, 0), streamSegments.getSegmentForKey(0.8));
        
        // 1 and 2 are merged into 4.
        Map<SegmentWithRange, List<Long>> newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(getSegment(4, 1), 0.25, 0.75),
                     ImmutableList.of(computeSegmentId(1, 0), computeSegmentId(2, 0)));
    
        // Simulate fetch successors for segment 1.
        streamSegments = streamSegments.withReplacementRange(getSegment(1, 0),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(4, 1), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(2, 0), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(3, 0), streamSegments.getSegmentForKey(0.8));

        newRange = new HashMap<>();
        // 1 and 2 are merged into 4.
        newRange.put(new SegmentWithRange(getSegment(4, 1), 0.25, 0.75),
                     ImmutableList.of(computeSegmentId(1, 0), computeSegmentId(2, 0)));

        // Simulate fetch successors for segment 2.
        streamSegments = streamSegments.withReplacementRange(getSegment(2, 0),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(4, 1), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(4, 1), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(3, 0), streamSegments.getSegmentForKey(0.8));
        
        newRange = new HashMap<>();
        // 3 and 4 are merged into 5.
        newRange.put(new SegmentWithRange(getSegment(5, 2), 0.25, 1.0),
                     ImmutableList.of(computeSegmentId(3, 0), computeSegmentId(4, 1)));
        // Simulate fetch successors for segment 4.
        streamSegments = streamSegments.withReplacementRange(getSegment(4, 1),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(5, 2), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(5, 2), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(3, 0), streamSegments.getSegmentForKey(0.8));
        // Simulate fetch successors for segment 3.
        streamSegments = streamSegments.withReplacementRange(getSegment(3, 0),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(5, 2), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(5, 2), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(5, 2), streamSegments.getSegmentForKey(0.8));
    }
    
    /**
    Segment Map used by the test.
           ^
           |
       1.0 +----------------------------->
           |                   |
           |    3              |    
           |                   |
       0.75+---------+---------+
           |         |         |
           |    2    |         |    5
           |         |         |
       0.50+---------|    4    +--------->
           |         |         |
           |    1    |         |    
           |         |         |
       0.25+---------+---------+    6
           |                   |
           |    0              |
           |                   |
       0.0 +-------------------+--------->
           +         1         2
     */
    @Test
    public void testDoubleMergeOffset() {
        StreamSegments streamSegments = initStreamSegments(4);

        // Verify.
        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(1, 0), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(2, 0), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(3, 0), streamSegments.getSegmentForKey(0.8));
        
        // 1 and 2 are merged into 4.
        Map<SegmentWithRange, List<Long>> newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(getSegment(4, 1), 0.25, 0.75),
                     ImmutableList.of(computeSegmentId(1, 0), computeSegmentId(2, 0)));
        // Simulate fetch successors for segment 1.
        streamSegments = streamSegments.withReplacementRange(getSegment(1, 0),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(4, 1), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(2, 0), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(3, 0), streamSegments.getSegmentForKey(0.8));

        newRange = new HashMap<>();
        // 1 and 2 are merged into 4.
        newRange.put(new SegmentWithRange(getSegment(4, 1), 0.25, 0.75),
                     ImmutableList.of(computeSegmentId(1, 0), computeSegmentId(2, 0)));
        // Simulate fetch successors for segment 2.
        streamSegments = streamSegments.withReplacementRange(getSegment(2, 0),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(4, 1), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(4, 1), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(3, 0), streamSegments.getSegmentForKey(0.8));
        
        newRange = new HashMap<>();
        // 3 and 4 are merged into 5.
        newRange.put(new SegmentWithRange(getSegment(5, 2), 0.5, 1.0),
                     ImmutableList.of(computeSegmentId(3, 0), computeSegmentId(4, 1)));
        // 0 and 4 are merged into 6.
        newRange.put(new SegmentWithRange(getSegment(6, 2), 0.0, 0.5),
                     ImmutableList.of(computeSegmentId(0, 0), computeSegmentId(4, 1)));
        // Simulate fetch successors for segment 4.
        streamSegments = streamSegments.withReplacementRange(getSegment(4, 1),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(6, 2), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(5, 2), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(3, 0), streamSegments.getSegmentForKey(0.8));
        // Simulate fetch successors for segment 3.
        streamSegments = streamSegments.withReplacementRange(getSegment(3, 0),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(6, 2), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(5, 2), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(5, 2), streamSegments.getSegmentForKey(0.8));
        // Simulate fetch successors for segment 0.
        streamSegments = streamSegments.withReplacementRange(getSegment(0, 0),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(6, 2), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(6, 2), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(5, 2), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(5, 2), streamSegments.getSegmentForKey(0.8));
    }
    
    
    
    /**
    Segment Map used by the test.
           ^
           |
       1.0 +---------------------------->
           |                   
           |    3                  
           |                   
       0.75+---------+---------+-------->
           |         |         |
           |    2    |         |    
           |         |         |
       0.50+---------|    4    |
           |         |         |
           |    1    |         |    5
           |         |         |
       0.25+---------+---------+
           |                   |
           |    0              |
           |                   |
       0.0 +-------------------+--------->
           +         1         2
     */
    @Test
    public void testDoubleMergeOutOfOrder() {
        StreamSegments streamSegments = initStreamSegments(4);

        // Verify.
        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(1, 0), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(2, 0), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(3, 0), streamSegments.getSegmentForKey(0.8));
        
        // 1 and 2 are merged into 4.
        Map<SegmentWithRange, List<Long>> newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(getSegment(4, 1), 0.25, 0.75),
                     ImmutableList.of(computeSegmentId(1, 0), computeSegmentId(2, 0)));
        // Simulate fetch successors for segment 1.
        streamSegments = streamSegments.withReplacementRange(getSegment(1, 0),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(4, 1), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(2, 0), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(3, 0), streamSegments.getSegmentForKey(0.8));

        newRange = new HashMap<>();
        // 0 and 4 are merged into 5.
        newRange.put(new SegmentWithRange(getSegment(5, 2), 0.0, 0.75),
                     ImmutableList.of(computeSegmentId(0, 0), computeSegmentId(4, 1)));
        // Simulate fetch successors for segment 4.
        streamSegments = streamSegments.withReplacementRange(getSegment(4, 1),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(5, 2), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(2, 0), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(3, 0), streamSegments.getSegmentForKey(0.8));
        // Simulate fetch successors for segment 0.
        streamSegments = streamSegments.withReplacementRange(getSegment(0, 0),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        assertEquals(getSegment(5, 2), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(5, 2), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(2, 0), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(3, 0), streamSegments.getSegmentForKey(0.8));
        
        newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(getSegment(4, 1), 0.25, 0.75),
                     ImmutableList.of(computeSegmentId(1, 0), computeSegmentId(2, 0)));
        // Simulate fetch successors for segment 1.
        streamSegments = streamSegments.withReplacementRange(getSegment(2, 0),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        assertEquals(getSegment(5, 2), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(5, 2), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(4, 1), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(3, 0), streamSegments.getSegmentForKey(0.8));
        
        newRange = new HashMap<>();   
        // 0 and 4 are merged into 5.
        newRange.put(new SegmentWithRange(getSegment(5, 2), 0.0, 0.75),
                     ImmutableList.of(computeSegmentId(0, 0), computeSegmentId(4, 1)));
        // Simulate fetch successors for segment 4.
        streamSegments = streamSegments.withReplacementRange(getSegment(4, 1),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        assertEquals(getSegment(5, 2), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(5, 2), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(5, 2), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(3, 0), streamSegments.getSegmentForKey(0.8));
    }
    
    
    /**
     * This is the same as {@link #testUnevenRangeReplacement()} except that 5 is observed before 4.
    Segment Map used by the test.
           ^
           |
       1.0 +---------+---------+-------->
           |         |         |
           |    2    |         |    5
           |         |    3    |
       0.66+---------|         +-------->
           |         |         |
           |    1    |---------+    6
           |         |         |
       0.33+---------|         |-------->
           |         |    4    |
           |    0    |         |    7
           |         |         |
           +-------------------+-------->
           +         1         2
     */
    @Test
    public void testOutOfOrderUnevenRangeReplacement() {
        TreeMap<Double, SegmentWithRange> segments = new TreeMap<>();
        addNewSegment(segments, 0, 0, 0.33);
        addNewSegment(segments, 1, 0.33, 0.66);
        addNewSegment(segments, 2, 0.66, 1);
        StreamSegments streamSegments = new StreamSegments(segments);

        // Verify.
        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(1, 0), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(1, 0), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(2, 0), streamSegments.getSegmentForKey(0.8));
        
        // 1 and 2 are merged into 3.
        Map<SegmentWithRange, List<Long>> newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(getSegment(3, 1), 0.5, 1.0),
                     ImmutableList.of(computeSegmentId(1, 0), computeSegmentId(2, 0)));
        // 1 and 0 are merged into 4.
        newRange.put(new SegmentWithRange(getSegment(4, 1), 0.0, 0.5),
                     ImmutableList.of(computeSegmentId(1, 0), computeSegmentId(0, 0)));

        // Simulate fetch successors for segment 1.
        streamSegments = streamSegments.withReplacementRange(getSegment(1, 0),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(4, 1), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(3, 1), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(2, 0), streamSegments.getSegmentForKey(0.8));

        // 3 is merged into 5 and 6.
        newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(getSegment(5, 2), 0.66, 1.0), ImmutableList.of(computeSegmentId(3, 1)));
        newRange.put(new SegmentWithRange(getSegment(6, 2), 0.33, 0.66),
                     ImmutableList.of(computeSegmentId(3, 1), computeSegmentId(4, 1)));
        // Simulate fetch successors for segment 3.
        streamSegments = streamSegments.withReplacementRange(getSegment(3, 1),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(4, 1), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(6, 2), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(2, 0), streamSegments.getSegmentForKey(0.8));
        
        newRange = new HashMap<>();
        // 1 and 0 are merged into 4.
        newRange.put(new SegmentWithRange(getSegment(4, 1), 0.0, 0.5),
                     ImmutableList.of(computeSegmentId(1, 0), computeSegmentId(0, 0)));

        // Simulate fetch successors for segment 0.
        streamSegments = streamSegments.withReplacementRange(getSegment(0, 0),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(4, 1), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(4, 1), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(6, 2), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(2, 0), streamSegments.getSegmentForKey(0.8));
        
        // 4 is merged into 6 and 7.
        newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(getSegment(6, 2), 0.33, 0.66),
                     ImmutableList.of(computeSegmentId(3, 1), computeSegmentId(4, 1)));
        newRange.put(new SegmentWithRange(getSegment(7, 2), 0.0, 0.33), ImmutableList.of(computeSegmentId(4, 1)));

        // Simulate fetch successors for segment 4.
        streamSegments = streamSegments.withReplacementRange(getSegment(4, 1),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(7, 2), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(6, 2), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(6, 2), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(2, 0), streamSegments.getSegmentForKey(0.8));
        
        // 1 and 2 are merged into 3.
        newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(getSegment(3, 1), 0.5, 1.0),
                     ImmutableList.of(computeSegmentId(1, 0), computeSegmentId(2, 0)));

        // Simulate fetch successor for segment 2.
        streamSegments = streamSegments.withReplacementRange(getSegment(2, 0),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(7, 2), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(6, 2), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(6, 2), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(3, 1), streamSegments.getSegmentForKey(0.8));
        
        // 3 is merged into 5 and 6.
        newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(getSegment(5, 2), 0.66, 1.0), ImmutableList.of(computeSegmentId(3, 1)));
        newRange.put(new SegmentWithRange(getSegment(6, 2), 0.33, 0.66),
                     ImmutableList.of(computeSegmentId(3, 1), computeSegmentId(4, 1)));
        // Simulate fetch successors for segment 3.
        streamSegments = streamSegments.withReplacementRange(getSegment(3, 1),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(7, 2), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(6, 2), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(6, 2), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(5, 2), streamSegments.getSegmentForKey(0.8));
    }
    
    
    
    /**
     *       ^
             |
         1.0 +---------+---------+-------->
             |         |         |
             |    2    |         |    5
             |         |    3    |
         0.66+---------|         +-------->
             |         |         |
             |    1    |         |    6
             |         |         |
         0.33+---------+---------+-------->
             |
             |    0
             |
             +-------------------+-------->
                       1         2
     */
    @Test
    public void testOutOfOrderUnevenRangeReplacementWithEvenSplit() {
        TreeMap<Double, SegmentWithRange> segments = new TreeMap<>();
        addNewSegment(segments, 0, 0, 0.33);
        addNewSegment(segments, 1, 0.33, 0.66);
        addNewSegment(segments, 2, 0.66, 1);
        StreamSegments streamSegments = new StreamSegments(segments);

        // Verify.
        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(1, 0), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(1, 0), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(2, 0), streamSegments.getSegmentForKey(0.8));

        // 1 and 2 are merged into 3.
        Map<SegmentWithRange, List<Long>> newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(getSegment(3, 1), 0.33, 1.0),
                     ImmutableList.of(computeSegmentId(1, 0), computeSegmentId(2, 0)));

        // Simulate fetch successors for segment 1.
        streamSegments = streamSegments.withReplacementRange(getSegment(1, 0),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(3, 1), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(3, 1), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(2, 0), streamSegments.getSegmentForKey(0.8));

        // 3 is split into 5 and 6.
        newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(getSegment(5, 2), 0.66, 1.0), ImmutableList.of(computeSegmentId(3, 1)));
        newRange.put(new SegmentWithRange(getSegment(6, 2), 0.33, 0.66),
                     ImmutableList.of(computeSegmentId(3, 1), computeSegmentId(4, 1)));
        // Simulate fetch successors for segment 3.
        streamSegments = streamSegments.withReplacementRange(getSegment(3, 1),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(6, 2), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(6, 2), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(2, 0), streamSegments.getSegmentForKey(0.8));
        
        // 1 and 2 are merged into 3.
        newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(getSegment(3, 1), 0.33, 1.0),
                     ImmutableList.of(computeSegmentId(1, 0), computeSegmentId(2, 0)));

        // Simulate fetch successors for segment 1.
        streamSegments = streamSegments.withReplacementRange(getSegment(2, 0),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        
        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(6, 2), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(6, 2), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(3, 1), streamSegments.getSegmentForKey(0.8));
        
        // 3 is split into 5 and 6.
        newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(getSegment(5, 2), 0.66, 1.0), ImmutableList.of(computeSegmentId(3, 1)));
        newRange.put(new SegmentWithRange(getSegment(6, 2), 0.33, 0.66),
                     ImmutableList.of(computeSegmentId(3, 1), computeSegmentId(4, 1)));
        // Simulate fetch successors for segment 3.
        streamSegments = streamSegments.withReplacementRange(getSegment(3, 1),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        
        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(6, 2), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(6, 2), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(5, 2), streamSegments.getSegmentForKey(0.8));
    }
    
    
    /**
     * Out of order re-split
             ^
             |
         1.0 +---------+---------+-------+-------->
             |         |    5    |       | 
             |         |         |       |   9
             |    1    +---------+       | 
         0.66|         |    4    |       +--------> 
             |         |         |       | 
             |---------+---------+   6   |   8
             |         |    3    |       | 
         0.33|         |         |       +--------> 
             |    0    +---------+       | 
             |         |    2    |       |   7
             |         |         |       | 
             +---------+---------+-------+-------->
                       1         2       3
        When end of segments are encountered on 0, 2, and 6 followed by 1, 4, and 6.
     */
    @Test
    public void testOutOfOrderSplit() {
        StreamSegments streamSegments = initStreamSegments(2);

        // Verify.
        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(1, 0), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(1, 0), streamSegments.getSegmentForKey(0.8));

        // 0 is split into 2, 3.
        Map<SegmentWithRange, List<Long>> newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(getSegment(2, 1), 0.0, 0.25),
                     ImmutableList.of(computeSegmentId(0, 0)));
        newRange.put(new SegmentWithRange(getSegment(3, 1), 0.25, 0.5),
                     ImmutableList.of(computeSegmentId(0, 0)));

        // Simulate fetch successors for segment 0.
        streamSegments = streamSegments.withReplacementRange(getSegment(0, 0),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(2, 1), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(3, 1), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(1, 0), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(1, 0), streamSegments.getSegmentForKey(0.8));
        
        newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(getSegment(6, 2), 0.0, 1.0),
                     ImmutableList.of(computeSegmentId(2, 1), computeSegmentId(3, 1), computeSegmentId(4, 1), computeSegmentId(5, 1)));

        // Simulate fetch successors for segment 2.
        streamSegments = streamSegments.withReplacementRange(getSegment(2, 1),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(6, 2), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(3, 1), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(1, 0), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(1, 0), streamSegments.getSegmentForKey(0.8));
        
        newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(getSegment(7, 3), 0.0, 0.33),
                     ImmutableList.of(computeSegmentId(6, 2)));
        newRange.put(new SegmentWithRange(getSegment(8, 3), 0.33, 0.66),
                     ImmutableList.of(computeSegmentId(6, 2)));
        newRange.put(new SegmentWithRange(getSegment(9, 3), 0.66, 1.0),
                     ImmutableList.of(computeSegmentId(6, 2)));
        
        // Simulate fetch successors for segment 6.
        streamSegments = streamSegments.withReplacementRange(getSegment(6, 2),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(7, 3), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(3, 1), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(1, 0), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(1, 0), streamSegments.getSegmentForKey(0.8));
        
        newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(getSegment(5, 1), 0.75, 1.0),
                     ImmutableList.of(computeSegmentId(1, 0)));
        newRange.put(new SegmentWithRange(getSegment(4, 1), 0.5, 0.75),
                     ImmutableList.of(computeSegmentId(1, 0)));
        
        // Simulate fetch successors for segment 1.
        streamSegments = streamSegments.withReplacementRange(getSegment(1, 0),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(7, 3), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(3, 1), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(4, 1), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(5, 1), streamSegments.getSegmentForKey(0.8));
        
        newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(getSegment(6, 2), 0.0, 1.0),
                     ImmutableList.of(computeSegmentId(2, 1), computeSegmentId(3, 1), computeSegmentId(4, 1), computeSegmentId(5, 1)));
        // Simulate fetch successors for segment 4.
        streamSegments = streamSegments.withReplacementRange(getSegment(4, 1),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(7, 3), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(3, 1), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(6, 2), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(5, 1), streamSegments.getSegmentForKey(0.8));
       
        newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(getSegment(7, 3), 0.0, 0.33),
                     ImmutableList.of(computeSegmentId(6, 2)));
        newRange.put(new SegmentWithRange(getSegment(8, 3), 0.33, 0.66),
                     ImmutableList.of(computeSegmentId(6, 2)));
        newRange.put(new SegmentWithRange(getSegment(9, 3), 0.66, 1.0),
                     ImmutableList.of(computeSegmentId(6, 2)));
        
        // Simulate fetch successors for segment 6.
        streamSegments = streamSegments.withReplacementRange(getSegment(6, 2),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(7, 3), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(3, 1), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(8, 3), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(5, 1), streamSegments.getSegmentForKey(0.8));
    }

    
    /**
     * Out of order re-split
             ^
             |
         1.0 +---------+---------+-------+-------->
             |         |    5    |       | 
             |         |         |       |   9
             |    1    +---------+       | 
         0.66|         |    4    |       +--------> 
             |         |         |       | 
             |---------+---------+   6   |   8
             |         |    3    |       | 
         0.33|         |         |       +--------> 
             |    0    +---------+       | 
             |         |    2    |       |   7
             |         |         |       | 
             +---------+---------+-------+-------->
                       1         2       3
        When end of segments are encountered on 1, 5, and 6 followed by 0, 3, and 6.
     */
    @Test
    public void testOutOfOrderSplit2() {
        StreamSegments streamSegments = initStreamSegments(2);
        
        // Verify.
        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(1, 0), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(1, 0), streamSegments.getSegmentForKey(0.8));

        // 1 is split into 4, 5.
        Map<SegmentWithRange, List<Long>> newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(getSegment(4, 1), 0.5, 0.75), ImmutableList.of(computeSegmentId(1, 0)));
        newRange.put(new SegmentWithRange(getSegment(5, 1), 0.75, 1.0), ImmutableList.of(computeSegmentId(1, 0)));

        // Simulate fetch successors for segment 1.
        streamSegments = streamSegments.withReplacementRange(getSegment(1, 0),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(4, 1), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(5, 1), streamSegments.getSegmentForKey(0.8));

        newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(getSegment(6, 2), 0.0, 1.0),
                     ImmutableList.of(computeSegmentId(2, 1), computeSegmentId(3, 1), computeSegmentId(4, 1),
                                      computeSegmentId(5, 1)));

        // Simulate fetch successors for segment 5.
        streamSegments = streamSegments.withReplacementRange(getSegment(5, 1),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(4, 1), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(6, 2), streamSegments.getSegmentForKey(0.8));

        newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(getSegment(7, 3), 0.0, 0.33), ImmutableList.of(computeSegmentId(6, 2)));
        newRange.put(new SegmentWithRange(getSegment(8, 3), 0.33, 0.66), ImmutableList.of(computeSegmentId(6, 2)));
        newRange.put(new SegmentWithRange(getSegment(9, 3), 0.66, 1.0), ImmutableList.of(computeSegmentId(6, 2)));

        // Simulate fetch successors for segment 6.
        streamSegments = streamSegments.withReplacementRange(getSegment(6, 2),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(0, 0), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(4, 1), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(9, 3), streamSegments.getSegmentForKey(0.8));

        newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(getSegment(2, 1), 0.0, 0.25), ImmutableList.of(computeSegmentId(0, 0)));
        newRange.put(new SegmentWithRange(getSegment(3, 1), 0.25, 0.5), ImmutableList.of(computeSegmentId(0, 0)));

        // Simulate fetch successors for segment 0.
        streamSegments = streamSegments.withReplacementRange(getSegment(0, 0),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(2, 1), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(3, 1), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(4, 1), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(9, 3), streamSegments.getSegmentForKey(0.8));

        newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(getSegment(6, 2), 0.0, 1.0),
                     ImmutableList.of(computeSegmentId(2, 1), computeSegmentId(3, 1), computeSegmentId(4, 1),
                                      computeSegmentId(5, 1)));
        // Simulate fetch successors for segment 3.
        streamSegments = streamSegments.withReplacementRange(getSegment(3, 1),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(2, 1), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(6, 2), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(4, 1), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(9, 3), streamSegments.getSegmentForKey(0.8));

        newRange = new HashMap<>();
        newRange.put(new SegmentWithRange(getSegment(7, 3), 0.0, 0.33), ImmutableList.of(computeSegmentId(6, 2)));
        newRange.put(new SegmentWithRange(getSegment(8, 3), 0.33, 0.66), ImmutableList.of(computeSegmentId(6, 2)));
        newRange.put(new SegmentWithRange(getSegment(9, 3), 0.66, 1.0), ImmutableList.of(computeSegmentId(6, 2)));

        // Simulate fetch successors for segment 6.
        streamSegments = streamSegments.withReplacementRange(getSegment(6, 2),
                                                             new StreamSegmentsWithPredecessors(newRange, ""));
        // Verify.
        assertEquals(getSegment(2, 1), streamSegments.getSegmentForKey(0.2));
        assertEquals(getSegment(8, 3), streamSegments.getSegmentForKey(0.4));
        assertEquals(getSegment(4, 1), streamSegments.getSegmentForKey(0.6));
        assertEquals(getSegment(9, 3), streamSegments.getSegmentForKey(0.8));
    }
    

    private void addNewSegment(TreeMap<Double, SegmentWithRange> segments, int number, double low, double high) {
        segments.put(high, new SegmentWithRange(new Segment(scope, streamName, number), low, high));
    }
    
    private Segment getSegment(int segmentNumber, int epoch) {
        return new Segment(scope, streamName, computeSegmentId(segmentNumber, epoch));
    }
    

    @Test
    public void testRandomSplitMerge() {
        Random r = new Random(0);
        NavigableMap<Double, SegmentWithRange> segmentMap = new TreeMap<>();
        HashMap<Segment, Range<Double>> ranges = new HashMap<>();
        segmentMap.put(1.0, new SegmentWithRange(createSegment(1, 0), 0.0, 1.0));
        ranges.put(createSegment(1, 0), Range.openClosed(0.0, 1.0));
        StreamSegments streamSegments = new StreamSegments(segmentMap);
        int segmentNumber = 10;

        for (int epoch = 1; epoch < 1000; epoch++) {
            LinkedHashMap<Segment, Integer> counts = getCounts(streamSegments);
            List<Segment> toSplit = findSegmentsToSplit(counts);

            HashMap<Segment, StreamSegmentsWithPredecessors> toApply = new HashMap<>();

            for (Segment s : toSplit) {
                if (r.nextDouble() < .2) {
                    Segment lower = createSegment(segmentNumber++, epoch);
                    Segment upper = createSegment(segmentNumber++, epoch);
                    toApply.putAll(splitSegment(ranges, s, lower, upper));
                }
            }

            Map<Segment, Segment> toMerge = findSegmentsToMerge(streamSegments, counts);
            for (Entry<Segment, Segment> pair : toMerge.entrySet()) {
                if (r.nextDouble() < .2) {
                    Segment combined = createSegment(segmentNumber++, epoch);
                    toApply.putAll(mergeSegments(ranges, pair, combined));
                }
            }

            streamSegments = applyUpdates(toApply, streamSegments, ranges, r);

            HashSet<Segment> actualSegments = new HashSet<>(streamSegments.getSegments());
            Set<Segment> expectedSegments = ranges.keySet();

            assertEquals(expectedSegments, actualSegments);
        }
    }

    private StreamSegments applyUpdates(HashMap<Segment, StreamSegmentsWithPredecessors> replacements,
                                        StreamSegments current, HashMap<Segment, Range<Double>> ranges, Random r) {
        ArrayList<Entry<Segment, StreamSegmentsWithPredecessors>> updates = new ArrayList<>(replacements.entrySet());
        Collections.shuffle(updates, r);
        for (Entry<Segment, StreamSegmentsWithPredecessors> entry : updates) {
            ranges.remove(entry.getKey());
            entry.getValue()
                 .getReplacementRanges()
                 .values()
                 .stream()
                 .flatMap(c -> c.stream())
                 .forEach(newSegment -> ranges.put(newSegment.getSegment(),
                                                   Range.openClosed(newSegment.getRange().getLow(), newSegment.getRange().getHigh())));
            current = current.withReplacementRange(entry.getKey(), entry.getValue());
        }
        return current;
    }

    private HashMap<Segment, StreamSegmentsWithPredecessors> mergeSegments(HashMap<Segment, Range<Double>> ranges,
                                                                           Entry<Segment, Segment> pair,
                                                                           Segment combined) {
        Range<Double> lowerRange = ranges.get(pair.getKey());
        Range<Double> upperRange = ranges.get(pair.getValue());
        assertEquals(lowerRange.upperEndpoint(), upperRange.lowerEndpoint());

        Map<SegmentWithRange, List<Long>> newSegments = new HashMap<>();
        newSegments.put(new SegmentWithRange(combined, lowerRange.lowerEndpoint(), upperRange.upperEndpoint()),
                        ImmutableList.of(pair.getKey().getSegmentId(), pair.getValue().getSegmentId()));
        StreamSegmentsWithPredecessors replacementRanges = new StreamSegmentsWithPredecessors(newSegments, "");
        HashMap<Segment, StreamSegmentsWithPredecessors> replacements = new HashMap<>();
        replacements.put(pair.getKey(), replacementRanges);
        replacements.put(pair.getValue(), replacementRanges);
        return replacements;
    }

    private List<Segment> findSegmentsToSplit(HashMap<Segment, Integer> counts) {
        List<Segment> toSplit = counts.entrySet()
                                      .stream()
                                      .filter(e -> e.getValue() >= 3)
                                      .map(e -> e.getKey())
                                      .collect(Collectors.toList());
        return toSplit;
    }

    private Map<Segment, Segment> findSegmentsToMerge(StreamSegments streamSegments, HashMap<Segment, Integer> counts) {
        Map<Segment, Segment> toMerge = new HashMap<>();
        Segment previous = null;
        for (Segment segment : new LinkedHashSet<>(streamSegments.getSegments())) {
            if (previous != null) {
                if (counts.get(previous) + counts.get(segment) <= 4) {
                    toMerge.put(previous, segment);
                    previous = null;
                } else {
                    previous = segment;
                }
            } else {
                previous = segment;
            }
        }
        return toMerge;
    }

    private HashMap<Segment, StreamSegmentsWithPredecessors> splitSegment(HashMap<Segment, Range<Double>> ranges,
                                                                          Segment oldSegment, Segment lower,
                                                                          Segment upper) {
        Range<Double> range = ranges.get(oldSegment);
        double midpoint = (range.upperEndpoint() + range.lowerEndpoint()) / 2.0;
        Map<SegmentWithRange, List<Long>> newSegments = new HashMap<>();
        newSegments.put(new SegmentWithRange(lower, range.lowerEndpoint(), midpoint),
                        Collections.singletonList(oldSegment.getSegmentId()));
        newSegments.put(new SegmentWithRange(upper, midpoint, range.upperEndpoint()),
                        Collections.singletonList(oldSegment.getSegmentId()));
        StreamSegmentsWithPredecessors replacementRanges = new StreamSegmentsWithPredecessors(newSegments, "");

        HashMap<Segment, StreamSegmentsWithPredecessors> replacements = new HashMap<>();
        replacements.put(oldSegment, replacementRanges);
        return replacements;
    }

    private LinkedHashMap<Segment, Integer> getCounts(StreamSegments streamSegments) {
        LinkedHashMap<Segment, Integer> counts = new LinkedHashMap<>();
        for (Segment s : streamSegments.getSegments()) {
            counts.put(s, 0);
        }
        for (double key : new double[] { 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9 }) {
            Segment segment = streamSegments.getSegmentForKey(key);
            counts.put(segment, counts.get(segment) + 1);
        }
        return counts;
    }

    private Segment createSegment(int num, int epoch) {
        return new Segment("scope", "stream", NameUtils.computeSegmentId(num, epoch));
    }
    
}

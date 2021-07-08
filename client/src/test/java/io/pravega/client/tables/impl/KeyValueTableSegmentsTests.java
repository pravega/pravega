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
package io.pravega.client.tables.impl;

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.impl.SegmentWithRange;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.TreeMap;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link KeyValueTableSegments} class.
 */
public class KeyValueTableSegmentsTests {
    private static final String SCOPE_NAME = "Scope";
    private static final String STREAM_NAME = "Stream";
    private static final int SEGMENT_COUNT = 16;

    /**
     * Verifies uniformity for {@link KeyValueTableSegments#getSegmentForKey(ByteBuffer)}.
     */
    @Test
    public void testGetSegmentForKeyByteBuf() {
        val testCount = 10000;
        val segmentsWithRange = createSegmentMap();
        val s = new KeyValueTableSegments(segmentsWithRange);
        Assert.assertEquals(segmentsWithRange.size(), s.getSegmentCount());
        val hits = new HashMap<Segment, Double>();
        val rnd = new Random(0);

        for (int i = 0; i < testCount; i++) {
            byte[] k = new byte[128];
            rnd.nextBytes(k);
            val segment = s.getSegmentForKey(ByteBuffer.wrap(k));
            hits.put(segment, hits.getOrDefault(segment, 0.0) + 1.0 / testCount);
        }

        // Since the SegmentsWithRange already contain a normalized distribution of ranges (over the interval [0,1]),
        // all we need to do is verify that the number of hits (normalized) is about the same as the range length.
        for (val e : segmentsWithRange.entrySet()) {
            double expected = e.getValue().getRange().getHigh() - e.getValue().getRange().getLow();
            double actual = hits.get(e.getValue().getSegment());
            Assert.assertEquals("Unexpected count for range " + e.getValue().getRange() + " for " + s, expected, actual, 0.01);
        }
    }

    @Test
    public void testEquals() {
        val s1 = new KeyValueTableSegments(createSegmentMap());
        val s2 = new KeyValueTableSegments(createSegmentMap());
        Assert.assertEquals(s1.hashCode(), s2.hashCode());
        Assert.assertEquals(s1, s2);
    }

    private TreeMap<Double, SegmentWithRange> createSegmentMap() {
        final int rangeIncrease = 10;
        int totalRangeLength = 0;
        val ranges = new ArrayList<Integer>();
        for (int i = 0; i < SEGMENT_COUNT; i++) {
            int rangeLength = (ranges.size() == 0 ? 0 : ranges.get(i - 1)) + rangeIncrease;
            ranges.add(rangeLength);
            totalRangeLength += rangeLength;
        }

        val result = new TreeMap<Double, SegmentWithRange>();
        int rangeLow = 0;
        for (int i = 0; i < ranges.size(); i++) {
            int rangeLength = ranges.get(i);
            int rangeHigh = rangeLow + rangeLength;

            val segment = new Segment(SCOPE_NAME, STREAM_NAME, i);
            val segmentWithRange = new SegmentWithRange(segment, (double) rangeLow / totalRangeLength, (double) rangeHigh / totalRangeLength);
            result.put(segmentWithRange.getRange().getHigh(), segmentWithRange);
            rangeLow = rangeHigh;
        }
        return result;
    }
}

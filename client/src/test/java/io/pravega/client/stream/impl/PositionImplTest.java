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

import io.pravega.client.segment.impl.Segment;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Set of tests to verify that a Position object built with a set of Segment offsets and ranges behaves exactly the same
 * as another Position object built with Segment offsets, ranges and a set of updates on the Segment offsets. This yields
 * that the set of Segment offset updates should be lazily applies before any method invocation to the Position object.
 */
@Slf4j
public class PositionImplTest {

    // Base segment offsets, before any read is performed.
    private Map<Segment, Long> ownedBaseSegmentsOffsets = getBaseSegmentOffsets(getSegments());
    // Eventual offsets for segments once few reads are performed.
    private Map<Segment, Long> ownedSegmentsEventual = getEventualSegmentOffsets(getSegments());
    private Map<Segment, SegmentWithRange.Range> segmentRanges = getSegmentRanges(getSegments());
    // Updates related to every read performed on a Segment.
    private List<Map.Entry<Segment, Long>> offsetUpdates = getSegmentOffsetUpdates(getSegments());

    @Test
    public void testPositionApplyUpdates() {
        List<Map.Entry<Segment, Long>> offsetUpdates = new ArrayList<>();
        // First position, with base segment offsets and no segment offset updates.
        PositionImpl p1 = PositionImpl.builder().segmentRanges(segmentRanges)
                                                .ownedSegments(ownedBaseSegmentsOffsets)
                                                .updatesToSegmentOffsets(offsetUpdates).build();
        // Update segment offsets to simulate a read.
        offsetUpdates.add(new AbstractMap.SimpleEntry<>(getSegments().get(0), 1L));
        // New Position object after the change in offsets. Note that we have not invoked any method to Position object,
        // so the segment updates are not applied (if any).
        PositionImpl p2 = PositionImpl.builder().segmentRanges(segmentRanges)
                                                .ownedSegments(ownedBaseSegmentsOffsets)
                                                .updatesToSegmentOffsets(offsetUpdates).build();
        // More reads.
        offsetUpdates.add(new AbstractMap.SimpleEntry<>(getSegments().get(1), 1L));
        offsetUpdates.add(new AbstractMap.SimpleEntry<>(getSegments().get(1), 2L));
        PositionImpl p3 = PositionImpl.builder().segmentRanges(segmentRanges)
                                                .ownedSegments(ownedBaseSegmentsOffsets)
                                                .updatesToSegmentOffsets(offsetUpdates).build();
        // Verify that each Position object lazily applies the updates on segment offsets and has the right state at the
        // time the event was read.
        Assert.assertEquals(p1.getOwnedSegmentsWithOffsets(), ownedBaseSegmentsOffsets);
        Map<Segment, Long> updatedSegmentsOffsets = getBaseSegmentOffsets(getSegments());
        updatedSegmentsOffsets.put(getSegments().get(0), 1L);
        Assert.assertEquals(p2.getOwnedSegmentsWithOffsets(), updatedSegmentsOffsets);
        updatedSegmentsOffsets.put(getSegments().get(1), 2L);
        Assert.assertEquals(p3.getOwnedSegmentsWithOffsets(), updatedSegmentsOffsets);
    }

    @Test
    public void testPositionEquals() {
        // Generate the same Position object in two ways: one with the eventual segment offsets after reads have been done.
        PositionImpl positionNormal = new PositionImpl(ownedSegmentsEventual, segmentRanges, null);
        // And ii) the other in a lazy fashion, meaning the base segment offsets + the list of updates to their offsets.
        PositionImpl positionLazy = new PositionImpl(ownedBaseSegmentsOffsets, segmentRanges, offsetUpdates);
        Assert.assertEquals(positionNormal, positionLazy);
        // Test equals from lazy object perspective.
        positionNormal = new PositionImpl(ownedSegmentsEventual, segmentRanges, null);
        positionLazy = new PositionImpl(ownedBaseSegmentsOffsets, segmentRanges, offsetUpdates);
        Assert.assertEquals(positionLazy, positionNormal);
    }

    @Test
    public void testPositionHashCode() {
        // Generate the same Position object in two ways: one with the eventual segment offsets after reads have been done.
        PositionImpl positionNormal = new PositionImpl(ownedSegmentsEventual, segmentRanges, null);
        // And ii) the other in a lazy fashion, meaning the base segment offsets + the list of updates to their offsets.
        PositionImpl positionLazy = new PositionImpl(ownedBaseSegmentsOffsets, segmentRanges, offsetUpdates);
        Assert.assertEquals(positionNormal.hashCode(), positionLazy.hashCode());
    }

    @Test
    public void testPositionToBytes() {
        // Generate the same Position object in two ways: one with the eventual segment offsets after reads have been done.
        PositionImpl positionNormal = new PositionImpl(ownedSegmentsEventual, segmentRanges, null);
        // And ii) the other in a lazy fashion, meaning the base segment offsets + the list of updates to their offsets.
        PositionImpl positionLazy = new PositionImpl(ownedBaseSegmentsOffsets, segmentRanges, offsetUpdates);
        Assert.assertEquals(PositionImpl.fromBytes(positionNormal.toBytes()), PositionImpl.fromBytes(positionLazy.toBytes()));
    }

    @Test
    public void testPositionToString() {
        // Generate the same Position object in two ways: one with the eventual segment offsets after reads have been done.
        PositionImpl positionNormal = new PositionImpl(ownedSegmentsEventual, segmentRanges, null);
        // And ii) the other in a lazy fashion, meaning the base segment offsets + the list of updates to their offsets.
        PositionImpl positionLazy = new PositionImpl(ownedBaseSegmentsOffsets, segmentRanges, offsetUpdates);
        Assert.assertEquals(positionNormal.toString(), positionLazy.toString());
    }

    @Test
    public void testPositionAsImpl() {
        // Generate the same Position object in two ways: one with the eventual segment offsets after reads have been done.
        PositionImpl positionNormal = new PositionImpl(ownedSegmentsEventual, segmentRanges, null);
        // And ii) the other in a lazy fashion, meaning the base segment offsets + the list of updates to their offsets.
        PositionImpl positionLazy = new PositionImpl(ownedBaseSegmentsOffsets, segmentRanges, offsetUpdates);
        Assert.assertEquals(positionNormal.asImpl(), positionLazy.asImpl());
    }

    @Test
    public void testPositionGetOwnedSegments() {
        // Generate the same Position object in two ways: one with the eventual segment offsets after reads have been done.
        PositionImpl positionNormal = new PositionImpl(ownedSegmentsEventual, segmentRanges, null);
        // And ii) the other in a lazy fashion, meaning the base segment offsets + the list of updates to their offsets.
        PositionImpl positionLazy = new PositionImpl(ownedBaseSegmentsOffsets, segmentRanges, offsetUpdates);
        Assert.assertEquals(positionNormal.getOwnedSegments(), positionLazy.getOwnedSegments());
    }

    @Test
    public void testPositionGetCompletedSegments() {
        // Generate the same Position object in two ways: one with the eventual segment offsets after reads have been done.
        PositionImpl positionNormal = new PositionImpl(ownedSegmentsEventual, segmentRanges, null);
        // And ii) the other in a lazy fashion, meaning the base segment offsets + the list of updates to their offsets.
        PositionImpl positionLazy = new PositionImpl(ownedBaseSegmentsOffsets, segmentRanges, offsetUpdates);
        Assert.assertEquals(positionNormal.getCompletedSegments(), positionLazy.getCompletedSegments());
    }

    @Test
    public void testPositionGetOwnedSegmentRangesWithOffsets() {
        // Generate the same Position object in two ways: one with the eventual segment offsets after reads have been done.
        PositionImpl positionNormal = new PositionImpl(ownedSegmentsEventual, segmentRanges, null);
        // And ii) the other in a lazy fashion, meaning the base segment offsets + the list of updates to their offsets.
        PositionImpl positionLazy = new PositionImpl(ownedBaseSegmentsOffsets, segmentRanges, offsetUpdates);
        Assert.assertEquals(positionNormal.getOwnedSegmentRangesWithOffsets(), positionLazy.getOwnedSegmentRangesWithOffsets());
    }

    @Test
    public void testPositionGetOffsetForOwnedSegment() {
        // Generate the same Position object in two ways: one with the eventual segment offsets after reads have been done.
        PositionImpl positionNormal = new PositionImpl(ownedSegmentsEventual, segmentRanges, null);
        // And ii) the other in a lazy fashion, meaning the base segment offsets + the list of updates to their offsets.
        PositionImpl positionLazy = new PositionImpl(ownedBaseSegmentsOffsets, segmentRanges, offsetUpdates);
        Assert.assertEquals(positionNormal.getOffsetForOwnedSegment(getSegments().get(0)), positionLazy.getOffsetForOwnedSegment(getSegments().get(0)));
    }

    private List<Segment> getSegments() {
        List<Segment> segments = new ArrayList<>();
        segments.add(new Segment("scope", "s0", 0));
        segments.add(new Segment("scope", "s1", 0));
        segments.add(new Segment("scope", "s2", 0));
        segments.add(new Segment("scope", "s3", 0));
        return segments;
    }

    private Map<Segment, Long> getBaseSegmentOffsets(List<Segment> segments) {
        Map<Segment, Long> ownedSegmentsBase = new HashMap<>();
        for (Segment s : segments) {
            ownedSegmentsBase.put(s, 0L);
        }
        return ownedSegmentsBase;
    }

    private Map<Segment, Long> getEventualSegmentOffsets(List<Segment> segments) {
        Map<Segment, Long> ownedSegmentsBase = new HashMap<>();
        long i = 1L;
        for (Segment s : segments) {
            ownedSegmentsBase.put(s, i);
            i++;
        }
        return ownedSegmentsBase;
    }

    private Map<Segment, SegmentWithRange.Range> getSegmentRanges(List<Segment> segments) {
        double rangeShare = 1.0 / segments.size();
        double rangeLow = 0.0;
        Map<Segment, SegmentWithRange.Range> segmentRanges = new HashMap<>();
        for (Segment s : segments) {
            segmentRanges.put(s, new SegmentWithRange.Range(rangeLow, rangeLow + rangeShare));
            rangeLow += rangeShare;
        }
        return segmentRanges;
    }

    private List<Map.Entry<Segment, Long>> getSegmentOffsetUpdates(List<Segment> segments) {
        List<Map.Entry<Segment, Long>> offsetUpdates = new ArrayList<>();
        int index = 1;
        for (Segment s : segments) {
            for (long i = 1; i <= index; i++) {
                offsetUpdates.add(new AbstractMap.SimpleEntry<>(s, i));
            }
            index++;
        }
        return offsetUpdates;
    }
}

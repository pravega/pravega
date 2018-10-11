/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream.records;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.pravega.controller.store.stream.tables.EpochTransitionRecord;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import org.junit.Test;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RecordsHelperTest {
    @Test
    public void binarySearchTest() {
        List<Long> list = Lists.newArrayList(10L, 30L, 75L, 100L, 152L, 200L, 400L, 700L);

        int index = RecordHelper.binarySearch(list, 0L, x -> x);
        assertEquals(index, 0);
        index = RecordHelper.binarySearch(list, 29L, x -> x);
        assertEquals(index, 0);
        index = RecordHelper.binarySearch(list, 101L, x -> x);
        assertEquals(index, 3);
        index = RecordHelper.binarySearch(list, Integer.MAX_VALUE, x -> x);
        assertEquals(index, 7);
    }

    @Test
    public void historyTimeIndexTest() {
        List<Long> leaves = Lists.newArrayList(10L, 30L, 75L, 100L, 152L);
        RetentionTimeIndexRootNode root = new RetentionTimeIndexRootNode(leaves);
        RetentionTimeIndexLeaf leaf0 = new RetentionTimeIndexLeaf(Lists.newArrayList(10L, 11L, 18L, 25L, 29L));
        RetentionTimeIndexLeaf leaf1 = new RetentionTimeIndexLeaf(Lists.newArrayList(30L, 32L, 35L, 45L, 71L));
        RetentionTimeIndexLeaf leaf2 = new RetentionTimeIndexLeaf(Lists.newArrayList(75L, 81L, 94L, 96L, 99L));
        RetentionTimeIndexLeaf leaf3 = new RetentionTimeIndexLeaf(Lists.newArrayList(100L, 132L, 135L, 145L, 151L));
        RetentionTimeIndexLeaf leaf4 = new RetentionTimeIndexLeaf(Lists.newArrayList(152L, 312L, 351L, 415L, 711L));
        List<RetentionTimeIndexLeaf> leavesRecords = Lists.newArrayList(leaf0, leaf1, leaf2, leaf3, leaf4);
        int leaf = root.findLeafNode(0L);
        assertEquals(leaf, 0);
        leaf = root.findLeafNode(10L);
        assertEquals(leaf, 0);
        leaf = root.findLeafNode(77L);
        assertEquals(leaf, 2);
        leaf = root.findLeafNode(166L);
        assertEquals(leaf, 4);

        leaf = root.findLeafNode(0L);
        int recordIndex = leavesRecords.get(leaf).findIndexAtTime(0L);
        assertEquals(recordIndex, 0);
        recordIndex = leavesRecords.get(leaf).findIndexAtTime(10L);
        assertEquals(recordIndex, 0);
        recordIndex = leavesRecords.get(leaf).findIndexAtTime(21L);
        assertEquals(recordIndex, 2);
        recordIndex = leavesRecords.get(leaf).findIndexAtTime(29L);
        assertEquals(recordIndex, 4);
    }

    @Test
    public void sealedSegmentShardingTest() {
        Map<Integer, SealedSegmentsMapShard> mapshards = new HashMap<>();

        int shard = SealedSegmentsMapShard.getShardNumber(StreamSegmentNameUtils.computeSegmentId(10, 10), 100);
        assertEquals(0, shard);

        Map<Long, Long> map = new HashMap<>();
        map.put(StreamSegmentNameUtils.computeSegmentId(10, 10), 100L);
        mapshards.put(shard, SealedSegmentsMapShard.builder().shardNumber(shard).sealedSegmentsSizeMap(map).build());

        shard = SealedSegmentsMapShard.getShardNumber(StreamSegmentNameUtils.computeSegmentId(10, 1000), 100);
        assertEquals(10, shard);

        map = new HashMap<>();
        map.put(StreamSegmentNameUtils.computeSegmentId(10, 1000), 100L);
        mapshards.put(shard, SealedSegmentsMapShard.builder().shardNumber(shard).sealedSegmentsSizeMap(map).build());

        long segmentId = StreamSegmentNameUtils.computeSegmentId(10000, 1000);
        shard = SealedSegmentsMapShard.getShardNumber(segmentId, 100);
        assertEquals(10, shard);

        mapshards.get(shard).addSealedSegmentSize(segmentId, 100L);
        assertEquals(100L, mapshards.get(shard).getSize(segmentId));
    }

    @Test
    public void retentionSetRecordTest() {
        RetentionSet retentionSet = new RetentionSet(Collections.emptyList());

        retentionSet = RetentionSet.addStreamCutIfLatest(retentionSet, new RetentionStreamCutRecord(0L, 0L, Collections.emptyMap()));
        assertTrue(!retentionSet.getRetentionRecords().isEmpty());

        retentionSet = RetentionSet.addStreamCutIfLatest(retentionSet, new RetentionStreamCutRecord(100L, 0L, Collections.emptyMap()));
        assertEquals(2, retentionSet.getRetentionRecords().size());
        assertEquals(100L, retentionSet.getLatest().recordingTime);
        retentionSet = RetentionSet.addStreamCutIfLatest(retentionSet, new RetentionStreamCutRecord(99L, 0L, Collections.emptyMap()));
        assertEquals(2, retentionSet.getRetentionRecords().size());
        assertEquals(100L, retentionSet.getLatest().recordingTime);

        retentionSet = RetentionSet.addStreamCutIfLatest(retentionSet, new RetentionStreamCutRecord(1000L, 0L, Collections.emptyMap()));
        retentionSet = RetentionSet.addStreamCutIfLatest(retentionSet, new RetentionStreamCutRecord(10000L, 0L, Collections.emptyMap()));
        retentionSet = RetentionSet.addStreamCutIfLatest(retentionSet, new RetentionStreamCutRecord(100000L, 0L, Collections.emptyMap()));
        assertEquals(5, retentionSet.getRetentionRecords().size());
        assertEquals(100000L, retentionSet.getLatest().recordingTime);

        List<RetentionSetRecord> before = retentionSet.retentionRecordsBefore(new RetentionSetRecord(99L, 0L));
        assertEquals(1, before.size());
        assertEquals(0L, before.get(0).recordingTime);

        before = retentionSet.retentionRecordsBefore(new RetentionSetRecord(9999L, 0L));
        assertEquals(3, before.size());
        assertEquals(1000L, before.get(2).recordingTime);

        retentionSet = RetentionSet.removeStreamCutBefore(retentionSet, new RetentionSetRecord(9999L, 0L));
        assertEquals(2, retentionSet.getRetentionRecords().size());
        assertEquals(100000L, retentionSet.getLatest().recordingTime);
    }

    @Test
    public void scaleHelperMethodTest() {
        long timestamp = System.currentTimeMillis();
        final double keyRangeChunk = 1.0 / 5;
        List<StreamSegmentRecord> list = Lists.newArrayList(new StreamSegmentRecord(0, 0, timestamp, 0.0, keyRangeChunk),
                new StreamSegmentRecord(1, 0, timestamp, keyRangeChunk, 2 * keyRangeChunk),
                new StreamSegmentRecord(2, 0, timestamp, 2 * keyRangeChunk, 3 * keyRangeChunk),
                new StreamSegmentRecord(3, 0, timestamp, 3 * keyRangeChunk, 4 * keyRangeChunk),
                new StreamSegmentRecord(4, 0, timestamp, 4 * keyRangeChunk, 1.0));
        EpochRecord epochRecord = new EpochRecord(0, 0, list, timestamp);

        assertFalse(RecordHelper.canScaleFor(Lists.newArrayList(0L, 1L, 5L), epochRecord));
        assertTrue(RecordHelper.canScaleFor(Lists.newArrayList(0L, 1L, 4L), epochRecord));

        List<AbstractMap.SimpleEntry<Double, Double>> newRanges = new ArrayList<>();
        // 1. empty newRanges
        assertFalse(RecordHelper.validateInputRange(Lists.newArrayList(0L, 1L), newRanges, epochRecord));

        // 2. simple mismatch
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, keyRangeChunk));
        assertFalse(RecordHelper.validateInputRange(Lists.newArrayList(0L, 1L), newRanges, epochRecord));

        // 3. simple valid match
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 2 * keyRangeChunk));
        ArrayList<Long> segmentsToSeal = Lists.newArrayList(0L, 1L);
        assertTrue(RecordHelper.validateInputRange(segmentsToSeal, newRanges, epochRecord));
        EpochTransitionRecord epochTransitionRecord = RecordHelper.computeEpochTransition(epochRecord,
                segmentsToSeal, newRanges, timestamp);
        assertEquals(0, epochTransitionRecord.getActiveEpoch());
        assertEquals(1, epochTransitionRecord.getNewEpoch());
        assertEquals(ImmutableSet.copyOf(segmentsToSeal), epochTransitionRecord.getSegmentsToSeal());
        assertEquals(1, epochTransitionRecord.getNewSegmentsWithRange().size());
        assertTrue(epochTransitionRecord.getNewSegmentsWithRange().containsKey(StreamSegmentNameUtils.computeSegmentId(5, 1)));
        assertEquals(newRanges.get(0), epochTransitionRecord.getNewSegmentsWithRange().get(StreamSegmentNameUtils.computeSegmentId(5, 1)));

        assertTrue(RecordHelper.verifyRecordMatchesInput(segmentsToSeal, newRanges, true, epochTransitionRecord));
        List<Long> duplicate = segmentsToSeal.stream().map(x -> StreamSegmentNameUtils.computeSegmentId(StreamSegmentNameUtils.getSegmentNumber(x), 3)).collect(Collectors.toList());
        assertFalse(RecordHelper.verifyRecordMatchesInput(duplicate, newRanges, false, epochTransitionRecord));
        assertTrue(RecordHelper.verifyRecordMatchesInput(duplicate, newRanges, true, epochTransitionRecord));

        // 4. valid 2 disjoint merges
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 2 * keyRangeChunk));
        newRanges.add(new AbstractMap.SimpleEntry<>(3 * keyRangeChunk, 1.0));
        assertTrue(RecordHelper.validateInputRange(Lists.newArrayList(0L, 1L, 3L, 4L), newRanges, epochRecord));

        // 5. valid 1 merge and 1 disjoint
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(keyRangeChunk, 2 * keyRangeChunk));
        newRanges.add(new AbstractMap.SimpleEntry<>(3 * keyRangeChunk, 1.0));
        assertTrue(RecordHelper.validateInputRange(Lists.newArrayList(1L, 3L, 4L), newRanges, epochRecord));

        // 6. valid 1 merge, 2 splits
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 2 * keyRangeChunk));
        newRanges.add(new AbstractMap.SimpleEntry<>(3 * keyRangeChunk, 0.7));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.7, 0.8));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.8, 0.9));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.9, 1.0));
        assertTrue(RecordHelper.validateInputRange(Lists.newArrayList(0L, 1L, 3L, 4L), newRanges, epochRecord));

        // 7. 1 merge, 1 split and 1 invalid split
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 2 * keyRangeChunk));
        newRanges.add(new AbstractMap.SimpleEntry<>(3 * keyRangeChunk, 0.7));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.7, 0.8));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.8, 0.9));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.9, 0.99));
        assertFalse(RecordHelper.validateInputRange(Lists.newArrayList(0L, 1L, 3L, 4L), newRanges, epochRecord));

        // 8. valid unsorted segments to seal
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 2 * keyRangeChunk));
        newRanges.add(new AbstractMap.SimpleEntry<>(3 * keyRangeChunk, 0.7));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.7, 0.8));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.8, 0.9));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.9, 1.0));
        assertTrue(RecordHelper.validateInputRange(Lists.newArrayList(4L, 0L, 1L, 3L), newRanges, epochRecord));

        // 9. valid unsorted new ranges
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.9, 1.0));
        newRanges.add(new AbstractMap.SimpleEntry<>(3 * keyRangeChunk, 0.7));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.7, 0.8));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 2 * keyRangeChunk));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.8, 0.9));
        assertTrue(RecordHelper.validateInputRange(Lists.newArrayList(4L, 0L, 1L, 3L), newRanges, epochRecord));

        // 10. invalid input range low == high
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 0.2));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.2));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.4));
        assertFalse(RecordHelper.validateInputRange(Lists.newArrayList(0L, 1L), newRanges, epochRecord));

        // 11. invalid input range low > high
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 0.2));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.3, 0.2));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.4));
        assertFalse(RecordHelper.validateInputRange(Lists.newArrayList(0L, 1L), newRanges, epochRecord));

        // 12. invalid overlapping key ranges
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.4));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.3, 3 * keyRangeChunk));
        assertFalse(RecordHelper.validateInputRange(Lists.newArrayList(1L, 2L), newRanges, epochRecord));

        // 13. invalid overlapping key ranges -- a contains b
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.4));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.3, 0.33));
        assertFalse(RecordHelper.validateInputRange(Lists.newArrayList(1L), newRanges, epochRecord));

        // 14. invalid overlapping key ranges -- b contains a (with b.low == a.low)
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.33));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.4));
        assertFalse(RecordHelper.validateInputRange(Lists.newArrayList(1L), newRanges, epochRecord));

        // 15. invalid overlapping key ranges b.low < a.high
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.35));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.3, 0.4));
        assertFalse(RecordHelper.validateInputRange(Lists.newArrayList(1L), newRanges, epochRecord));

        // 16. invalid overlapping key ranges.. a.high < b.low
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.25));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.3, 0.4));
        assertFalse(RecordHelper.validateInputRange(Lists.newArrayList(1L), newRanges, epochRecord));
    }
}

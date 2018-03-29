/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import com.google.common.collect.Lists;
import io.pravega.controller.store.stream.tables.EpochTransitionRecord;
import io.pravega.controller.store.stream.tables.HistoryRecord;
import io.pravega.controller.store.stream.tables.StreamTruncationRecord;
import io.pravega.controller.store.stream.tables.TableHelper;
import io.pravega.test.common.AssertExtensions;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TableHelperTest {
    @Test
    public void getSegmentTest() {
        long time = System.currentTimeMillis();
        int epoch = 0;
        Pair<byte[], byte[]> segmentTableAndIndex = createSegmentTableAndIndex(5, time);
        byte[] segmentTable = segmentTableAndIndex.getValue();
        byte[] segmentIndex = segmentTableAndIndex.getKey();
        assertEquals(TableHelper.getSegmentCount(segmentIndex, segmentTable), 5);

        Segment segment = TableHelper.getSegment(0, segmentIndex, segmentTable);
        assertEquals(segment.getNumber(), 0);
        assertEquals(segment.getStart(), time);
        assertEquals(segment.getKeyStart(), 0, 0);
        assertEquals(segment.getKeyEnd(), 1.0 / 5, 0);

        time = System.currentTimeMillis();
        epoch++;
        segmentTableAndIndex = updateSegmentTableAndIndex(segmentIndex, segmentTable, 5, epoch, time);
        segmentTable = segmentTableAndIndex.getValue();
        segmentIndex = segmentTableAndIndex.getKey();
        assertEquals(TableHelper.getSegmentCount(segmentIndex, segmentTable), 10);

        segment = TableHelper.getSegment(9, segmentIndex, segmentTable);
        assertEquals(segment.getNumber(), 9);
        assertEquals(segment.getStart(), time);
        assertEquals(segment.getKeyStart(), 1.0 / 5 * 4, 0);
        assertEquals(segment.getKeyEnd(), 1.0, 0);

        // Test with updated index but stale segment table
        time = System.currentTimeMillis();
        segmentTableAndIndex = updateSegmentTableAndIndex(segmentIndex, segmentTable, 5, 2, time);
        final byte[] segmentIndex2 = segmentTableAndIndex.getKey();

        final byte[] segmentTablecopy = segmentTable;
        AssertExtensions.assertThrows(StoreException.class, () -> TableHelper.getSegment(10, segmentIndex2, segmentTablecopy));
        assertEquals(10, TableHelper.getSegmentCount(segmentIndex2, segmentTable));

        segmentTableAndIndex = updateSegmentTableAndIndex(segmentIndex2, segmentTable, 5, 2, time);
        byte[] segmentTable3 = segmentTableAndIndex.getValue();
        byte[] segmentIndex3 = segmentTableAndIndex.getKey();

        segment = TableHelper.getSegment(10, segmentIndex3, segmentTable3);
        assertEquals(segment.getNumber(), 10);
        assertEquals(15, TableHelper.getSegmentCount(segmentIndex3, segmentTable3));
    }

    @Test
    public void getActiveSegmentsTest() {
        final List<Integer> startSegments = Lists.newArrayList(0, 1, 2, 3, 4);
        long timestamp = 1;
        byte[] historyTable = TableHelper.createHistoryTable(timestamp, startSegments);
        byte[] historyIndex = TableHelper.createHistoryIndex(timestamp);
        List<Integer> activeSegments = TableHelper.getActiveSegments(historyIndex, historyTable);
        assertEquals(activeSegments, startSegments);

        List<Integer> newSegments = Lists.newArrayList(5, 6, 7, 8, 9);
        timestamp = timestamp + 5;
        historyIndex = TableHelper.updateHistoryIndex(historyIndex, historyTable.length);
        historyTable = TableHelper.addPartialRecordToHistoryTable(historyIndex, historyTable, newSegments);
        activeSegments = TableHelper.getActiveSegments(historyIndex, historyTable);
        assertEquals(activeSegments, startSegments);

        int epoch = TableHelper.getActiveEpoch(historyIndex, historyTable).getKey();
        assertEquals(0, epoch);
        epoch = TableHelper.getLatestEpoch(historyIndex, historyTable).getEpoch();
        assertEquals(1, epoch);

        timestamp = timestamp + 5;
        HistoryRecord partial = HistoryRecord.readLatestRecord(historyIndex, historyTable, false).get();
        historyTable = TableHelper.completePartialRecordInHistoryTable(historyIndex, historyTable, partial, timestamp);

        activeSegments = TableHelper.getActiveSegments(historyIndex, historyTable);
        assertEquals(activeSegments, newSegments);

        activeSegments = TableHelper.getActiveSegments(0, historyIndex, historyTable, null, null, null);
        assertEquals(startSegments, activeSegments);

        activeSegments = TableHelper.getActiveSegments(timestamp - 1, historyIndex, historyTable, null, null, null);
        assertEquals(startSegments, activeSegments);

        activeSegments = TableHelper.getActiveSegments(timestamp + 1, historyIndex, historyTable, null, null, null);
        assertEquals(newSegments, activeSegments);
    }

    private Segment getSegment(int number, List<Segment> segments) {
        return segments.stream().filter(x -> x.getNumber() == number).findAny().get();
    }

    @Test
    public void predecessorAndSuccessorTest() {
        // multiple rows in history table, find predecessor
        // - more than one predecessor
        // - one predecessor
        // - no predecessor
        // - immediate predecessor
        // - predecessor few rows behind
        List<Segment> segments = new ArrayList<>();
        List<Integer> newSegments = Lists.newArrayList(0, 1, 2, 3, 4);
        long timestamp = System.currentTimeMillis();
        int epoch = 0;
        Segment zero = new Segment(0, epoch, timestamp, 0, 0.2);
        segments.add(zero);
        Segment one = new Segment(1, epoch, timestamp, 0.2, 0.4);
        segments.add(one);
        Segment two = new Segment(2, epoch, timestamp, 0.4, 0.6);
        segments.add(two);
        Segment three = new Segment(3, epoch, timestamp, 0.6, 0.8);
        segments.add(three);
        Segment four = new Segment(4, epoch, timestamp, 0.8, 1);
        segments.add(four);

        List<Integer> predecessors, successors;

        // find predecessors and successors when update to history and index table hasnt happened
        predecessors = TableHelper.getOverlaps(zero,
                TableHelper
                        .findSegmentPredecessorCandidates(zero,
                                new byte[0],
                                new byte[0])
                        .stream()
                        .map(x -> getSegment(x, segments))
                        .collect(Collectors.toList()));
        successors = TableHelper.getOverlaps(zero,
                TableHelper.findSegmentSuccessorCandidates(zero,
                        new byte[0],
                        new byte[0])
                        .stream()
                        .map(x -> getSegment(x, segments))
                        .collect(Collectors.toList()));

        assertEquals(predecessors, new ArrayList<Integer>());
        assertEquals(successors, new ArrayList<Integer>());

        byte[] historyTable = TableHelper.createHistoryTable(timestamp, newSegments);
        byte[] historyIndex = TableHelper.createHistoryIndex(timestamp);

        int nextHistoryOffset = historyTable.length;

        // 3, 4 -> 5
        epoch++;
        newSegments = Lists.newArrayList(0, 1, 2, 5);
        timestamp = timestamp + 1;
        Segment five = new Segment(5, epoch, timestamp, 0.6, 1);
        segments.add(five);

        historyIndex = TableHelper.updateHistoryIndex(historyIndex, nextHistoryOffset);
        historyTable = TableHelper.addPartialRecordToHistoryTable(historyIndex, historyTable, newSegments);

        // check predecessor segment in partial record
        predecessors = TableHelper.getOverlaps(five,
                TableHelper.findSegmentPredecessorCandidates(five,
                        historyIndex,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments))
                        .collect(Collectors.toList()));
        // check that segment from partial record is returned as successor
        successors = TableHelper.getOverlaps(three,
                TableHelper.findSegmentSuccessorCandidates(three,
                        historyIndex,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments))
                        .collect(Collectors.toList()));
        assertEquals(predecessors, Lists.newArrayList(3, 4));
        assertEquals(successors, Lists.newArrayList(5));

        HistoryRecord partial = HistoryRecord.readLatestRecord(historyIndex, historyTable, false).get();
        historyTable = TableHelper.completePartialRecordInHistoryTable(historyIndex, historyTable, partial, timestamp);

        // 1 -> 6,7.. 2,5 -> 8
        nextHistoryOffset = historyTable.length;
        epoch++;
        newSegments = Lists.newArrayList(0, 6, 7, 8);
        timestamp = timestamp + 10;
        Segment six = new Segment(6, epoch, timestamp, 0.2, 0.3);
        segments.add(six);
        Segment seven = new Segment(7, epoch, timestamp, 0.3, 0.4);
        segments.add(seven);
        Segment eight = new Segment(8, epoch, timestamp, 0.4, 1);
        segments.add(eight);

        historyIndex = TableHelper.updateHistoryIndex(historyIndex, nextHistoryOffset);
        historyTable = TableHelper.addPartialRecordToHistoryTable(historyIndex, historyTable, newSegments);

        // check that previous partial record is not a regular record and its successor and predecessors are returned successfully
        predecessors = TableHelper.getOverlaps(five,
                TableHelper.findSegmentPredecessorCandidates(five,
                        historyIndex,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments))
                        .collect(Collectors.toList()));
        successors = TableHelper.getOverlaps(five,
                TableHelper.findSegmentSuccessorCandidates(five,
                        historyIndex,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments))
                        .collect(Collectors.toList()));
        assertEquals(predecessors, Lists.newArrayList(3, 4));
        assertEquals(successors, Lists.newArrayList(8));

        partial = HistoryRecord.readLatestRecord(historyIndex, historyTable, false).get();
        timestamp = timestamp + 5;
        historyTable = TableHelper.completePartialRecordInHistoryTable(historyIndex, historyTable, partial, timestamp);

        // 7 -> 9,10.. 8 -> 10, 11
        nextHistoryOffset = historyTable.length;
        epoch++;
        newSegments = Lists.newArrayList(0, 6, 9, 10, 11);
        timestamp = timestamp + 10;
        Segment nine = new Segment(9, epoch, timestamp, 0.3, 0.35);
        segments.add(nine);
        Segment ten = new Segment(10, epoch, timestamp, 0.35, 0.6);
        segments.add(ten);
        Segment eleven = new Segment(11, epoch, timestamp, 0.6, 1);
        segments.add(eleven);

        historyIndex = TableHelper.updateHistoryIndex(historyIndex, nextHistoryOffset);
        historyTable = TableHelper.addPartialRecordToHistoryTable(historyIndex, historyTable, newSegments);
        partial = HistoryRecord.readLatestRecord(historyIndex, historyTable, false).get();
        timestamp = timestamp + 5;
        historyTable = TableHelper.completePartialRecordInHistoryTable(historyIndex, historyTable, partial, timestamp);

        // find predecessor and successor with index table being stale
        predecessors = TableHelper.getOverlaps(ten,
                TableHelper.findSegmentPredecessorCandidates(ten,
                        historyIndex,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments))
                        .collect(Collectors.toList()));
        successors = TableHelper.getOverlaps(seven,
                TableHelper.findSegmentSuccessorCandidates(seven,
                        historyIndex,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments))
                        .collect(Collectors.toList()));

        assertEquals(predecessors, Lists.newArrayList(7, 8));
        assertEquals(successors, Lists.newArrayList(9, 10));

        // 0 has no successor and no predecessor
        // 10 has multiple predecessor
        // 1 has a successor few rows down

        predecessors = TableHelper.getOverlaps(zero,
                TableHelper.findSegmentPredecessorCandidates(zero,
                        historyIndex,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments))
                        .collect(Collectors.toList()));
        successors = TableHelper.getOverlaps(zero,
                TableHelper.findSegmentSuccessorCandidates(zero,
                        historyIndex,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments))
                        .collect(Collectors.toList()));

        assertEquals(predecessors, new ArrayList<Integer>());
        assertEquals(successors, new ArrayList<Integer>());

        predecessors = TableHelper.getOverlaps(one,
                TableHelper.findSegmentPredecessorCandidates(one,
                        historyIndex,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments))
                        .collect(Collectors.toList()));
        successors = TableHelper.getOverlaps(one,
                TableHelper.findSegmentSuccessorCandidates(one,
                        historyIndex,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments))
                        .collect(Collectors.toList()));
        assertEquals(predecessors, new ArrayList<Integer>());
        assertEquals(successors, Lists.newArrayList(6, 7));

        predecessors = TableHelper.getOverlaps(two,
                TableHelper.findSegmentPredecessorCandidates(two,
                        historyIndex,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        successors = TableHelper.getOverlaps(two,
                TableHelper.findSegmentSuccessorCandidates(two,
                        historyIndex,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        assertEquals(predecessors, new ArrayList<Integer>());
        assertEquals(successors, Lists.newArrayList(8));

        predecessors = TableHelper.getOverlaps(three,
                TableHelper.findSegmentPredecessorCandidates(three,
                        historyIndex,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        successors = TableHelper.getOverlaps(three,
                TableHelper.findSegmentSuccessorCandidates(three,
                        historyIndex,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        assertEquals(predecessors, new ArrayList<Integer>());
        assertEquals(successors, Lists.newArrayList(5));

        predecessors = TableHelper.getOverlaps(four,
                TableHelper.findSegmentPredecessorCandidates(four,
                        historyIndex,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        successors = TableHelper.getOverlaps(four,
                TableHelper.findSegmentSuccessorCandidates(four,
                        historyIndex,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        assertEquals(predecessors, new ArrayList<Integer>());
        assertEquals(successors, Lists.newArrayList(5));

        predecessors = TableHelper.getOverlaps(five,
                TableHelper.findSegmentPredecessorCandidates(five,
                        historyIndex,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        successors = TableHelper.getOverlaps(five,
                TableHelper.findSegmentSuccessorCandidates(five,
                        historyIndex,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        assertEquals(predecessors, Lists.newArrayList(3, 4));
        assertEquals(successors, Lists.newArrayList(8));

        predecessors = TableHelper.getOverlaps(six,
                TableHelper.findSegmentPredecessorCandidates(six,
                        historyIndex,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        successors = TableHelper.getOverlaps(six,
                TableHelper.findSegmentSuccessorCandidates(six,
                        historyIndex,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        assertEquals(predecessors, Lists.newArrayList(1));
        assertEquals(successors, new ArrayList<>());

        predecessors = TableHelper.getOverlaps(seven,
                TableHelper.findSegmentPredecessorCandidates(seven,
                        historyIndex,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        successors = TableHelper.getOverlaps(seven,
                TableHelper.findSegmentSuccessorCandidates(seven,
                        historyIndex,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        assertEquals(predecessors, Lists.newArrayList(1));
        assertEquals(successors, Lists.newArrayList(9, 10));

        predecessors = TableHelper.getOverlaps(eight,
                TableHelper.findSegmentPredecessorCandidates(eight,
                        historyIndex,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        successors = TableHelper.getOverlaps(eight,
                TableHelper.findSegmentSuccessorCandidates(eight,
                        historyIndex,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        assertEquals(predecessors, Lists.newArrayList(2, 5));
        assertEquals(successors, Lists.newArrayList(10, 11));

        predecessors = TableHelper.getOverlaps(nine,
                TableHelper.findSegmentPredecessorCandidates(nine,
                        historyIndex,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        successors = TableHelper.getOverlaps(nine,
                TableHelper.findSegmentSuccessorCandidates(nine,
                        historyIndex,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        assertEquals(predecessors, Lists.newArrayList(7));
        assertEquals(successors, new ArrayList<>());

        predecessors = TableHelper.getOverlaps(ten,
                TableHelper.findSegmentPredecessorCandidates(ten,
                        historyIndex,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        successors = TableHelper.getOverlaps(ten,
                TableHelper.findSegmentSuccessorCandidates(ten,
                        historyIndex,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        assertEquals(predecessors, Lists.newArrayList(7, 8));
        assertEquals(successors, new ArrayList<>());

        predecessors = TableHelper.getOverlaps(eleven,
                TableHelper.findSegmentPredecessorCandidates(eleven,
                        historyIndex,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        successors = TableHelper.getOverlaps(eleven,
                TableHelper.findSegmentSuccessorCandidates(eleven,
                        historyIndex,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        assertEquals(predecessors, Lists.newArrayList(8));
        assertEquals(successors, new ArrayList<>());
    }

    @Test
    public void epochTransitionConsistencyTest() {
        long timestamp = System.currentTimeMillis();
        final List<Integer> startSegments = Lists.newArrayList(0, 1, 2, 3, 4);
        int epoch = 0;
        Pair<byte[], byte[]> segmentTableAndIndex = createSegmentTableAndIndex(5, timestamp);
        byte[] segmentIndex = segmentTableAndIndex.getKey();
        byte[] segmentTable = segmentTableAndIndex.getValue();
        byte[] historyTable = TableHelper.createHistoryTable(timestamp, startSegments);
        byte[] historyIndex = TableHelper.createHistoryIndex(timestamp);

        // start new scale
        List<Integer> newSegments = Lists.newArrayList(5, 6, 7, 8, 9);
        final double keyRangeChunk = 1.0 / 5;
        final List<AbstractMap.SimpleEntry<Double, Double>> newRanges = IntStream.range(0, 5)
                .boxed()
                .map(x -> new AbstractMap.SimpleEntry<>(x * keyRangeChunk, (x + 1) * keyRangeChunk))
                .collect(Collectors.toList());
        EpochTransitionRecord consistentEpochTransitionRecord = TableHelper.computeEpochTransition(historyIndex, historyTable,
                segmentIndex, segmentTable, Lists.newArrayList(0, 1, 2, 3, 4), newRanges, timestamp + 1);

        final double keyRangeChunkInconsistent = 1.0 / 2;
        final List<AbstractMap.SimpleEntry<Double, Double>> newRangesInconsistent = IntStream.range(0, 2)
                .boxed()
                .map(x -> new AbstractMap.SimpleEntry<>(x * keyRangeChunkInconsistent, (x + 1) * keyRangeChunkInconsistent))
                .collect(Collectors.toList());

        EpochTransitionRecord inconsistentEpochTransitionRecord = TableHelper.computeEpochTransition(historyIndex, historyTable,
                segmentIndex, segmentTable, Lists.newArrayList(0, 1, 2, 3, 4), newRangesInconsistent, timestamp + 1);

        // before updating segment table, both records should be consistent.
        assertTrue(TableHelper.isEpochTransitionConsistent(consistentEpochTransitionRecord, historyIndex, historyTable,
                segmentIndex, segmentTable));
        assertTrue(TableHelper.isEpochTransitionConsistent(inconsistentEpochTransitionRecord, historyIndex, historyTable,
                segmentIndex, segmentTable));

        // update segment table corresponding to consistent epoch transition record
        epoch++;
        segmentTableAndIndex = updateSegmentTableAndIndex(5, epoch, segmentIndex, segmentTable, newRanges, timestamp + 1);
        // update index
        segmentIndex = segmentTableAndIndex.getKey();
        assertTrue(TableHelper.isEpochTransitionConsistent(consistentEpochTransitionRecord, historyIndex, historyTable,
                segmentIndex, segmentTable));
        assertTrue(TableHelper.isEpochTransitionConsistent(inconsistentEpochTransitionRecord, historyIndex, historyTable,
                segmentIndex, segmentTable));

        // update segment table
        segmentTable = segmentTableAndIndex.getValue();

        // now only consistentEpochTransitionRecord should return true as only its new range should match the state in
        // segment table
        assertTrue(TableHelper.isEpochTransitionConsistent(consistentEpochTransitionRecord, historyIndex, historyTable,
                segmentIndex, segmentTable));
        assertFalse(TableHelper.isEpochTransitionConsistent(inconsistentEpochTransitionRecord, historyIndex, historyTable,
                segmentIndex, segmentTable));

        // update history index
        historyIndex = TableHelper.updateHistoryIndex(historyIndex, historyTable.length);
        assertTrue(TableHelper.isEpochTransitionConsistent(consistentEpochTransitionRecord, historyIndex, historyTable,
                segmentIndex, segmentTable));
        assertFalse(TableHelper.isEpochTransitionConsistent(inconsistentEpochTransitionRecord, historyIndex, historyTable,
                segmentIndex, segmentTable));

        // update history table
        historyTable = TableHelper.addPartialRecordToHistoryTable(historyIndex, historyTable, newSegments);
        // nothing should change the consistency even with history table update
        assertTrue(TableHelper.isEpochTransitionConsistent(consistentEpochTransitionRecord, historyIndex, historyTable,
                segmentIndex, segmentTable));
        assertFalse(TableHelper.isEpochTransitionConsistent(inconsistentEpochTransitionRecord, historyIndex, historyTable,
                segmentIndex, segmentTable));

        // complete history record
        HistoryRecord partial = HistoryRecord.readLatestRecord(historyIndex, historyTable, false).get();
        historyTable = TableHelper.completePartialRecordInHistoryTable(historyIndex, historyTable, partial, timestamp + 2);
        // nothing should change the consistency even with history table update
        assertTrue(TableHelper.isEpochTransitionConsistent(consistentEpochTransitionRecord, historyIndex, historyTable,
                segmentIndex, segmentTable));
        assertFalse(TableHelper.isEpochTransitionConsistent(inconsistentEpochTransitionRecord, historyIndex, historyTable,
                segmentIndex, segmentTable));
    }

    @Test
    public void scaleInputValidityTest() {
        long timestamp = System.currentTimeMillis();

        Pair<byte[], byte[]> segmentTableAndIndex = createSegmentTableAndIndex(5, timestamp);
        byte[] segmentTable = segmentTableAndIndex.getValue();
        byte[] segmentIndex = segmentTableAndIndex.getKey();
        final double keyRangeChunk = 1.0 / 5;

        List<AbstractMap.SimpleEntry<Double, Double>> newRanges = new ArrayList<>();
        // 1. empty newRanges
        assertFalse(TableHelper.isScaleInputValid(Lists.newArrayList(0, 1), newRanges, segmentIndex, segmentTable));

        // 2. simple mismatch
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, keyRangeChunk));
        assertFalse(TableHelper.isScaleInputValid(Lists.newArrayList(0, 1), newRanges, segmentIndex, segmentTable));

        // 3. simple valid match
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 2 * keyRangeChunk));
        assertTrue(TableHelper.isScaleInputValid(Lists.newArrayList(0, 1), newRanges, segmentIndex, segmentTable));

        // 4. valid 2 disjoint merges
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 2 * keyRangeChunk));
        newRanges.add(new AbstractMap.SimpleEntry<>(3 * keyRangeChunk, 1.0));
        assertTrue(TableHelper.isScaleInputValid(Lists.newArrayList(0, 1, 3, 4), newRanges, segmentIndex, segmentTable));

        // 5. valid 1 merge and 1 disjoint
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(keyRangeChunk, 2 * keyRangeChunk));
        newRanges.add(new AbstractMap.SimpleEntry<>(3 * keyRangeChunk, 1.0));
        assertTrue(TableHelper.isScaleInputValid(Lists.newArrayList(1, 3, 4), newRanges, segmentIndex, segmentTable));

        // 6. valid 1 merge, 2 splits
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 2 * keyRangeChunk));
        newRanges.add(new AbstractMap.SimpleEntry<>(3 * keyRangeChunk, 0.7));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.7, 0.8));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.8, 0.9));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.9, 1.0));
        assertTrue(TableHelper.isScaleInputValid(Lists.newArrayList(0, 1, 3, 4), newRanges, segmentIndex, segmentTable));

        // 7. 1 merge, 1 split and 1 invalid split
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 2 * keyRangeChunk));
        newRanges.add(new AbstractMap.SimpleEntry<>(3 * keyRangeChunk, 0.7));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.7, 0.8));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.8, 0.9));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.9, 0.99));
        assertFalse(TableHelper.isScaleInputValid(Lists.newArrayList(0, 1, 3, 4), newRanges, segmentIndex, segmentTable));

        // 8. valid unsorted segments to seal
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 2 * keyRangeChunk));
        newRanges.add(new AbstractMap.SimpleEntry<>(3 * keyRangeChunk, 0.7));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.7, 0.8));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.8, 0.9));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.9, 1.0));
        assertTrue(TableHelper.isScaleInputValid(Lists.newArrayList(4, 0, 1, 3), newRanges, segmentIndex, segmentTable));

        // 9. valid unsorted new ranges
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.9, 1.0));
        newRanges.add(new AbstractMap.SimpleEntry<>(3 * keyRangeChunk, 0.7));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.7, 0.8));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 2 * keyRangeChunk));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.8, 0.9));
        assertTrue(TableHelper.isScaleInputValid(Lists.newArrayList(4, 0, 1, 3), newRanges, segmentIndex, segmentTable));

        // 10. invalid input range low == high
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 0.2));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.2));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.4));
        assertFalse(TableHelper.isScaleInputValid(Lists.newArrayList(0, 1), newRanges, segmentIndex, segmentTable));

        // 11. invalid input range low > high
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 0.2));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.3, 0.2));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.4));
        assertFalse(TableHelper.isScaleInputValid(Lists.newArrayList(0, 1), newRanges, segmentIndex, segmentTable));

        // 12. invalid overlapping key ranges
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.4));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.3, 3 * keyRangeChunk));
        assertFalse(TableHelper.isScaleInputValid(Lists.newArrayList(1, 2), newRanges, segmentIndex, segmentTable));

        // 13. invalid overlapping key ranges -- a contains b
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.4));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.3, 0.33));
        assertFalse(TableHelper.isScaleInputValid(Lists.newArrayList(1), newRanges, segmentIndex, segmentTable));

        // 14. invalid overlapping key ranges -- b contains a (with b.low == a.low)
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.33));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.4));
        assertFalse(TableHelper.isScaleInputValid(Lists.newArrayList(1), newRanges, segmentIndex, segmentTable));

        // 15. invalid overlapping key ranges b.low < a.high
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.35));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.3, 0.4));
        assertFalse(TableHelper.isScaleInputValid(Lists.newArrayList(1), newRanges, segmentIndex, segmentTable));

        // 16. invalid overlapping key ranges.. a.high < b.low
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.25));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.3, 0.4));
        assertFalse(TableHelper.isScaleInputValid(Lists.newArrayList(1), newRanges, segmentIndex, segmentTable));
    }

    @Test
    public void truncationTest() {
        final List<Integer> startSegments = Lists.newArrayList(0, 1);
        int epoch = 0;
        // epoch 0
        long timestamp = System.currentTimeMillis();
        Pair<byte[], byte[]> segmentTableAndIndex = createSegmentTableAndIndex(2, timestamp);
        byte[] segmentTable = segmentTableAndIndex.getValue();
        byte[] segmentIndex = segmentTableAndIndex.getKey();
        byte[] historyTable = TableHelper.createHistoryTable(timestamp, startSegments);
        byte[] historyIndex = TableHelper.createHistoryIndex(timestamp);

        List<Integer> activeSegments = TableHelper.getActiveSegments(historyIndex, historyTable);
        assertEquals(activeSegments, startSegments);

        // epoch 1
        epoch++;
        List<Integer> newSegments1 = Lists.newArrayList(0, 2, 3);
        List<AbstractMap.SimpleEntry<Double, Double>> newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<Double, Double>(0.5, 0.75));
        newRanges.add(new AbstractMap.SimpleEntry<Double, Double>(0.75, 1.0));

        segmentTableAndIndex = updateSegmentTableAndIndex(2, epoch, segmentIndex, segmentTable, newRanges, timestamp + 1);
        segmentIndex = segmentTableAndIndex.getKey();
        segmentTable = segmentTableAndIndex.getValue();
        historyIndex = TableHelper.updateHistoryIndex(historyIndex, historyTable.length);
        historyTable = TableHelper.addPartialRecordToHistoryTable(historyIndex, historyTable, newSegments1);
        HistoryRecord partial = HistoryRecord.readLatestRecord(historyIndex, historyTable, false).get();
        historyTable = TableHelper.completePartialRecordInHistoryTable(historyIndex, historyTable, partial, timestamp + 1);

        // epoch 2
        epoch++;
        List<Integer> newSegments2 = Lists.newArrayList(0, 2, 4, 5);
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<Double, Double>(0.75, (0.75 + 1.0) / 2));
        newRanges.add(new AbstractMap.SimpleEntry<Double, Double>((0.75 + 1.0) / 2, 1.0));

        segmentTableAndIndex = updateSegmentTableAndIndex(4, epoch, segmentIndex, segmentTable, newRanges, timestamp + 2);
        segmentIndex = segmentTableAndIndex.getKey();
        segmentTable = segmentTableAndIndex.getValue();
        historyIndex = TableHelper.updateHistoryIndex(historyIndex, historyTable.length);
        historyTable = TableHelper.addPartialRecordToHistoryTable(historyIndex, historyTable, newSegments2);
        partial = HistoryRecord.readLatestRecord(historyIndex, historyTable, false).get();
        historyTable = TableHelper.completePartialRecordInHistoryTable(historyIndex, historyTable, partial, timestamp + 2);

        // epoch 3
        epoch++;
        List<Integer> newSegments3 = Lists.newArrayList(0, 4, 5, 6, 7);
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<Double, Double>(0.5, (0.75 + 0.5) / 2));
        newRanges.add(new AbstractMap.SimpleEntry<Double, Double>((0.75 + 0.5) / 2, 0.75));

        segmentTableAndIndex = updateSegmentTableAndIndex(6, epoch, segmentIndex, segmentTable, newRanges, timestamp + 3);
        segmentIndex = segmentTableAndIndex.getKey();
        segmentTable = segmentTableAndIndex.getValue();
        historyIndex = TableHelper.updateHistoryIndex(historyIndex, historyTable.length);
        historyTable = TableHelper.addPartialRecordToHistoryTable(historyIndex, historyTable, newSegments3);
        partial = HistoryRecord.readLatestRecord(historyIndex, historyTable, false).get();
        historyTable = TableHelper.completePartialRecordInHistoryTable(historyIndex, historyTable, partial, timestamp + 3);

        // epoch 4
        epoch++;
        List<Integer> newSegments4 = Lists.newArrayList(4, 5, 6, 7, 8, 9);
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<Double, Double>(0.0, (0.0 + 0.5) / 2));
        newRanges.add(new AbstractMap.SimpleEntry<Double, Double>((0.0 + 0.5) / 2, 0.5));

        segmentTableAndIndex = updateSegmentTableAndIndex(8, epoch, segmentIndex, segmentTable, newRanges, timestamp + 4);
        segmentIndex = segmentTableAndIndex.getKey();
        segmentTable = segmentTableAndIndex.getValue();
        historyIndex = TableHelper.updateHistoryIndex(historyIndex, historyTable.length);
        historyTable = TableHelper.addPartialRecordToHistoryTable(historyIndex, historyTable, newSegments4);
        partial = HistoryRecord.readLatestRecord(historyIndex, historyTable, false).get();
        historyTable = TableHelper.completePartialRecordInHistoryTable(historyIndex, historyTable, partial, timestamp + 4);

        // happy day
        Map<Integer, Long> streamCut1 = new HashMap<>();
        streamCut1.put(0, 1L);
        streamCut1.put(1, 1L);
        StreamTruncationRecord truncationRecord = TableHelper.computeTruncationRecord(historyIndex, historyTable, segmentIndex,
                segmentTable, streamCut1, StreamTruncationRecord.EMPTY);

        assertTrue(truncationRecord.getToDelete().isEmpty());
        assertTrue(truncationRecord.getStreamCut().equals(streamCut1));
        assertTrue(truncationRecord.getCutEpochMap().get(0) == 0 &&
                truncationRecord.getCutEpochMap().get(1) == 0);
        truncationRecord = truncationRecord.mergeDeleted();

        Map<Integer, Long> streamCut2 = new HashMap<>();
        streamCut2.put(0, 1L);
        streamCut2.put(2, 1L);
        streamCut2.put(4, 1L);
        streamCut2.put(5, 1L);
        truncationRecord = TableHelper.computeTruncationRecord(historyIndex, historyTable, segmentIndex, segmentTable,
                streamCut2, truncationRecord);
        assertTrue(truncationRecord.getToDelete().size() == 2
                && truncationRecord.getToDelete().contains(1)
                && truncationRecord.getToDelete().contains(3));
        assertTrue(truncationRecord.getStreamCut().equals(streamCut2));
        assertTrue(truncationRecord.getCutEpochMap().get(0) == 2 &&
                truncationRecord.getCutEpochMap().get(2) == 2 &&
                truncationRecord.getCutEpochMap().get(4) == 2 &&
                truncationRecord.getCutEpochMap().get(5) == 2);
        truncationRecord = truncationRecord.mergeDeleted();

        Map<Integer, Long> streamCut3 = new HashMap<>();
        streamCut3.put(2, 10L);
        streamCut3.put(4, 10L);
        streamCut3.put(5, 10L);
        streamCut3.put(8, 10L);
        streamCut3.put(9, 10L);
        truncationRecord = TableHelper.computeTruncationRecord(historyIndex, historyTable, segmentIndex, segmentTable, streamCut3, truncationRecord);
        assertTrue(truncationRecord.getToDelete().size() == 1
                && truncationRecord.getToDelete().contains(0));
        assertTrue(truncationRecord.getStreamCut().equals(streamCut3));
        assertTrue(truncationRecord.getCutEpochMap().get(2) == 2 &&
                truncationRecord.getCutEpochMap().get(4) == 4 &&
                truncationRecord.getCutEpochMap().get(5) == 4 &&
                truncationRecord.getCutEpochMap().get(8) == 4 &&
                truncationRecord.getCutEpochMap().get(9) == 4);
        truncationRecord = truncationRecord.mergeDeleted();

        // behind previous
        Map<Integer, Long> streamCut4 = new HashMap<>();
        streamCut4.put(2, 1L);
        streamCut4.put(4, 1L);
        streamCut4.put(5, 1L);
        streamCut4.put(8, 1L);
        streamCut4.put(9, 1L);
        byte[] finalIndexTable = historyIndex;
        byte[] finalHistoryTable = historyTable;
        byte[] finalSegmentIndex = segmentIndex;
        byte[] finalSegmentTable = segmentTable;
        StreamTruncationRecord finalTruncationRecord = truncationRecord;
        AssertExtensions.assertThrows("",
                () -> TableHelper.computeTruncationRecord(finalIndexTable, finalHistoryTable, finalSegmentIndex, finalSegmentTable,
                        streamCut4, finalTruncationRecord), e -> e instanceof IllegalArgumentException);

        Map<Integer, Long> streamCut5 = new HashMap<>();
        streamCut3.put(2, 10L);
        streamCut3.put(4, 10L);
        streamCut3.put(5, 10L);
        streamCut3.put(0, 10L);
        AssertExtensions.assertThrows("",
                () -> TableHelper.computeTruncationRecord(finalIndexTable, finalHistoryTable, finalSegmentIndex, finalSegmentTable,
                        streamCut5, finalTruncationRecord), e -> e instanceof IllegalArgumentException);
    }

    private Pair<byte[], byte[]> createSegmentTableAndIndex(int numSegments, long eventTime) {
        final double keyRangeChunk = 1.0 / numSegments;

        List<AbstractMap.SimpleEntry<Double, Double>> newRanges = IntStream.range(0, numSegments)
                .boxed()
                .map(x -> new AbstractMap.SimpleEntry<>(x * keyRangeChunk, (x + 1) * keyRangeChunk))
                .collect(Collectors.toList());

        return TableHelper.createSegmentTableAndIndex(newRanges, eventTime);
    }

    private Pair<byte[], byte[]> updateSegmentTableAndIndex(byte[] segmentIndex, byte[] segmentTable, int numSegments,
                                                            int newEpoch, long eventTime) {
        final double keyRangeChunk = 1.0 / numSegments;
        List<AbstractMap.SimpleEntry<Double, Double>> newRanges = IntStream.range(0, numSegments)
                .boxed()
                .map(x -> new AbstractMap.SimpleEntry<>(x * keyRangeChunk, (x + 1) * keyRangeChunk))
                .collect(Collectors.toList());

        int startingSegNum = TableHelper.getSegmentCount(segmentIndex, segmentTable);
        return updateSegmentTableAndIndex(startingSegNum, newEpoch, segmentIndex, segmentTable, newRanges, eventTime);
    }

    private Pair<byte[], byte[]> updateSegmentTableAndIndex(int startingSegNum, int newEpoch, byte[] segmentIndex,
                                                            byte[] segmentTable, List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                                            long eventTime) {

        return TableHelper.addNewSegmentsToSegmentTableAndIndex(startingSegNum, newEpoch, segmentIndex, segmentTable,
                newRanges, eventTime);
    }

}


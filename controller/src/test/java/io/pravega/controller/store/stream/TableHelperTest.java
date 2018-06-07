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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.pravega.shared.segment.StreamSegmentNameUtils.computeSegmentId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TableHelperTest {
    @Test
    public void getSegmentTest() {
        long time = System.currentTimeMillis();
        int epoch = 0;
        final List<Long> startSegments = Lists.newArrayList(0L, 1L, 2L, 3L, 4L);
        Pair<byte[], byte[]> segmentTableAndIndex = createSegmentTableAndIndex(5, time);
        byte[] segmentTable = segmentTableAndIndex.getValue();
        byte[] segmentIndex = segmentTableAndIndex.getKey();
        assertEquals(TableHelper.getSegmentCount(segmentIndex, segmentTable), 5);
        byte[] historyTable = TableHelper.createHistoryTable(time, startSegments);
        byte[] historyIndex = TableHelper.createHistoryIndex();

        Segment segment = TableHelper.getSegment(0L, segmentIndex, segmentTable, historyIndex, historyTable);
        assertEquals(segment.getSegmentId(), 0L);
        assertEquals(segment.getStart(), time);
        assertEquals(segment.getKeyStart(), 0, 0);
        assertEquals(segment.getKeyEnd(), 1.0 / 5, 0);

        time = System.currentTimeMillis();
        epoch++;
        segmentTableAndIndex = updateSegmentTableAndIndex(segmentIndex, segmentTable, 5, epoch, time);
        segmentTable = segmentTableAndIndex.getValue();
        segmentIndex = segmentTableAndIndex.getKey();
        assertEquals(TableHelper.getSegmentCount(segmentIndex, segmentTable), 10);

        List<Long> newSegments = Lists.newArrayList(computeSegmentId(5, 1),
                computeSegmentId(6, 1), computeSegmentId(7, 1),
                computeSegmentId(8, 1), computeSegmentId(9, 1));
        historyIndex = TableHelper.updateHistoryIndex(historyIndex, historyTable.length);
        historyTable = TableHelper.addPartialRecordToHistoryTable(historyIndex, historyTable, newSegments);
        HistoryRecord partial = HistoryRecord.readLatestRecord(historyIndex, historyTable, false).get();
        historyTable = TableHelper.completePartialRecordInHistoryTable(historyIndex, historyTable, partial, time);

        segment = TableHelper.getSegment(computeSegmentId(9, 1), segmentIndex, segmentTable, historyIndex, historyTable);
        assertEquals(computeSegmentId(9, 1), segment.getSegmentId());
        assertEquals(segment.getStart(), time);
        assertEquals(segment.getKeyStart(), 1.0 / 5 * 4, 0);
        assertEquals(segment.getKeyEnd(), 1.0, 0);

        // Test with updated index but stale segment table
        time = System.currentTimeMillis();
        segmentTableAndIndex = updateSegmentTableAndIndex(segmentIndex, segmentTable, 5, 2, time);
        final byte[] segmentIndex2 = segmentTableAndIndex.getKey();
        final byte[] segmentTablecopy = segmentTable;
        final byte[] historyIndexCopy = historyIndex;
        final byte[] historyTablecopy = historyTable;

        AssertExtensions.assertThrows(StoreException.class, () -> TableHelper.getSegment(computeSegmentId(10, 1),
                segmentIndex2, segmentTablecopy, historyIndexCopy, historyTablecopy));
        assertEquals(10, TableHelper.getSegmentCount(segmentIndex2, segmentTable));

        segmentTableAndIndex = updateSegmentTableAndIndex(segmentIndex2, segmentTable, 5, 2, time);
        byte[] segmentTable3 = segmentTableAndIndex.getValue();
        byte[] segmentIndex3 = segmentTableAndIndex.getKey();

        newSegments = Lists.newArrayList(computeSegmentId(11, 2),
                computeSegmentId(12, 2), computeSegmentId(13, 2),
                computeSegmentId(14, 2), computeSegmentId(15, 2));
        historyIndex = TableHelper.updateHistoryIndex(historyIndex, historyTable.length);
        historyTable = TableHelper.addPartialRecordToHistoryTable(historyIndex, historyTable, newSegments);
        partial = HistoryRecord.readLatestRecord(historyIndex, historyTable, false).get();
        historyTable = TableHelper.completePartialRecordInHistoryTable(historyIndex, historyTable, partial, time);

        segment = TableHelper.getSegment(computeSegmentId(10, 2), segmentIndex3, segmentTable3, historyIndex, historyTable);
        assertEquals(segment.getSegmentId(), computeSegmentId(10, 2));
        assertEquals(15, TableHelper.getSegmentCount(segmentIndex3, segmentTable3));
    }

    @Test
    public void getActiveSegmentsTest() {
        final List<Long> startSegments = Lists.newArrayList(0L, 1L, 2L, 3L, 4L);
        long timestamp = 1;
        byte[] historyTable = TableHelper.createHistoryTable(timestamp, startSegments);
        byte[] historyIndex = TableHelper.createHistoryIndex();
        List<Long> activeSegments = TableHelper.getActiveSegments(historyIndex, historyTable);
        assertEquals(activeSegments, startSegments);

        List<Long> newSegments = Lists.newArrayList(computeSegmentId(5, 1),
                computeSegmentId(6, 1), computeSegmentId(7, 1),
                computeSegmentId(8, 1), computeSegmentId(9, 1));
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

    private Segment getSegment(long number, List<Segment> segments) {
        return segments.stream().filter(x -> x.getSegmentId() == number).findAny().get();
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
        List<Long> newSegments = Lists.newArrayList(0L, 1L, 2L, 3L, 4L);
        long timestamp = System.currentTimeMillis();
        int epoch = 0;
        Segment zero = new Segment(0L, epoch, timestamp, 0, 0.2);
        segments.add(zero);
        Segment one = new Segment(1L, epoch, timestamp, 0.2, 0.4);
        segments.add(one);
        Segment two = new Segment(2L, epoch, timestamp, 0.4, 0.6);
        segments.add(two);
        Segment three = new Segment(3L, epoch, timestamp, 0.6, 0.8);
        segments.add(three);
        Segment four = new Segment(4L, epoch, timestamp, 0.8, 1);
        segments.add(four);

        List<Long> predecessors, successors;

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

        assertEquals(predecessors, new ArrayList<Long>());
        assertEquals(successors, new ArrayList<Long>());

        byte[] historyTable = TableHelper.createHistoryTable(timestamp, newSegments);
        byte[] historyIndex = TableHelper.createHistoryIndex();

        int nextHistoryOffset = historyTable.length;

        // 3, 4 -> 5
        epoch++;
        long fiveSegmentId = computeSegmentId(5, 1);
        newSegments = Lists.newArrayList(0L, 1L, 2L, fiveSegmentId);
        timestamp = timestamp + 1;
        Segment five = new Segment(fiveSegmentId, epoch, timestamp, 0.6, 1);
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
        assertEquals(predecessors, Lists.newArrayList(3L, 4L));
        assertEquals(successors, Lists.newArrayList(fiveSegmentId));

        HistoryRecord partial = HistoryRecord.readLatestRecord(historyIndex, historyTable, false).get();
        historyTable = TableHelper.completePartialRecordInHistoryTable(historyIndex, historyTable, partial, timestamp);

        // 1 -> 6,7.. 2,5 -> 8
        nextHistoryOffset = historyTable.length;
        epoch++;
        long sixSegmentId = computeSegmentId(6, 2);
        long sevenSegmentId = computeSegmentId(7, 2);
        long eightSegmentId = computeSegmentId(8, 2);

        newSegments = Lists.newArrayList(0L, sixSegmentId, sevenSegmentId, eightSegmentId);
        timestamp = timestamp + 10;
        Segment six = new Segment(sixSegmentId, epoch, timestamp, 0.2, 0.3);
        segments.add(six);
        Segment seven = new Segment(sevenSegmentId, epoch, timestamp, 0.3, 0.4);
        segments.add(seven);
        Segment eight = new Segment(eightSegmentId, epoch, timestamp, 0.4, 1);
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
        assertEquals(predecessors, Lists.newArrayList(3L, 4L));
        assertEquals(successors, Lists.newArrayList(eightSegmentId));

        partial = HistoryRecord.readLatestRecord(historyIndex, historyTable, false).get();
        timestamp = timestamp + 5;
        historyTable = TableHelper.completePartialRecordInHistoryTable(historyIndex, historyTable, partial, timestamp);

        // 7 -> 9,10.. 8 -> 10, 11
        nextHistoryOffset = historyTable.length;
        epoch++;
        long nineSegmentId = computeSegmentId(9, 3);
        long tenSegmentId = computeSegmentId(10, 3);
        long elevenSegmentId = computeSegmentId(11, 3);

        newSegments = Lists.newArrayList(0L, sixSegmentId, nineSegmentId, tenSegmentId, elevenSegmentId);
        timestamp = timestamp + 10;
        Segment nine = new Segment(nineSegmentId, epoch, timestamp, 0.3, 0.35);
        segments.add(nine);
        Segment ten = new Segment(tenSegmentId, epoch, timestamp, 0.35, 0.6);
        segments.add(ten);
        Segment eleven = new Segment(elevenSegmentId, epoch, timestamp, 0.6, 1);
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

        assertEquals(predecessors, Lists.newArrayList(sevenSegmentId, eightSegmentId));
        assertEquals(successors, Lists.newArrayList(nineSegmentId, tenSegmentId));

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

        assertEquals(predecessors, new ArrayList<Long>());
        assertEquals(successors, new ArrayList<Long>());

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
        assertEquals(predecessors, new ArrayList<Long>());
        assertEquals(successors, Lists.newArrayList(sixSegmentId, sevenSegmentId));

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
        assertEquals(predecessors, new ArrayList<Long>());
        assertEquals(successors, Lists.newArrayList(eightSegmentId));

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
        assertEquals(predecessors, new ArrayList<Long>());
        assertEquals(successors, Lists.newArrayList(fiveSegmentId));

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
        assertEquals(predecessors, new ArrayList<Long>());
        assertEquals(successors, Lists.newArrayList(fiveSegmentId));

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
        assertEquals(predecessors, Lists.newArrayList(3L, 4L));
        assertEquals(successors, Lists.newArrayList(eightSegmentId));

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
        assertEquals(predecessors, Lists.newArrayList(1L));
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
        assertEquals(predecessors, Lists.newArrayList(1L));
        assertEquals(successors, Lists.newArrayList(nineSegmentId, tenSegmentId));

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
        assertEquals(predecessors, Lists.newArrayList(2L, fiveSegmentId));
        assertEquals(successors, Lists.newArrayList(tenSegmentId, elevenSegmentId));

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
        assertEquals(predecessors, Lists.newArrayList(sevenSegmentId));
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
        assertEquals(predecessors, Lists.newArrayList(sevenSegmentId, eightSegmentId));
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
        assertEquals(predecessors, Lists.newArrayList(eightSegmentId));
        assertEquals(successors, new ArrayList<>());
    }

    @Test
    public void epochTransitionConsistencyTest() {
        long timestamp = System.currentTimeMillis();
        final List<Long> startSegments = Lists.newArrayList(0L, 1L, 2L, 3L, 4L);
        int epoch = 0;
        Pair<byte[], byte[]> segmentTableAndIndex = createSegmentTableAndIndex(5, timestamp);
        byte[] segmentIndex = segmentTableAndIndex.getKey();
        byte[] segmentTable = segmentTableAndIndex.getValue();
        byte[] historyTable = TableHelper.createHistoryTable(timestamp, startSegments);
        byte[] historyIndex = TableHelper.createHistoryIndex();

        // start new scale
        List<Long> newSegments = Lists.newArrayList(computeSegmentId(5, 1),
                computeSegmentId(6, 1), computeSegmentId(7, 1),
                computeSegmentId(8, 1), computeSegmentId(9, 1));
        final double keyRangeChunk = 1.0 / 5;
        final List<AbstractMap.SimpleEntry<Double, Double>> newRanges = IntStream.range(0, 5)
                .boxed()
                .map(x -> new AbstractMap.SimpleEntry<>(x * keyRangeChunk, (x + 1) * keyRangeChunk))
                .collect(Collectors.toList());
        EpochTransitionRecord consistentEpochTransitionRecord = TableHelper.computeEpochTransition(historyIndex, historyTable,
                segmentIndex, segmentTable, Lists.newArrayList(0L, 1L, 2L, 3L, 4L), newRanges, timestamp + 1);

        final double keyRangeChunkInconsistent = 1.0 / 2;
        final List<AbstractMap.SimpleEntry<Double, Double>> newRangesInconsistent = IntStream.range(0, 2)
                .boxed()
                .map(x -> new AbstractMap.SimpleEntry<>(x * keyRangeChunkInconsistent, (x + 1) * keyRangeChunkInconsistent))
                .collect(Collectors.toList());

        EpochTransitionRecord inconsistentEpochTransitionRecord = TableHelper.computeEpochTransition(historyIndex, historyTable,
                segmentIndex, segmentTable, Lists.newArrayList(0L, 1L, 2L, 3L, 4L), newRangesInconsistent, timestamp + 1);

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
        assertFalse(TableHelper.isScaleInputValid(Lists.newArrayList(0L, 1L), newRanges, segmentIndex, segmentTable));

        // 2. simple mismatch
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, keyRangeChunk));
        assertFalse(TableHelper.isScaleInputValid(Lists.newArrayList(0L, 1L), newRanges, segmentIndex, segmentTable));

        // 3. simple valid match
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 2 * keyRangeChunk));
        assertTrue(TableHelper.isScaleInputValid(Lists.newArrayList(0L, 1L), newRanges, segmentIndex, segmentTable));

        // 4. valid 2 disjoint merges
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 2 * keyRangeChunk));
        newRanges.add(new AbstractMap.SimpleEntry<>(3 * keyRangeChunk, 1.0));
        assertTrue(TableHelper.isScaleInputValid(Lists.newArrayList(0L, 1L, 3L, 4L), newRanges, segmentIndex, segmentTable));

        // 5. valid 1 merge and 1 disjoint
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(keyRangeChunk, 2 * keyRangeChunk));
        newRanges.add(new AbstractMap.SimpleEntry<>(3 * keyRangeChunk, 1.0));
        assertTrue(TableHelper.isScaleInputValid(Lists.newArrayList(1L, 3L, 4L), newRanges, segmentIndex, segmentTable));

        // 6. valid 1 merge, 2 splits
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 2 * keyRangeChunk));
        newRanges.add(new AbstractMap.SimpleEntry<>(3 * keyRangeChunk, 0.7));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.7, 0.8));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.8, 0.9));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.9, 1.0));
        assertTrue(TableHelper.isScaleInputValid(Lists.newArrayList(0L, 1L, 3L, 4L), newRanges, segmentIndex, segmentTable));

        // 7. 1 merge, 1 split and 1 invalid split
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 2 * keyRangeChunk));
        newRanges.add(new AbstractMap.SimpleEntry<>(3 * keyRangeChunk, 0.7));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.7, 0.8));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.8, 0.9));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.9, 0.99));
        assertFalse(TableHelper.isScaleInputValid(Lists.newArrayList(0L, 1L, 3L, 4L), newRanges, segmentIndex, segmentTable));

        // 8. valid unsorted segments to seal
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 2 * keyRangeChunk));
        newRanges.add(new AbstractMap.SimpleEntry<>(3 * keyRangeChunk, 0.7));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.7, 0.8));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.8, 0.9));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.9, 1.0));
        assertTrue(TableHelper.isScaleInputValid(Lists.newArrayList(4L, 0L, 1L, 3L), newRanges, segmentIndex, segmentTable));

        // 9. valid unsorted new ranges
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.9, 1.0));
        newRanges.add(new AbstractMap.SimpleEntry<>(3 * keyRangeChunk, 0.7));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.7, 0.8));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 2 * keyRangeChunk));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.8, 0.9));
        assertTrue(TableHelper.isScaleInputValid(Lists.newArrayList(4L, 0L, 1L, 3L), newRanges, segmentIndex, segmentTable));

        // 10. invalid input range low == high
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 0.2));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.2));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.4));
        assertFalse(TableHelper.isScaleInputValid(Lists.newArrayList(0L, 1L), newRanges, segmentIndex, segmentTable));

        // 11. invalid input range low > high
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 0.2));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.3, 0.2));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.4));
        assertFalse(TableHelper.isScaleInputValid(Lists.newArrayList(0L, 1L), newRanges, segmentIndex, segmentTable));

        // 12. invalid overlapping key ranges
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.4));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.3, 3 * keyRangeChunk));
        assertFalse(TableHelper.isScaleInputValid(Lists.newArrayList(1L, 2L), newRanges, segmentIndex, segmentTable));

        // 13. invalid overlapping key ranges -- a contains b
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.4));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.3, 0.33));
        assertFalse(TableHelper.isScaleInputValid(Lists.newArrayList(1L), newRanges, segmentIndex, segmentTable));

        // 14. invalid overlapping key ranges -- b contains a (with b.low == a.low)
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.33));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.4));
        assertFalse(TableHelper.isScaleInputValid(Lists.newArrayList(1L), newRanges, segmentIndex, segmentTable));

        // 15. invalid overlapping key ranges b.low < a.high
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.35));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.3, 0.4));
        assertFalse(TableHelper.isScaleInputValid(Lists.newArrayList(1L), newRanges, segmentIndex, segmentTable));

        // 16. invalid overlapping key ranges.. a.high < b.low
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.25));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.3, 0.4));
        assertFalse(TableHelper.isScaleInputValid(Lists.newArrayList(1L), newRanges, segmentIndex, segmentTable));
    }

    @Test
    public void truncationTest() {
        final List<Long> startSegments = Lists.newArrayList(0L, 1L);
        int epoch = 0;
        // epoch 0
        long timestamp = System.currentTimeMillis();
        Pair<byte[], byte[]> segmentTableAndIndex = createSegmentTableAndIndex(2, timestamp);
        byte[] segmentTable = segmentTableAndIndex.getValue();
        byte[] segmentIndex = segmentTableAndIndex.getKey();
        byte[] historyTable = TableHelper.createHistoryTable(timestamp, startSegments);
        byte[] historyIndex = TableHelper.createHistoryIndex();

        List<Long> activeSegments = TableHelper.getActiveSegments(historyIndex, historyTable);
        assertEquals(activeSegments, startSegments);

        // epoch 1
        epoch++;
        long twoSegmentId = computeSegmentId(2, 1);
        long threeSegmentId = computeSegmentId(3, 1);

        List<Long> newSegments1 = Lists.newArrayList(0L, twoSegmentId, threeSegmentId);
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
        long fourSegmentId = computeSegmentId(4, 2);
        long fiveSegmentId = computeSegmentId(5, 2);

        List<Long> newSegments2 = Lists.newArrayList(0L, twoSegmentId, fourSegmentId, fiveSegmentId);
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
        long sixSegmentId = computeSegmentId(6, 3);
        long sevenSegmentId = computeSegmentId(7, 3);

        List<Long> newSegments3 = Lists.newArrayList(0L, fourSegmentId, fiveSegmentId, sixSegmentId, sevenSegmentId);
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
        long eightSegmentId = computeSegmentId(8, 4);
        long nineSegmentId = computeSegmentId(9, 4);

        List<Long> newSegments4 = Lists.newArrayList(fourSegmentId, fiveSegmentId, sixSegmentId, sevenSegmentId, eightSegmentId, nineSegmentId);
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
        Map<Long, Long> streamCut1 = new HashMap<>();
        streamCut1.put(0L, 1L);
        streamCut1.put(1L, 1L);
        StreamTruncationRecord truncationRecord = TableHelper.computeTruncationRecord(historyIndex, historyTable, segmentIndex,
                segmentTable, streamCut1, StreamTruncationRecord.EMPTY);

        assertTrue(truncationRecord.getToDelete().isEmpty());
        assertTrue(truncationRecord.getStreamCut().equals(streamCut1));
        assertTrue(truncationRecord.getCutEpochMap().get(0L) == 0 &&
                truncationRecord.getCutEpochMap().get(1L) == 0);
        truncationRecord = StreamTruncationRecord.complete(truncationRecord);

        Map<Long, Long> streamCut2 = new HashMap<>();
        streamCut2.put(0L, 1L);
        streamCut2.put(twoSegmentId, 1L);
        streamCut2.put(fourSegmentId, 1L);
        streamCut2.put(fiveSegmentId, 1L);
        truncationRecord = TableHelper.computeTruncationRecord(historyIndex, historyTable, segmentIndex, segmentTable,
                streamCut2, truncationRecord);
        assertTrue(truncationRecord.getToDelete().size() == 2
                && truncationRecord.getToDelete().contains(1L)
                && truncationRecord.getToDelete().contains(threeSegmentId));
        assertTrue(truncationRecord.getStreamCut().equals(streamCut2));
        assertTrue(truncationRecord.getCutEpochMap().get(0L) == 2 &&
                truncationRecord.getCutEpochMap().get(twoSegmentId) == 2 &&
                truncationRecord.getCutEpochMap().get(fourSegmentId) == 2 &&
                truncationRecord.getCutEpochMap().get(fiveSegmentId) == 2);
        truncationRecord = StreamTruncationRecord.complete(truncationRecord);

        Map<Long, Long> streamCut3 = new HashMap<>();
        streamCut3.put(twoSegmentId, 10L);
        streamCut3.put(fourSegmentId, 10L);
        streamCut3.put(fiveSegmentId, 10L);
        streamCut3.put(eightSegmentId, 10L);
        streamCut3.put(nineSegmentId, 10L);
        truncationRecord = TableHelper.computeTruncationRecord(historyIndex, historyTable, segmentIndex, segmentTable, streamCut3, truncationRecord);
        assertTrue(truncationRecord.getToDelete().size() == 1
                && truncationRecord.getToDelete().contains(0L));
        assertTrue(truncationRecord.getStreamCut().equals(streamCut3));
        assertTrue(truncationRecord.getCutEpochMap().get(twoSegmentId) == 2 &&
                truncationRecord.getCutEpochMap().get(fourSegmentId) == 4 &&
                truncationRecord.getCutEpochMap().get(fiveSegmentId) == 4 &&
                truncationRecord.getCutEpochMap().get(eightSegmentId) == 4 &&
                truncationRecord.getCutEpochMap().get(nineSegmentId) == 4);
        truncationRecord = StreamTruncationRecord.complete(truncationRecord);

        // behind previous
        Map<Long, Long> streamCut4 = new HashMap<>();
        streamCut4.put(twoSegmentId, 1L);
        streamCut4.put(fourSegmentId, 1L);
        streamCut4.put(fiveSegmentId, 1L);
        streamCut4.put(eightSegmentId, 1L);
        streamCut4.put(nineSegmentId, 1L);
        byte[] finalIndexTable = historyIndex;
        byte[] finalHistoryTable = historyTable;
        byte[] finalSegmentIndex = segmentIndex;
        byte[] finalSegmentTable = segmentTable;
        StreamTruncationRecord finalTruncationRecord = truncationRecord;
        AssertExtensions.assertThrows("",
                () -> TableHelper.computeTruncationRecord(finalIndexTable, finalHistoryTable, finalSegmentIndex, finalSegmentTable,
                        streamCut4, finalTruncationRecord), e -> e instanceof IllegalArgumentException);

        Map<Long, Long> streamCut5 = new HashMap<>();
        streamCut3.put(twoSegmentId, 10L);
        streamCut3.put(fourSegmentId, 10L);
        streamCut3.put(fiveSegmentId, 10L);
        streamCut3.put(0L, 10L);
        AssertExtensions.assertThrows("",
                () -> TableHelper.computeTruncationRecord(finalIndexTable, finalHistoryTable, finalSegmentIndex, finalSegmentTable,
                        streamCut5, finalTruncationRecord), e -> e instanceof IllegalArgumentException);
    }

    // region stream cut test
    /*
     Segment mapping of stream8 used for the below tests.

     +-------+------+-------+-------+
     |       |   8  |       |       |
     |   2   +------|       |       |
     |       |   7  |   10  |       |
     +-------+ -----|       |       |
     |       |   6  |       |       |
     |  1    +------+-------+-------+
     |       |   5  |       |       |
     +-------+------|       |       |
     |       |   4  |   9   |       |
     |  0    +------|       |       |
     |       |   3  |       |       |
     +-------+------+----------------
     */
    private List<byte[]> setupTablesForStreamCut() {
        List<Segment> segments;
        List<Long> newSegments;
        List<AbstractMap.SimpleEntry<Double, Double>> newRanges;
        long timestamp = System.currentTimeMillis();
        int epoch = 0;

        long threeId = computeSegmentId(3, 1);
        long fourId = computeSegmentId(4, 1);
        long fiveId = computeSegmentId(5, 1);
        long sixId = computeSegmentId(6, 1);
        long sevenId = computeSegmentId(7, 1);
        long eightId = computeSegmentId(8, 1);
        long nineId = computeSegmentId(9, 2);
        long tenId = computeSegmentId(10, 2);
        Segment zero = new Segment(0L, epoch, timestamp, 0, 0.33);
        Segment one = new Segment(1L, epoch, timestamp, 0.33, 0.66);
        Segment two = new Segment(2L, epoch, timestamp, 0.66, 1.0);
        Segment three = new Segment(threeId, epoch, timestamp, 0.0, 0.16);
        Segment four = new Segment(fourId, epoch, timestamp, 0.16, 0.33);
        Segment five = new Segment(fiveId, epoch, timestamp, 0.33, 0.5);
        Segment six = new Segment(sixId, epoch, timestamp, 0.5, 0.66);
        Segment seven = new Segment(sevenId, epoch, timestamp, 0.66, 0.83);
        Segment eight = new Segment(eightId, epoch, timestamp, 0.83, 1);
        Segment nine = new Segment(nineId, epoch, timestamp, 0.0, 0.5);
        Segment ten = new Segment(tenId, epoch, timestamp, 0.5, 1);

        segments = new LinkedList<>();
        segments.add(zero);
        segments.add(one);
        segments.add(two);
        newRanges = segments.stream()
                .map(x -> new AbstractMap.SimpleEntry<>(x.getKeyStart(), x.getKeyEnd())).collect(Collectors.toList());
        newSegments = segments.stream().map(x -> x.getSegmentId()).collect(Collectors.toList());

        Pair<byte[], byte[]> segmentAndIndex = TableHelper.createSegmentTableAndIndex(newRanges, timestamp);
        byte[] segmentIndex = segmentAndIndex.getKey();
        byte[] segmentTable = segmentAndIndex.getValue();
        byte[] historyIndex = TableHelper.createHistoryIndex();
        byte[] historyTable = TableHelper.createHistoryTable(timestamp, newSegments);

        int nextHistoryOffset = historyTable.length;

        // 2 -> 7, 8
        // 1 -> 5, 6
        // 0 -> 3, 4
        segments = new LinkedList<>();
        segments.add(three);
        segments.add(four);
        segments.add(five);
        segments.add(six);
        segments.add(seven);
        segments.add(eight);
        newRanges = segments.stream()
                .map(x -> new AbstractMap.SimpleEntry<>(x.getKeyStart(), x.getKeyEnd())).collect(Collectors.toList());
        newSegments = segments.stream().map(x -> x.getSegmentId()).collect(Collectors.toList());

        epoch++;
        timestamp = timestamp + 1;

        segmentAndIndex = TableHelper.addNewSegmentsToSegmentTableAndIndex(3, epoch, segmentIndex, segmentTable, newRanges, timestamp);
        segmentIndex = segmentAndIndex.getKey();
        segmentTable = segmentAndIndex.getValue();

        historyIndex = TableHelper.updateHistoryIndex(historyIndex, nextHistoryOffset);
        historyTable = TableHelper.addPartialRecordToHistoryTable(historyIndex, historyTable, newSegments);
        HistoryRecord partial = HistoryRecord.readLatestRecord(historyIndex, historyTable, false).get();
        historyTable = TableHelper.completePartialRecordInHistoryTable(historyIndex, historyTable, partial, timestamp);

        // 6, 7, 8 -> 10
        // 3, 4, 5 -> 9
        segments = new LinkedList<>();
        segments.add(nine);
        segments.add(ten);
        newRanges = segments.stream()
                .map(x -> new AbstractMap.SimpleEntry<>(x.getKeyStart(), x.getKeyEnd())).collect(Collectors.toList());
        newSegments = segments.stream().map(x -> x.getSegmentId()).collect(Collectors.toList());

        epoch++;
        timestamp = timestamp + 1;
        nextHistoryOffset = historyTable.length;

        segmentAndIndex = TableHelper.addNewSegmentsToSegmentTableAndIndex(9, epoch, segmentIndex, segmentTable, newRanges, timestamp);
        segmentIndex = segmentAndIndex.getKey();
        segmentTable = segmentAndIndex.getValue();

        historyIndex = TableHelper.updateHistoryIndex(historyIndex, nextHistoryOffset);
        historyTable = TableHelper.addPartialRecordToHistoryTable(historyIndex, historyTable, newSegments);
        partial = HistoryRecord.readLatestRecord(historyIndex, historyTable, false).get();
        historyTable = TableHelper.completePartialRecordInHistoryTable(historyIndex, historyTable, partial, timestamp);

        return Lists.newArrayList(segmentIndex, segmentTable, historyIndex, historyTable);
    }

    @Test
    public void testCutpointSuccessors() {
        long zero = computeSegmentId(0, 0);
        long one = computeSegmentId(1, 0);
        long two = computeSegmentId(2, 0);
        long three = computeSegmentId(3, 1);
        long four = computeSegmentId(4, 1);
        long five = computeSegmentId(5, 1);
        long six = computeSegmentId(6, 1);
        long seven = computeSegmentId(7, 1);
        long eight = computeSegmentId(8, 1);
        long nine = computeSegmentId(9, 2);
        long ten = computeSegmentId(10, 2);
        List<byte[]> list = setupTablesForStreamCut();
        byte[] segmentIndex = list.get(0);
        byte[] segmentTable = list.get(1);
        byte[] historyIndex = list.get(2);
        byte[] historyTable = list.get(3);

        Map<Long, Long> fromStreamCut = new HashMap<>();
        fromStreamCut.put(zero, 0L);
        fromStreamCut.put(one, 0L);
        fromStreamCut.put(two, 0L);

        List<Segment> segments = TableHelper.findSegmentsBetweenStreamCuts(historyIndex, historyTable, segmentIndex, segmentTable, fromStreamCut, Collections.emptyMap());
        assertEquals(11, segments.size());

        fromStreamCut = new HashMap<>();
        fromStreamCut.put(zero, 0L);
        fromStreamCut.put(two, 0L);
        fromStreamCut.put(five, 0L);
        fromStreamCut.put(six, 0L);
        segments = TableHelper.findSegmentsBetweenStreamCuts(historyIndex, historyTable, segmentIndex, segmentTable, fromStreamCut, Collections.emptyMap());
        assertEquals(10, segments.size());
        assertTrue(segments.stream().noneMatch(x -> x.getSegmentId() == one));

        fromStreamCut = new HashMap<>();
        fromStreamCut.put(zero, 0L);
        fromStreamCut.put(five, 0L);
        fromStreamCut.put(ten, 0L);
        segments = TableHelper.findSegmentsBetweenStreamCuts(historyIndex, historyTable, segmentIndex, segmentTable, fromStreamCut, Collections.emptyMap());
        assertEquals(6, segments.size());
        // 0, 3, 4, 5, 9, 10
        assertTrue(segments.stream().noneMatch(x -> x.getSegmentId() == one || x.getSegmentId() == two || x.getSegmentId() == six ||
                x.getSegmentId() == seven || x.getSegmentId() == eight));

        fromStreamCut = new HashMap<>();
        fromStreamCut.put(six, 0L);
        fromStreamCut.put(seven, 0L);
        fromStreamCut.put(eight, 0L);
        fromStreamCut.put(nine, 0L);
        segments = TableHelper.findSegmentsBetweenStreamCuts(historyIndex, historyTable, segmentIndex, segmentTable, fromStreamCut, Collections.emptyMap());
        assertEquals(5, segments.size());
        assertTrue(segments.stream().noneMatch(x -> x.getSegmentId() == one || x.getSegmentId() == two || x.getSegmentId() == three ||
                x.getSegmentId() == four || x.getSegmentId() == five));

        fromStreamCut = new HashMap<>();
        fromStreamCut.put(ten, 0L);
        fromStreamCut.put(nine, 0L);
        segments = TableHelper.findSegmentsBetweenStreamCuts(historyIndex, historyTable, segmentIndex, segmentTable, fromStreamCut, Collections.emptyMap());
        assertEquals(2, segments.size());
        assertTrue(segments.stream().noneMatch(x -> x.getSegmentId() == one || x.getSegmentId() == two || x.getSegmentId() == three ||
                x.getSegmentId() == four || x.getSegmentId() == five || x.getSegmentId() == six || x.getSegmentId() == seven ||
                x.getSegmentId() == eight));
    }

    @Test
    public void testGetSegmentsBetweenStreamCuts() throws Exception {
        long zero = computeSegmentId(0, 0);
        long one = computeSegmentId(1, 0);
        long two = computeSegmentId(2, 0);
        long three = computeSegmentId(3, 1);
        long four = computeSegmentId(4, 1);
        long five = computeSegmentId(5, 1);
        long six = computeSegmentId(6, 1);
        long seven = computeSegmentId(7, 1);
        long eight = computeSegmentId(8, 1);
        long nine = computeSegmentId(9, 2);
        long ten = computeSegmentId(10, 2);
        List<byte[]> list = setupTablesForStreamCut();
        byte[] segmentIndex = list.get(0);
        byte[] segmentTable = list.get(1);
        byte[] historyIndex = list.get(2);
        byte[] historyTable = list.get(3);

        // to before from
        Map<Long, Long> fromStreamCut = new HashMap<>();
        fromStreamCut.put(three, 0L);
        fromStreamCut.put(four, 0L);
        fromStreamCut.put(one, 0L);
        fromStreamCut.put(two, 0L);

        Map<Long, Long> toStreamCut = new HashMap<>();
        toStreamCut.put(zero, 0L);
        toStreamCut.put(one, 0L);
        toStreamCut.put(two, 0L);
        AssertExtensions.assertThrows(IllegalArgumentException.class,
                () -> TableHelper.findSegmentsBetweenStreamCuts(historyIndex, historyTable, segmentIndex, segmentTable, fromStreamCut, toStreamCut));

        // to and from overlap
        Map<Long, Long> fromStreamCutOverlap = new HashMap<>();
        fromStreamCutOverlap.put(three, 0L);
        fromStreamCutOverlap.put(four, 0L);
        fromStreamCutOverlap.put(one, 0L);
        fromStreamCutOverlap.put(two, 0L);

        Map<Long, Long> toStreamCutOverlap = new HashMap<>();
        toStreamCutOverlap.put(zero, 0L);
        toStreamCutOverlap.put(five, 0L);
        toStreamCutOverlap.put(six, 0L);
        toStreamCutOverlap.put(two, 0L);
        AssertExtensions.assertThrows(IllegalArgumentException.class,
                () -> TableHelper.findSegmentsBetweenStreamCuts(historyIndex, historyTable, segmentIndex, segmentTable, fromStreamCutOverlap, toStreamCutOverlap));

        Map<Long, Long> fromPartialOverlap = new HashMap<>();
        fromPartialOverlap.put(zero, 0L);
        fromPartialOverlap.put(five, 0L);
        fromPartialOverlap.put(six, 0L);
        fromPartialOverlap.put(two, 0L);

        Map<Long, Long> toPartialOverlap = new HashMap<>();
        toPartialOverlap.put(eight, 0L);
        toPartialOverlap.put(seven, 0L);
        toPartialOverlap.put(one, 0L);
        toPartialOverlap.put(three, 0L);
        toPartialOverlap.put(four, 0L);
        AssertExtensions.assertThrows(IllegalArgumentException.class,
                () -> TableHelper.findSegmentsBetweenStreamCuts(historyIndex, historyTable, segmentIndex, segmentTable, fromPartialOverlap, toPartialOverlap));

        // Success cases
        Map<Long, Long> fromStreamCutSuccess = new HashMap<>();
        fromStreamCutSuccess.put(zero, 0L);
        fromStreamCutSuccess.put(one, 0L);
        fromStreamCutSuccess.put(two, 0L);

        Map<Long, Long> toStreamCutSuccess = new HashMap<>();
        toStreamCutSuccess.put(zero, 0L);
        toStreamCutSuccess.put(five, 0L);
        toStreamCutSuccess.put(six, 0L);
        toStreamCutSuccess.put(two, 0L);
        List<Segment> segments = TableHelper.findSegmentsBetweenStreamCuts(historyIndex, historyTable, segmentIndex, segmentTable, fromStreamCutSuccess, toStreamCutSuccess);
        assertEquals(5, segments.size());
        assertTrue(segments.stream().allMatch(x -> x.getSegmentId() == zero || x.getSegmentId() == one || x.getSegmentId() == two ||
                x.getSegmentId() == five || x.getSegmentId() == six));

        fromStreamCutSuccess = new HashMap<>();
        fromStreamCutSuccess.put(zero, 0L);
        fromStreamCutSuccess.put(five, 0L);
        fromStreamCutSuccess.put(six, 0L);
        fromStreamCutSuccess.put(two, 0L);

        toStreamCutSuccess = new HashMap<>();
        toStreamCutSuccess.put(nine, 0L);
        toStreamCutSuccess.put(ten, 0L);
        segments = TableHelper.findSegmentsBetweenStreamCuts(historyIndex, historyTable, segmentIndex, segmentTable, fromStreamCutSuccess, toStreamCutSuccess);
        assertEquals(10, segments.size());
        assertTrue(segments.stream().noneMatch(x -> x.getSegmentId() == one));

        // empty from
        toStreamCutSuccess = new HashMap<>();
        toStreamCutSuccess.put(zero, 0L);
        toStreamCutSuccess.put(five, 0L);
        toStreamCutSuccess.put(six, 0L);
        toStreamCutSuccess.put(two, 0L);
        segments = TableHelper.findSegmentsBetweenStreamCuts(historyIndex, historyTable, segmentIndex, segmentTable, Collections.emptyMap(), toStreamCutSuccess);
        assertEquals(5, segments.size());
        assertTrue(segments.stream().noneMatch(x -> x.getSegmentId() == three || x.getSegmentId() == four || x.getSegmentId() == seven ||
                x.getSegmentId() == eight || x.getSegmentId() == nine || x.getSegmentId() == ten));
    }
    // endregion

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


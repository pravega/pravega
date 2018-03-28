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
import io.pravega.controller.store.stream.tables.SegmentRecord;
import io.pravega.controller.store.stream.tables.StreamTruncationRecord;
import io.pravega.controller.store.stream.tables.TableHelper;
import io.pravega.test.common.AssertExtensions;
import org.junit.Assert;
import org.junit.Test;

import java.text.ParseException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
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
        byte[] segmentTable = createSegmentTable(5, time);
        Assert.assertEquals(segmentTable.length / SegmentRecord.SEGMENT_RECORD_SIZE, 5);

        Segment segment = TableHelper.getSegment(0, segmentTable);
        assertEquals(segment.getNumber(), 0);
        assertEquals(segment.getStart(), time);
        assertEquals(segment.getKeyStart(), 0, 0);
        assertEquals(segment.getKeyEnd(), 1.0 / 5, 0);

        time = System.currentTimeMillis();
        epoch++;
        segmentTable = updateSegmentTable(segmentTable, epoch, 5, time);
        assertEquals(segmentTable.length / SegmentRecord.SEGMENT_RECORD_SIZE, 10);

        segment = TableHelper.getSegment(9, segmentTable);
        assertEquals(segment.getNumber(), 9);
        assertEquals(segment.getStart(), time);
        assertEquals(segment.getKeyStart(), 1.0 / 5 * 4, 0);
        assertEquals(segment.getKeyEnd(), 1.0, 0);
    }

    @Test
    public void getActiveSegmentsTest() {
        final List<Integer> startSegments = Lists.newArrayList(0, 1, 2, 3, 4);
        long timestamp = System.currentTimeMillis();

        byte[] indexTable = TableHelper.createIndexTable(timestamp);
        byte[] historyTable = TableHelper.createHistoryTable(timestamp, startSegments);
        List<Integer> activeSegments = TableHelper.getActiveSegments(historyTable);
        assertEquals(activeSegments, startSegments);

        List<Integer> newSegments = Lists.newArrayList(5, 6, 7, 8, 9);
        indexTable = TableHelper.updateIndexTable(indexTable, timestamp, historyTable.length);
        historyTable = TableHelper.addPartialRecordToHistoryTable(historyTable, newSegments);
        activeSegments = TableHelper.getActiveSegments(historyTable);
        assertEquals(activeSegments, startSegments);

        int epoch = TableHelper.getActiveEpoch(historyTable).getKey();
        assertEquals(0, epoch);
        epoch = TableHelper.getLatestEpoch(historyTable).getKey();
        assertEquals(1, epoch);

        HistoryRecord partial = HistoryRecord.readLatestRecord(historyTable, false).get();
        historyTable = TableHelper.completePartialRecordInHistoryTable(historyTable, partial, timestamp + 2);

        activeSegments = TableHelper.getActiveSegments(historyTable);
        assertEquals(activeSegments, newSegments);

        activeSegments = TableHelper.getActiveSegments(timestamp, new byte[0], historyTable, null, null);
        assertEquals(startSegments, activeSegments);

        activeSegments = TableHelper.getActiveSegments(0, new byte[0], historyTable, null, null);
        assertEquals(startSegments, activeSegments);

        activeSegments = TableHelper.getActiveSegments(timestamp - 1, new byte[0], historyTable, null, null);
        assertEquals(startSegments, activeSegments);

        activeSegments = TableHelper.getActiveSegments(timestamp + 1, new byte[0], historyTable, null, null);
        assertEquals(startSegments, activeSegments);

        activeSegments = TableHelper.getActiveSegments(timestamp + 2, new byte[0], historyTable, null, null);
        assertEquals(newSegments, activeSegments);

        activeSegments = TableHelper.getActiveSegments(timestamp + 3, new byte[0], historyTable, null, null);
        assertEquals(newSegments, activeSegments);
    }

    private Segment getSegment(int number, List<Segment> segments) {
        return segments.stream().filter(x -> x.getNumber() == number).findAny().get();
    }

    @Test(timeout = 10000)
    public void testSegmentCreationBeforePreviousScale() throws ParseException {
        List<Segment> segments = new ArrayList<>();
        List<Integer> newSegments = Lists.newArrayList(0, 1);
        // create stream
        long timestamp = 1503933145366L;
        Segment zero = new Segment(0, 0, timestamp, 0, 0.5);
        segments.add(zero);
        Segment one = new Segment(1, 0, timestamp, 0.5, 1.0);
        segments.add(one);

        byte[] historyTable = TableHelper.createHistoryTable(timestamp, newSegments);
        byte[] indexTable = TableHelper.createIndexTable(timestamp);

        // scale up 1... 0 -> 2, 3
        int numOfSplits = 2;
        double delta = (zero.getKeyEnd() - zero.getKeyStart()) / numOfSplits;

        ArrayList<AbstractMap.SimpleEntry<Double, Double>> simpleEntries = new ArrayList<>();
        for (int i = 0; i < numOfSplits; i++) {
            simpleEntries.add(new AbstractMap.SimpleEntry<>(zero.getKeyStart() + delta * i,
                    zero.getKeyStart() + (delta * (i + 1))));
        }

        // create segments before scale
        Segment two = new Segment(2, 1, 1503933266113L, simpleEntries.get(0).getKey(), simpleEntries.get(0).getValue());
        segments.add(two);
        Segment three = new Segment(3, 1, 1503933266113L, simpleEntries.get(1).getKey(), simpleEntries.get(1).getValue());
        segments.add(three);

        newSegments = Lists.newArrayList(1, 2, 3);

        // add partial record to history table
        indexTable = TableHelper.updateIndexTable(indexTable,
                1503933266113L,
                historyTable.length);
        historyTable = TableHelper.addPartialRecordToHistoryTable(historyTable, newSegments);

        HistoryRecord partial = HistoryRecord.readLatestRecord(historyTable, false).get();

        timestamp = 1503933266862L;

        // complete record in history table by adding time
        historyTable = TableHelper.completePartialRecordInHistoryTable(historyTable, partial, timestamp);
        HistoryRecord historyRecord = HistoryRecord.readLatestRecord(historyTable, false).get();

        // scale up 2.. 1 -> 4, 5
        delta = (one.getKeyEnd() - one.getKeyStart()) / numOfSplits;

        simpleEntries = new ArrayList<>();
        for (int i = 0; i < numOfSplits; i++) {
            simpleEntries.add(new AbstractMap.SimpleEntry<>(one.getKeyStart() + delta * i,
                    one.getKeyStart() + (delta * (i + 1))));
        }
        // create segments before scale
        Segment four = new Segment(4, 2, 1503933266188L, simpleEntries.get(0).getKey(), simpleEntries.get(0).getValue());
        segments.add(four);

        Segment five = new Segment(5, 2, 1503933266188L, simpleEntries.get(1).getKey(), simpleEntries.get(1).getValue());
        segments.add(five);

        newSegments = Lists.newArrayList(2, 3, 4, 5);

        // add partial record to history table
        indexTable = TableHelper.updateIndexTable(indexTable,
                1503933266188L,
                historyTable.length);
        historyTable = TableHelper.addPartialRecordToHistoryTable(historyTable, newSegments);

        partial = HistoryRecord.readLatestRecord(historyTable, false).get();
        // Notice: segment was created at timestamp but we are recording its entry in history table at timestamp + 10000
        timestamp = 1503933288726L;

        // complete record in history table by adding time
        historyTable = TableHelper.completePartialRecordInHistoryTable(historyTable, partial, timestamp);
        historyRecord = HistoryRecord.readLatestRecord(historyTable, false).get();

        // scale up 3.. 5 -> 6, 7
        delta = (five.getKeyEnd() - five.getKeyStart()) / numOfSplits;

        simpleEntries = new ArrayList<>();
        for (int i = 0; i < numOfSplits; i++) {
            simpleEntries.add(new AbstractMap.SimpleEntry<>(five.getKeyStart() + delta * i,
                    five.getKeyStart() + (delta * (i + 1))));
        }

        // create new segments
        Segment six = new Segment(6, 3, 1503933409076L, simpleEntries.get(0).getKey(), simpleEntries.get(0).getValue());
        segments.add(six);

        Segment seven = new Segment(7, 3, 1503933409076L, simpleEntries.get(1).getKey(), simpleEntries.get(1).getValue());
        segments.add(seven);

        newSegments = Lists.newArrayList(2, 3, 4, 6, 7);
        // create partial record in history table
        indexTable = TableHelper.updateIndexTable(indexTable,
                timestamp,
                historyTable.length);
        historyTable = TableHelper.addPartialRecordToHistoryTable(historyTable, newSegments);

        partial = HistoryRecord.readLatestRecord(historyTable, false).get();

        timestamp = 1503933409806L;
        // find successor candidates before completing scale.
        List<Integer> candidates5 = TableHelper.findSegmentSuccessorCandidates(five,
                indexTable,
                historyTable);

        assertTrue(candidates5.containsAll(Arrays.asList(2, 3, 4, 6, 7)));
        // complete record in history table by adding time
        historyTable = TableHelper.completePartialRecordInHistoryTable(historyTable, partial, timestamp);

        // verify successor candidates after completing history record
        candidates5 = TableHelper.findSegmentSuccessorCandidates(five,
                indexTable,
                historyTable);
        assertTrue(candidates5.containsAll(Arrays.asList(2, 3, 4, 6, 7)));

        // scale down 6, 7 -> 8

        // scale down
        timestamp = 1503933560447L;
        // add another scale
        Segment eight = new Segment(8, 3, timestamp, six.keyStart, seven.keyEnd);
        segments.add(eight);

        newSegments = Lists.newArrayList(2, 3, 4, 8);
        indexTable = TableHelper.updateIndexTable(indexTable,
                timestamp,
                historyTable.length);
        historyTable = TableHelper.addPartialRecordToHistoryTable(historyTable, newSegments);

        partial = HistoryRecord.readLatestRecord(historyTable, false).get();
        timestamp = 1503933560448L;
        // complete scale
        historyTable = TableHelper.completePartialRecordInHistoryTable(historyTable, partial, timestamp);
        historyRecord = HistoryRecord.readLatestRecord(historyTable, false).get();

        // verify successors again after a new scale entry comes in
        candidates5 = TableHelper.findSegmentSuccessorCandidates(five,
                indexTable,
                historyTable);

        assertTrue(candidates5.containsAll(Arrays.asList(2, 3, 4, 6, 7)));
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
        byte[] indexTable = TableHelper.createIndexTable(timestamp);

        // 3, 4 -> 5
        epoch++;
        newSegments = Lists.newArrayList(0, 1, 2, 5);
        timestamp = timestamp + 1;
        Segment five = new Segment(5, epoch, timestamp, 0.6, 1);
        segments.add(five);

        indexTable = TableHelper.updateIndexTable(indexTable, timestamp, historyTable.length);
        historyTable = TableHelper.addPartialRecordToHistoryTable(historyTable, newSegments);

        // check predecessor segment in partial record
        predecessors = TableHelper.getOverlaps(five,
                TableHelper.findSegmentPredecessorCandidates(five,
                        indexTable,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments))
                        .collect(Collectors.toList()));
        // check that segment from partial record is returned as successor
        successors = TableHelper.getOverlaps(three,
                TableHelper.findSegmentSuccessorCandidates(three,
                        indexTable,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments))
                        .collect(Collectors.toList()));
        assertEquals(predecessors, Lists.newArrayList(3, 4));
        assertEquals(successors, Lists.newArrayList(5));

        HistoryRecord partial = HistoryRecord.readLatestRecord(historyTable, false).get();
        // Notice: segment was created at timestamp but we are recording its entry in history table at timestamp + 5
        timestamp = timestamp + 5;
        historyTable = TableHelper.completePartialRecordInHistoryTable(historyTable, partial, timestamp);

        // 1 -> 6,7.. 2,5 -> 8
        epoch++;
        newSegments = Lists.newArrayList(0, 6, 7, 8);
        timestamp = timestamp + 10;
        Segment six = new Segment(6, epoch, timestamp, 0.2, 0.3);
        segments.add(six);
        Segment seven = new Segment(7, epoch, timestamp, 0.3, 0.4);
        segments.add(seven);
        Segment eight = new Segment(8, epoch, timestamp, 0.4, 1);
        segments.add(eight);

        indexTable = TableHelper.updateIndexTable(indexTable, timestamp, historyTable.length);
        historyTable = TableHelper.addPartialRecordToHistoryTable(historyTable, newSegments);

        // check that previous partial record is not a regular record and its successor and predecessors are returned successfully
        predecessors = TableHelper.getOverlaps(five,
                TableHelper.findSegmentPredecessorCandidates(five,
                        indexTable,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments))
                        .collect(Collectors.toList()));
        successors = TableHelper.getOverlaps(five,
                TableHelper.findSegmentSuccessorCandidates(five,
                        indexTable,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments))
                        .collect(Collectors.toList()));
        assertEquals(predecessors, Lists.newArrayList(3, 4));
        assertEquals(successors, Lists.newArrayList(8));

        partial = HistoryRecord.readLatestRecord(historyTable, false).get();
        timestamp = timestamp + 5;
        historyTable = TableHelper.completePartialRecordInHistoryTable(historyTable, partial, timestamp);

        // 7 -> 9,10.. 8 -> 10, 11
        epoch++;
        newSegments = Lists.newArrayList(0, 6, 9, 10, 11);
        timestamp = timestamp + 10;
        Segment nine = new Segment(9, epoch, timestamp, 0.3, 0.35);
        segments.add(nine);
        Segment ten = new Segment(10, epoch, timestamp, 0.35, 0.6);
        segments.add(ten);
        Segment eleven = new Segment(11, epoch, timestamp, 0.6, 1);
        segments.add(eleven);

        indexTable = TableHelper.updateIndexTable(indexTable, timestamp, historyTable.length);
        historyTable = TableHelper.addPartialRecordToHistoryTable(historyTable, newSegments);
        partial = HistoryRecord.readLatestRecord(historyTable, false).get();
        timestamp = timestamp + 5;
        historyTable = TableHelper.completePartialRecordInHistoryTable(historyTable, partial, timestamp);

        // find predecessor and successor with index table being stale
        predecessors = TableHelper.getOverlaps(ten,
                TableHelper.findSegmentPredecessorCandidates(ten,
                        indexTable,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments))
                        .collect(Collectors.toList()));
        successors = TableHelper.getOverlaps(seven,
                TableHelper.findSegmentSuccessorCandidates(seven,
                        indexTable,
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
                        indexTable,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments))
                        .collect(Collectors.toList()));
        successors = TableHelper.getOverlaps(zero,
                TableHelper.findSegmentSuccessorCandidates(zero,
                        indexTable,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments))
                        .collect(Collectors.toList()));

        assertEquals(predecessors, new ArrayList<Integer>());
        assertEquals(successors, new ArrayList<Integer>());

        predecessors = TableHelper.getOverlaps(one,
                TableHelper.findSegmentPredecessorCandidates(one,
                        indexTable,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments))
                        .collect(Collectors.toList()));
        successors = TableHelper.getOverlaps(one,
                TableHelper.findSegmentSuccessorCandidates(one,
                        indexTable,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments))
                        .collect(Collectors.toList()));
        assertEquals(predecessors, new ArrayList<Integer>());
        assertEquals(successors, Lists.newArrayList(6, 7));

        predecessors = TableHelper.getOverlaps(two,
                TableHelper.findSegmentPredecessorCandidates(two,
                        indexTable,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        successors = TableHelper.getOverlaps(two,
                TableHelper.findSegmentSuccessorCandidates(two,
                        indexTable,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        assertEquals(predecessors, new ArrayList<Integer>());
        assertEquals(successors, Lists.newArrayList(8));

        predecessors = TableHelper.getOverlaps(three,
                TableHelper.findSegmentPredecessorCandidates(three,
                        indexTable,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        successors = TableHelper.getOverlaps(three,
                TableHelper.findSegmentSuccessorCandidates(three,
                        indexTable,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        assertEquals(predecessors, new ArrayList<Integer>());
        assertEquals(successors, Lists.newArrayList(5));

        predecessors = TableHelper.getOverlaps(four,
                TableHelper.findSegmentPredecessorCandidates(four,
                        indexTable,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        successors = TableHelper.getOverlaps(four,
                TableHelper.findSegmentSuccessorCandidates(four,
                        indexTable,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        assertEquals(predecessors, new ArrayList<Integer>());
        assertEquals(successors, Lists.newArrayList(5));

        predecessors = TableHelper.getOverlaps(five,
                TableHelper.findSegmentPredecessorCandidates(five,
                        indexTable,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        successors = TableHelper.getOverlaps(five,
                TableHelper.findSegmentSuccessorCandidates(five,
                        indexTable,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        assertEquals(predecessors, Lists.newArrayList(3, 4));
        assertEquals(successors, Lists.newArrayList(8));

        predecessors = TableHelper.getOverlaps(six,
                TableHelper.findSegmentPredecessorCandidates(six,
                        indexTable,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        successors = TableHelper.getOverlaps(six,
                TableHelper.findSegmentSuccessorCandidates(six,
                        indexTable,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        assertEquals(predecessors, Lists.newArrayList(1));
        assertEquals(successors, new ArrayList<>());

        predecessors = TableHelper.getOverlaps(seven,
                TableHelper.findSegmentPredecessorCandidates(seven,
                        indexTable,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        successors = TableHelper.getOverlaps(seven,
                TableHelper.findSegmentSuccessorCandidates(seven,
                        indexTable,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        assertEquals(predecessors, Lists.newArrayList(1));
        assertEquals(successors, Lists.newArrayList(9, 10));

        predecessors = TableHelper.getOverlaps(eight,
                TableHelper.findSegmentPredecessorCandidates(eight,
                        indexTable,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        successors = TableHelper.getOverlaps(eight,
                TableHelper.findSegmentSuccessorCandidates(eight,
                        indexTable,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        assertEquals(predecessors, Lists.newArrayList(2, 5));
        assertEquals(successors, Lists.newArrayList(10, 11));

        predecessors = TableHelper.getOverlaps(nine,
                TableHelper.findSegmentPredecessorCandidates(nine,
                        indexTable,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        successors = TableHelper.getOverlaps(nine,
                TableHelper.findSegmentSuccessorCandidates(nine,
                        indexTable,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        assertEquals(predecessors, Lists.newArrayList(7));
        assertEquals(successors, new ArrayList<>());

        predecessors = TableHelper.getOverlaps(ten,
                TableHelper.findSegmentPredecessorCandidates(ten,
                        indexTable,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        successors = TableHelper.getOverlaps(ten,
                TableHelper.findSegmentSuccessorCandidates(ten,
                        indexTable,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        assertEquals(predecessors, Lists.newArrayList(7, 8));
        assertEquals(successors, new ArrayList<>());

        predecessors = TableHelper.getOverlaps(eleven,
                TableHelper.findSegmentPredecessorCandidates(eleven,
                        indexTable,
                        historyTable)
                        .stream()
                        .map(x -> getSegment(x, segments)).collect(Collectors.toList()));
        successors = TableHelper.getOverlaps(eleven,
                TableHelper.findSegmentSuccessorCandidates(eleven,
                        indexTable,
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
        byte[] segmentTable = createSegmentTable(5, timestamp);

        byte[] historyTable = TableHelper.createHistoryTable(timestamp, startSegments);

        // start new scale
        List<Integer> newSegments = Lists.newArrayList(5, 6, 7, 8, 9);
        final double keyRangeChunk = 1.0 / 5;
        final List<AbstractMap.SimpleEntry<Double, Double>> newRanges = IntStream.range(0, 5)
                .boxed()
                .map(x -> new AbstractMap.SimpleEntry<>(x * keyRangeChunk, (x + 1) * keyRangeChunk))
                .collect(Collectors.toList());
        EpochTransitionRecord consistentEpochTransitionRecord = TableHelper.computeEpochTransition(historyTable, segmentTable,
                Lists.newArrayList(0, 1, 2, 3, 4), newRanges, timestamp + 1);

        final double keyRangeChunkInconsistent = 1.0 / 2;
        final List<AbstractMap.SimpleEntry<Double, Double>> newRangesInconsistent = IntStream.range(0, 2)
                .boxed()
                .map(x -> new AbstractMap.SimpleEntry<>(x * keyRangeChunkInconsistent, (x + 1) * keyRangeChunkInconsistent))
                .collect(Collectors.toList());

        EpochTransitionRecord inconsistentEpochTransitionRecord = TableHelper.computeEpochTransition(historyTable, segmentTable,
                Lists.newArrayList(0, 1, 2, 3, 4), newRangesInconsistent, timestamp + 1);

        // before updating segment table, both records should be consistent.
        assertTrue(TableHelper.isEpochTransitionConsistent(consistentEpochTransitionRecord, historyTable, segmentTable));
        assertTrue(TableHelper.isEpochTransitionConsistent(inconsistentEpochTransitionRecord, historyTable, segmentTable));

        // update segment table corresponding to consistent epoch transition record
        epoch++;
        segmentTable = updateSegmentTable(segmentTable, epoch, newRanges, timestamp + 1);

        // now only consistentEpochTransitionRecord should return true as only its new range should match the state in
        // segment table
        assertTrue(TableHelper.isEpochTransitionConsistent(consistentEpochTransitionRecord, historyTable, segmentTable));
        assertFalse(TableHelper.isEpochTransitionConsistent(inconsistentEpochTransitionRecord, historyTable, segmentTable));

        historyTable = TableHelper.addPartialRecordToHistoryTable(historyTable, newSegments);
        // nothing should change the consistency even with history table update
        assertTrue(TableHelper.isEpochTransitionConsistent(consistentEpochTransitionRecord, historyTable, segmentTable));
        assertFalse(TableHelper.isEpochTransitionConsistent(inconsistentEpochTransitionRecord, historyTable, segmentTable));

        HistoryRecord partial = HistoryRecord.readLatestRecord(historyTable, false).get();
        historyTable = TableHelper.completePartialRecordInHistoryTable(historyTable, partial, timestamp + 2);

        // nothing should change the consistency even with history table update
        assertTrue(TableHelper.isEpochTransitionConsistent(consistentEpochTransitionRecord, historyTable, segmentTable));
        assertFalse(TableHelper.isEpochTransitionConsistent(inconsistentEpochTransitionRecord, historyTable, segmentTable));
    }

    @Test
    public void scaleInputValidityTest() {
        long timestamp = System.currentTimeMillis();

        byte[] segmentTable = createSegmentTable(5, timestamp);
        final double keyRangeChunk = 1.0 / 5;

        List<AbstractMap.SimpleEntry<Double, Double>> newRanges = new ArrayList<>();
        // 1. empty newRanges
        assertFalse(TableHelper.isScaleInputValid(Lists.newArrayList(0, 1), newRanges, segmentTable));

        // 2. simple mismatch
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, keyRangeChunk));
        assertFalse(TableHelper.isScaleInputValid(Lists.newArrayList(0, 1), newRanges, segmentTable));

        // 3. simple valid match
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 2 * keyRangeChunk));
        assertTrue(TableHelper.isScaleInputValid(Lists.newArrayList(0, 1), newRanges, segmentTable));

        // 4. valid 2 disjoint merges
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 2 * keyRangeChunk));
        newRanges.add(new AbstractMap.SimpleEntry<>(3 * keyRangeChunk, 1.0));
        assertTrue(TableHelper.isScaleInputValid(Lists.newArrayList(0, 1, 3, 4), newRanges, segmentTable));

        // 5. valid 1 merge and 1 disjoint
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(keyRangeChunk, 2 * keyRangeChunk));
        newRanges.add(new AbstractMap.SimpleEntry<>(3 * keyRangeChunk, 1.0));
        assertTrue(TableHelper.isScaleInputValid(Lists.newArrayList(1, 3, 4), newRanges, segmentTable));

        // 6. valid 1 merge, 2 splits
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 2 * keyRangeChunk));
        newRanges.add(new AbstractMap.SimpleEntry<>(3 * keyRangeChunk, 0.7));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.7, 0.8));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.8, 0.9));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.9, 1.0));
        assertTrue(TableHelper.isScaleInputValid(Lists.newArrayList(0, 1, 3, 4), newRanges, segmentTable));

        // 7. 1 merge, 1 split and 1 invalid split
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 2 * keyRangeChunk));
        newRanges.add(new AbstractMap.SimpleEntry<>(3 * keyRangeChunk, 0.7));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.7, 0.8));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.8, 0.9));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.9, 0.99));
        assertFalse(TableHelper.isScaleInputValid(Lists.newArrayList(0, 1, 3, 4), newRanges, segmentTable));

        // 8. valid unsorted segments to seal
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 2 * keyRangeChunk));
        newRanges.add(new AbstractMap.SimpleEntry<>(3 * keyRangeChunk, 0.7));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.7, 0.8));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.8, 0.9));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.9, 1.0));
        assertTrue(TableHelper.isScaleInputValid(Lists.newArrayList(4, 0, 1, 3), newRanges, segmentTable));

        // 9. valid unsorted new ranges
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.9, 1.0));
        newRanges.add(new AbstractMap.SimpleEntry<>(3 * keyRangeChunk, 0.7));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.7, 0.8));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 2 * keyRangeChunk));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.8, 0.9));
        assertTrue(TableHelper.isScaleInputValid(Lists.newArrayList(4, 0, 1, 3), newRanges, segmentTable));

        // 10. invalid input range low == high
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 0.2));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.2));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.4));
        assertFalse(TableHelper.isScaleInputValid(Lists.newArrayList(0, 1), newRanges, segmentTable));

        // 11. invalid input range low > high
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 0.2));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.3, 0.2));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.4));
        assertFalse(TableHelper.isScaleInputValid(Lists.newArrayList(0, 1), newRanges, segmentTable));

        // 12. invalid overlapping key ranges
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.4));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.3, 3 * keyRangeChunk));
        assertFalse(TableHelper.isScaleInputValid(Lists.newArrayList(1, 2), newRanges, segmentTable));

        // 13. invalid overlapping key ranges -- a contains b
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.4));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.3, 0.33));
        assertFalse(TableHelper.isScaleInputValid(Lists.newArrayList(1), newRanges, segmentTable));

        // 14. invalid overlapping key ranges -- b contains a (with b.low == a.low)
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.33));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.4));
        assertFalse(TableHelper.isScaleInputValid(Lists.newArrayList(1), newRanges, segmentTable));

        // 15. invalid overlapping key ranges b.low < a.high
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.35));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.3, 0.4));
        assertFalse(TableHelper.isScaleInputValid(Lists.newArrayList(1), newRanges, segmentTable));

        // 16. invalid overlapping key ranges.. a.high < b.low
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.25));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.3, 0.4));
        assertFalse(TableHelper.isScaleInputValid(Lists.newArrayList(1), newRanges, segmentTable));
    }

    @Test(timeout = 10000)
    public void truncationTest() {
        final List<Integer> startSegments = Lists.newArrayList(0, 1);
        int epoch = 0;
        // epoch 0
        long timestamp = System.currentTimeMillis();
        byte[] segmentTable = createSegmentTable(2, timestamp);

        byte[] historyTable = TableHelper.createHistoryTable(timestamp, startSegments);
        byte[] indexTable = TableHelper.createIndexTable(timestamp);

        List<Integer> activeSegments = TableHelper.getActiveSegments(historyTable);
        assertEquals(activeSegments, startSegments);

        // epoch 1
        epoch++;
        List<Integer> newSegments1 = Lists.newArrayList(0, 2, 3);
        List<AbstractMap.SimpleEntry<Double, Double>> newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<Double, Double>(0.5, 0.75));
        newRanges.add(new AbstractMap.SimpleEntry<Double, Double>(0.75, 1.0));

        segmentTable = updateSegmentTable(segmentTable, epoch, newRanges, timestamp + 1);
        indexTable = TableHelper.updateIndexTable(indexTable, timestamp + 1, historyTable.length);
        historyTable = TableHelper.addPartialRecordToHistoryTable(historyTable, newSegments1);
        HistoryRecord partial = HistoryRecord.readLatestRecord(historyTable, false).get();
        historyTable = TableHelper.completePartialRecordInHistoryTable(historyTable, partial, timestamp + 1);

        // epoch 2
        epoch++;
        List<Integer> newSegments2 = Lists.newArrayList(0, 2, 4, 5);
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<Double, Double>(0.75, (0.75 + 1.0) / 2));
        newRanges.add(new AbstractMap.SimpleEntry<Double, Double>((0.75 + 1.0) / 2, 1.0));

        segmentTable = updateSegmentTable(segmentTable, epoch, newRanges, timestamp + 2);
        indexTable = TableHelper.updateIndexTable(indexTable, timestamp + 2, historyTable.length);
        historyTable = TableHelper.addPartialRecordToHistoryTable(historyTable, newSegments2);
        partial = HistoryRecord.readLatestRecord(historyTable, false).get();
        historyTable = TableHelper.completePartialRecordInHistoryTable(historyTable, partial, timestamp + 2);

        // epoch 3
        epoch++;
        List<Integer> newSegments3 = Lists.newArrayList(0, 4, 5, 6, 7);
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<Double, Double>(0.5, (0.75 + 0.5) / 2));
        newRanges.add(new AbstractMap.SimpleEntry<Double, Double>((0.75 + 0.5) / 2, 0.75));

        segmentTable = updateSegmentTable(segmentTable, epoch, newRanges, timestamp + 3);
        indexTable = TableHelper.updateIndexTable(indexTable, timestamp + 3, historyTable.length);
        historyTable = TableHelper.addPartialRecordToHistoryTable(historyTable, newSegments3);
        partial = HistoryRecord.readLatestRecord(historyTable, false).get();
        historyTable = TableHelper.completePartialRecordInHistoryTable(historyTable, partial, timestamp + 3);

        // epoch 4
        epoch++;
        List<Integer> newSegments4 = Lists.newArrayList(4, 5, 6, 7, 8, 9);
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<Double, Double>(0.0, (0.0 + 0.5) / 2));
        newRanges.add(new AbstractMap.SimpleEntry<Double, Double>((0.0 + 0.5) / 2, 0.5));

        segmentTable = updateSegmentTable(segmentTable, epoch, newRanges, timestamp + 4);
        indexTable = TableHelper.updateIndexTable(indexTable, timestamp + 4, historyTable.length);
        historyTable = TableHelper.addPartialRecordToHistoryTable(historyTable, newSegments4);
        partial = HistoryRecord.readLatestRecord(historyTable, false).get();
        historyTable = TableHelper.completePartialRecordInHistoryTable(historyTable, partial, timestamp + 4);

        // happy day
        Map<Integer, Long> streamCut1 = new HashMap<>();
        streamCut1.put(0, 1L);
        streamCut1.put(1, 1L);
        StreamTruncationRecord truncationRecord = TableHelper.computeTruncationRecord(indexTable, historyTable,
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
        truncationRecord = TableHelper.computeTruncationRecord(indexTable, historyTable, segmentTable, streamCut2, truncationRecord);
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
        truncationRecord = TableHelper.computeTruncationRecord(indexTable, historyTable, segmentTable, streamCut3, truncationRecord);
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
        byte[] finalIndexTable = indexTable;
        byte[] finalHistoryTable = historyTable;
        byte[] finalSegmentTable = segmentTable;
        StreamTruncationRecord finalTruncationRecord = truncationRecord;
        AssertExtensions.assertThrows("",
                () -> TableHelper.computeTruncationRecord(finalIndexTable, finalHistoryTable, finalSegmentTable, streamCut4, finalTruncationRecord),
                e -> e instanceof IllegalArgumentException);

        Map<Integer, Long> streamCut5 = new HashMap<>();
        streamCut3.put(2, 10L);
        streamCut3.put(4, 10L);
        streamCut3.put(5, 10L);
        streamCut3.put(0, 10L);
        AssertExtensions.assertThrows("",
                () -> TableHelper.computeTruncationRecord(finalIndexTable, finalHistoryTable, finalSegmentTable, streamCut5, finalTruncationRecord),
                e -> e instanceof IllegalArgumentException);
    }

    private byte[] createSegmentTable(int numSegments, long eventTime) {
        final double keyRangeChunk = 1.0 / numSegments;

        List<AbstractMap.SimpleEntry<Double, Double>> newRanges = IntStream.range(0, numSegments)
                .boxed()
                .map(x -> new AbstractMap.SimpleEntry<>(x * keyRangeChunk, (x + 1) * keyRangeChunk))
                .collect(Collectors.toList());

        return TableHelper.updateSegmentTable(0, 0, new byte[0], newRanges, eventTime);
    }

    private byte[] updateSegmentTable(byte[] segmentTable, int creationEpoch, int numSegments, long eventTime) {
        final double keyRangeChunk = 1.0 / numSegments;
        List<AbstractMap.SimpleEntry<Double, Double>> newRanges = IntStream.range(0, numSegments)
                .boxed()
                .map(x -> new AbstractMap.SimpleEntry<>(x * keyRangeChunk, (x + 1) * keyRangeChunk))
                .collect(Collectors.toList());

        return updateSegmentTable(segmentTable, creationEpoch, newRanges, eventTime);
    }

    private byte[] updateSegmentTable(byte[] segmentTable, int creationEpoch, List<AbstractMap.SimpleEntry<Double, Double>> newRanges, long eventTime) {
        final int startingSegNum = segmentTable.length / SegmentRecord.SEGMENT_RECORD_SIZE;

        return TableHelper.updateSegmentTable(startingSegNum, creationEpoch, segmentTable, newRanges, eventTime);
    }

}


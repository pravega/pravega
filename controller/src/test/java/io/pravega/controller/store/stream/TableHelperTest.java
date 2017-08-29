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
import io.pravega.controller.store.stream.tables.HistoryRecord;
import io.pravega.controller.store.stream.tables.SegmentRecord;
import io.pravega.controller.store.stream.tables.TableHelper;
import org.junit.Assert;
import org.junit.Test;

import java.text.ParseException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TableHelperTest {
    @Test
    public void getSegmentTest() {
        long time = System.currentTimeMillis();
        byte[] segmentTable = createSegmentTable(5, time);
        Assert.assertEquals(segmentTable.length / SegmentRecord.SEGMENT_RECORD_SIZE, 5);

        Segment segment = TableHelper.getSegment(0, segmentTable);
        assertEquals(segment.getNumber(), 0);
        assertEquals(segment.getStart(), time);
        assertEquals(segment.getKeyStart(), 0, 0);
        assertEquals(segment.getKeyEnd(), 1.0 / 5, 0);

        time = System.currentTimeMillis();
        segmentTable = updateSegmentTable(segmentTable, 5, time);
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
        byte[] historyTable = TableHelper.createHistoryTable(timestamp, startSegments);
        List<Integer> activeSegments = TableHelper.getActiveSegments(historyTable);
        assertEquals(activeSegments, startSegments);

        List<Integer> newSegments = Lists.newArrayList(5, 6, 7, 8, 9);

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

        activeSegments = TableHelper.getActiveSegments(timestamp, new byte[0], historyTable);
        assertEquals(startSegments, activeSegments);

        activeSegments = TableHelper.getActiveSegments(0, new byte[0], historyTable);
        assertEquals(startSegments, activeSegments);

        activeSegments = TableHelper.getActiveSegments(timestamp - 1, new byte[0], historyTable);
        assertEquals(startSegments, activeSegments);

        activeSegments = TableHelper.getActiveSegments(timestamp + 1, new byte[0], historyTable);
        assertEquals(startSegments, activeSegments);

        activeSegments = TableHelper.getActiveSegments(timestamp + 2, new byte[0], historyTable);
        assertEquals(newSegments, activeSegments);

        activeSegments = TableHelper.getActiveSegments(timestamp + 3, new byte[0], historyTable);
        assertEquals(newSegments, activeSegments);
    }

    private Segment getSegment(int number, List<Segment> segments) {
        return segments.stream().filter(x -> x.getNumber() == number).findAny().get();
    }

    @Test
    public void testNoValuePresentError() {
        // no value present error comes because:
        // - Index is not yet updated.
        // - And segment creation time is before history record's time.
        // While trying to find successor we look for record in history table with
        // segment creation and get an old record. We search for segment sealed event
        // between history record and last indexed entry both of which preceed segment creation entry.
        List<Segment> segments = new ArrayList<>();
        List<Integer> newSegments = Lists.newArrayList(0, 1, 2, 3, 4);
        long timestamp = System.currentTimeMillis();
        Segment zero = new Segment(0, timestamp, 0, 0.2);
        segments.add(zero);
        Segment one = new Segment(1, timestamp, 0.2, 0.4);
        segments.add(one);
        Segment two = new Segment(2, timestamp, 0.4, 0.6);
        segments.add(two);
        Segment three = new Segment(3, timestamp, 0.6, 0.8);
        segments.add(three);
        Segment four = new Segment(4, timestamp, 0.8, 1);
        segments.add(four);

        byte[] historyTable = TableHelper.createHistoryTable(timestamp, newSegments);
        byte[] indexTable = TableHelper.createIndexTable(timestamp, 0);

        timestamp = timestamp + 10000;
        // scale down
        Segment five = new Segment(5, timestamp, 0.4, 1);
        segments.add(five);
        newSegments = Lists.newArrayList(0, 1, 5);

        historyTable = TableHelper.addPartialRecordToHistoryTable(historyTable, newSegments);

        HistoryRecord partial = HistoryRecord.readLatestRecord(historyTable, false).get();
        // Notice: segment was created at timestamp but we are recording its entry in history table at timestamp + 10000
        timestamp = timestamp + 10000;
        historyTable = TableHelper.completePartialRecordInHistoryTable(historyTable, partial, timestamp);

        timestamp = timestamp + 10000;
        Segment six = new Segment(6, timestamp, 0.0, 1);
        segments.add(six);
        newSegments = Lists.newArrayList(6);

        historyTable = TableHelper.addPartialRecordToHistoryTable(historyTable, newSegments);

        partial = HistoryRecord.readLatestRecord(historyTable, false).get();
        // Notice: segment was created at timestamp but we are recording its entry in history table at timestamp + 10000
        timestamp = timestamp + 10000;
        historyTable = TableHelper.completePartialRecordInHistoryTable(historyTable, partial, timestamp);

        List<Integer> predecessors, successors;

        // find predecessors and successors when update to index table hasn't happened
        predecessors = TableHelper.getOverlaps(five,
                TableHelper
                        .findSegmentPredecessorCandidates(five,
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
        assertEquals(predecessors, Lists.newArrayList(2, 3, 4));
        assertEquals(successors, Lists.newArrayList(6));
    }

    @Test
    public void testSegmentCreationBeforePreviousScale() throws ParseException {
        // no value present error comes because:
        // - Index is not yet updated.
        // - And segment creation time is before history record's time.
        // While trying to find successor we look for record in history table with
        // segment creation and get an old record. We search for segment sealed event
        // between history record and last indexed entry both of which preceed segment creation entry.
        List<Segment> segments = new ArrayList<>();
        List<Integer> newSegments = Lists.newArrayList(0, 1);
        // create stream
        long timestamp = 1503933145366L;
        Segment zero = new Segment(0, timestamp, 0, 0.5);
        segments.add(zero);
        Segment one = new Segment(1, timestamp, 0.5, 1.0);
        segments.add(one);

        byte[] historyTable = TableHelper.createHistoryTable(timestamp, newSegments);
        byte[] indexTable = TableHelper.createIndexTable(timestamp, 0);

        // scale up 1... 0 -> 2, 3
        int numOfSplits = 2;
        double delta = (zero.getKeyEnd() - zero.getKeyStart()) / numOfSplits;

        ArrayList<AbstractMap.SimpleEntry<Double, Double>> simpleEntries = new ArrayList<>();
        for (int i = 0; i < numOfSplits; i++) {
            simpleEntries.add(new AbstractMap.SimpleEntry<>(zero.getKeyStart() + delta * i,
                    zero.getKeyStart() + (delta * (i + 1))));
        }

        Segment two = new Segment(2, 1503933266113L, simpleEntries.get(0).getKey(), simpleEntries.get(0).getValue());
        segments.add(two);
        Segment three = new Segment(3, 1503933266113L, simpleEntries.get(1).getKey(), simpleEntries.get(1).getValue());
        segments.add(three);

        newSegments = Lists.newArrayList(1, 2, 3);

        historyTable = TableHelper.addPartialRecordToHistoryTable(historyTable, newSegments);

        HistoryRecord partial = HistoryRecord.readLatestRecord(historyTable, false).get();
        // Notice: segment was created at timestamp but we are recording its entry in history table at timestamp + 10000
        timestamp = 1503933266862L;

        historyTable = TableHelper.completePartialRecordInHistoryTable(historyTable, partial, timestamp);
        HistoryRecord historyRecord = HistoryRecord.readLatestRecord(historyTable, false).get();

        indexTable = TableHelper.updateIndexTable(indexTable,
                historyRecord.getScaleTime(),
                historyRecord.getOffset());

        // scale up 2.. 1 -> 4, 5
        delta = (one.getKeyEnd() - one.getKeyStart()) / numOfSplits;

        simpleEntries = new ArrayList<>();
        for (int i = 0; i < numOfSplits; i++) {
            simpleEntries.add(new AbstractMap.SimpleEntry<>(one.getKeyStart() + delta * i,
                    one.getKeyStart() + (delta * (i + 1))));
        }

        Segment four = new Segment(4, 1503933266188L, simpleEntries.get(0).getKey(), simpleEntries.get(0).getValue());
        segments.add(four);

        Segment five = new Segment(5, 1503933266188L, simpleEntries.get(1).getKey(), simpleEntries.get(1).getValue());
        segments.add(five);

        newSegments = Lists.newArrayList(2, 3, 4, 5);

        historyTable = TableHelper.addPartialRecordToHistoryTable(historyTable, newSegments);

        partial = HistoryRecord.readLatestRecord(historyTable, false).get();
        // Notice: segment was created at timestamp but we are recording its entry in history table at timestamp + 10000
        timestamp = 1503933288726L;

        historyTable = TableHelper.completePartialRecordInHistoryTable(historyTable, partial, timestamp);
        historyRecord = HistoryRecord.readLatestRecord(historyTable, false).get();

        indexTable = TableHelper.updateIndexTable(indexTable,
                historyRecord.getScaleTime(),
                historyRecord.getOffset());

        // scale up 3.. 5 -> 6, 7
        delta = (five.getKeyEnd() - five.getKeyStart()) / numOfSplits;

        simpleEntries = new ArrayList<>();
        for (int i = 0; i < numOfSplits; i++) {
            simpleEntries.add(new AbstractMap.SimpleEntry<>(five.getKeyStart() + delta * i,
                    five.getKeyStart() + (delta * (i + 1))));
        }

        Segment six = new Segment(6, 1503933409076L, simpleEntries.get(0).getKey(), simpleEntries.get(0).getValue());
        segments.add(six);

        Segment seven = new Segment(7, 1503933409076L, simpleEntries.get(1).getKey(), simpleEntries.get(1).getValue());
        segments.add(seven);

        newSegments = Lists.newArrayList(2, 3, 4, 6, 7);

        historyTable = TableHelper.addPartialRecordToHistoryTable(historyTable, newSegments);

        partial = HistoryRecord.readLatestRecord(historyTable, false).get();

        timestamp = 1503933409806L;

        List<Integer> candidates5 = TableHelper.findSegmentSuccessorCandidates(five,
                indexTable,
                historyTable);

        assertTrue(candidates5.containsAll(Arrays.asList(2, 3, 4, 6, 7)));

        historyTable = TableHelper.completePartialRecordInHistoryTable(historyTable, partial, timestamp);
        historyRecord = HistoryRecord.readLatestRecord(historyTable, false).get();

        candidates5 = TableHelper.findSegmentSuccessorCandidates(five,
                indexTable,
                historyTable);
        assertTrue(candidates5.containsAll(Arrays.asList(2, 3, 4, 6, 7)));

        indexTable = TableHelper.updateIndexTable(indexTable,
                historyRecord.getScaleTime(),
                historyRecord.getOffset());

        candidates5 = TableHelper.findSegmentSuccessorCandidates(five,
                indexTable,
                historyTable);
        assertTrue(candidates5.containsAll(Arrays.asList(2, 3, 4, 6, 7)));

        // scale down 6, 7 -> 8

        // scale down
        timestamp = 1503933560447L;

        Segment eight = new Segment(8, timestamp, six.keyStart, seven.keyEnd);
        segments.add(eight);

        newSegments = Lists.newArrayList(2, 3, 4, 8);

        historyTable = TableHelper.addPartialRecordToHistoryTable(historyTable, newSegments);

        partial = HistoryRecord.readLatestRecord(historyTable, false).get();
        // Notice: segment was created at timestamp but we are recording its entry in history table at timestamp + 10000
        timestamp = 1503933560448L;

        historyTable = TableHelper.completePartialRecordInHistoryTable(historyTable, partial, timestamp);
        historyRecord = HistoryRecord.readLatestRecord(historyTable, false).get();

        indexTable = TableHelper.updateIndexTable(indexTable,
                historyRecord.getScaleTime(),
                historyRecord.getOffset());

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
        Segment zero = new Segment(0, timestamp, 0, 0.2);
        segments.add(zero);
        Segment one = new Segment(1, timestamp, 0.2, 0.4);
        segments.add(one);
        Segment two = new Segment(2, timestamp, 0.4, 0.6);
        segments.add(two);
        Segment three = new Segment(3, timestamp, 0.6, 0.8);
        segments.add(three);
        Segment four = new Segment(4, timestamp, 0.8, 1);
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
        byte[] indexTable = TableHelper.createIndexTable(timestamp, 0);

        int nextHistoryOffset = historyTable.length;

        // 3, 4 -> 5
        newSegments = Lists.newArrayList(0, 1, 2, 5);
        timestamp = timestamp + 1;
        Segment five = new Segment(5, timestamp, 0.6, 1);
        segments.add(five);

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

        indexTable = TableHelper.updateIndexTable(indexTable, timestamp, nextHistoryOffset);
        nextHistoryOffset = historyTable.length;

        // 1 -> 6,7.. 2,5 -> 8
        newSegments = Lists.newArrayList(0, 6, 7, 8);
        timestamp = timestamp + 10;
        Segment six = new Segment(6, timestamp, 0.2, 0.3);
        segments.add(six);
        Segment seven = new Segment(7, timestamp, 0.3, 0.4);
        segments.add(seven);
        Segment eight = new Segment(8, timestamp, 0.4, 1);
        segments.add(eight);

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

        indexTable = TableHelper.updateIndexTable(indexTable, timestamp, nextHistoryOffset);
        nextHistoryOffset = historyTable.length;

        // 7 -> 9,10.. 8 -> 10, 11
        newSegments = Lists.newArrayList(0, 6, 9, 10, 11);
        timestamp = timestamp + 10;
        Segment nine = new Segment(9, timestamp, 0.3, 0.35);
        segments.add(nine);
        Segment ten = new Segment(10, timestamp, 0.35, 0.6);
        segments.add(ten);
        Segment eleven = new Segment(11, timestamp, 0.6, 1);
        segments.add(eleven);

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

        indexTable = TableHelper.updateIndexTable(indexTable, timestamp, nextHistoryOffset);

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
    public void scaleTest() {
        long timestamp = System.currentTimeMillis();
        final List<Integer> startSegments = Lists.newArrayList(0, 1, 2, 3, 4);
        byte[] segmentTable = createSegmentTable(5, timestamp);

        byte[] historyTable = TableHelper.createHistoryTable(timestamp, startSegments);

        // start new scale
        List<Integer> newSegments = Lists.newArrayList(5, 6, 7, 8, 9);
        final double keyRangeChunk = 1.0 / 5;
        final List<AbstractMap.SimpleEntry<Double, Double>> newRanges = IntStream.range(0, 5)
                .boxed()
                .map(x -> new AbstractMap.SimpleEntry<>(x * keyRangeChunk, (x + 1) * keyRangeChunk))
                .collect(Collectors.toList());

        segmentTable = updateSegmentTable(segmentTable, newRanges, timestamp + 1);
        assertTrue(TableHelper.isScaleOngoing(historyTable, segmentTable));
        assertTrue(TableHelper.isRerunOf(startSegments, newRanges, historyTable, segmentTable));

        final double keyRangeChunkInvalid = 1.0 / 5;
        final List<AbstractMap.SimpleEntry<Double, Double>> newRangesInvalid = IntStream.range(0, 2)
                .boxed()
                .map(x -> new AbstractMap.SimpleEntry<>(x * keyRangeChunkInvalid, (x + 1) * keyRangeChunkInvalid))
                .collect(Collectors.toList());
        assertFalse(TableHelper.isRerunOf(Lists.newArrayList(5, 6), newRangesInvalid, historyTable, segmentTable));

        historyTable = TableHelper.addPartialRecordToHistoryTable(historyTable, newSegments);
        assertTrue(TableHelper.isScaleOngoing(historyTable, segmentTable));
        assertTrue(TableHelper.isRerunOf(startSegments, newRanges, historyTable, segmentTable));

        HistoryRecord partial = HistoryRecord.readLatestRecord(historyTable, false).get();
        historyTable = TableHelper.completePartialRecordInHistoryTable(historyTable, partial, timestamp + 2);

        assertFalse(TableHelper.isScaleOngoing(historyTable, segmentTable));
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

    private byte[] createSegmentTable(int numSegments, long eventTime) {
        final double keyRangeChunk = 1.0 / numSegments;

        List<AbstractMap.SimpleEntry<Double, Double>> newRanges = IntStream.range(0, numSegments)
                .boxed()
                .map(x -> new AbstractMap.SimpleEntry<>(x * keyRangeChunk, (x + 1) * keyRangeChunk))
                .collect(Collectors.toList());

        return TableHelper.updateSegmentTable(0, new byte[0], newRanges, eventTime);
    }

    private byte[] updateSegmentTable(byte[] segmentTable, int numSegments, long eventTime) {
        final double keyRangeChunk = 1.0 / numSegments;
        final int startingSegNum = segmentTable.length / SegmentRecord.SEGMENT_RECORD_SIZE;
        List<AbstractMap.SimpleEntry<Double, Double>> newRanges = IntStream.range(0, numSegments)
                .boxed()
                .map(x -> new AbstractMap.SimpleEntry<>(x * keyRangeChunk, (x + 1) * keyRangeChunk))
                .collect(Collectors.toList());

        return updateSegmentTable(segmentTable, newRanges, eventTime);
    }

    private byte[] updateSegmentTable(byte[] segmentTable, List<AbstractMap.SimpleEntry<Double, Double>> newRanges, long eventTime) {
        final int startingSegNum = segmentTable.length / SegmentRecord.SEGMENT_RECORD_SIZE;

        return TableHelper.updateSegmentTable(startingSegNum, segmentTable, newRanges, eventTime);
    }

}


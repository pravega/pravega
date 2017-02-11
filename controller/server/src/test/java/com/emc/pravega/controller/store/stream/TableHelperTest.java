/**
 *  Copyright (c) 2016 Dell Inc. or its subsidiaries. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.controller.store.stream;

import static org.junit.Assert.assertEquals;

import com.emc.pravega.controller.store.stream.tables.SegmentRecord;
import com.emc.pravega.controller.store.stream.tables.TableHelper;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TableHelperTest {
    @Test
    public void getSegmentTest() {
        long time = System.currentTimeMillis();
        byte[] segmentTable = createSegmentTable(5, time);
        assertEquals(segmentTable.length / SegmentRecord.SEGMENT_RECORD_SIZE, 5);

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
        final List<Integer> newSegments = Lists.newArrayList(0, 1, 2, 3, 4);
        long timestamp = System.currentTimeMillis();
        byte[] historyTable = TableHelper.updateHistoryTable(new byte[0], timestamp, newSegments);
        List<Integer> activeSegments = TableHelper.getActiveSegments(historyTable);
        assertEquals(activeSegments, newSegments);

        List<Integer> newSegments2 = Lists.newArrayList(5, 6, 7, 8, 9);

        historyTable = TableHelper.updateHistoryTable(historyTable, System.currentTimeMillis() + 1, newSegments2);
        activeSegments = TableHelper.getActiveSegments(historyTable);
        assertEquals(activeSegments, newSegments2);

        activeSegments = TableHelper.getActiveSegments(timestamp, new byte[0], historyTable);
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

        byte[] historyTable = TableHelper.updateHistoryTable(new byte[0], timestamp, newSegments);
        byte[] indexTable = TableHelper.updateIndexTable(new byte[0], timestamp, 0);

        int nextHistoryOffset = historyTable.length;

        // 3, 4 -> 5
        newSegments = Lists.newArrayList(0, 1, 2, 5);
        timestamp = System.currentTimeMillis() + 1;
        Segment five = new Segment(5, timestamp, 0.6, 1);
        segments.add(five);

        historyTable = TableHelper.updateHistoryTable(historyTable, timestamp, newSegments);
        indexTable = TableHelper.updateIndexTable(indexTable, timestamp, nextHistoryOffset);
        nextHistoryOffset = historyTable.length;

        // 1 -> 6,7.. 2,5 -> 8
        newSegments = Lists.newArrayList(0, 6, 7, 8);
        timestamp = System.currentTimeMillis() + 2;
        Segment six = new Segment(6, timestamp, 0.2, 0.3);
        segments.add(six);
        Segment seven = new Segment(7, timestamp, 0.3, 0.4);
        segments.add(seven);
        Segment eight = new Segment(8, timestamp, 0.4, 1);
        segments.add(eight);

        historyTable = TableHelper.updateHistoryTable(historyTable, timestamp, newSegments);
        indexTable = TableHelper.updateIndexTable(indexTable, timestamp, nextHistoryOffset);
        nextHistoryOffset = historyTable.length;

        // 7 -> 9,10.. 8 -> 10, 11
        newSegments = Lists.newArrayList(0, 6, 9, 10, 11);
        timestamp = System.currentTimeMillis() + 3;
        Segment nine = new Segment(9, timestamp, 0.3, 0.35);
        segments.add(nine);
        Segment ten = new Segment(10, timestamp, 0.35, 0.6);
        segments.add(ten);
        Segment eleven = new Segment(11, timestamp, 0.6, 1);
        segments.add(eleven);

        historyTable = TableHelper.updateHistoryTable(historyTable, timestamp, newSegments);
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
        assertEquals(successors, new ArrayList());

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
        assertEquals(successors, new ArrayList());

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
        assertEquals(successors, new ArrayList());

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
        assertEquals(successors, new ArrayList());
    }

    private byte[] createSegmentTable(int numSegments, long eventTime) {
        final double keyRangeChunk = 1.0 / numSegments;

        List<AbstractMap.SimpleEntry<Double, Double>> newRanges = IntStream.range(0, numSegments)
                .boxed()
                .map(x -> new AbstractMap.SimpleEntry<>(x * keyRangeChunk, (x + 1) * keyRangeChunk))
                .collect(Collectors.toList());

        return TableHelper.updateSegmentTable(0, new byte[0], numSegments, newRanges, eventTime);
    }

    private byte[] updateSegmentTable(byte[] segmentTable, int numSegments, long eventTime) {
        final double keyRangeChunk = 1.0 / numSegments;
        final int startingSegNum = segmentTable.length / SegmentRecord.SEGMENT_RECORD_SIZE;
        List<AbstractMap.SimpleEntry<Double, Double>> newRanges = IntStream.range(0, numSegments)
                .boxed()
                .map(x -> new AbstractMap.SimpleEntry<>(x * keyRangeChunk, (x + 1) * keyRangeChunk))
                .collect(Collectors.toList());

        return TableHelper.updateSegmentTable(startingSegNum, segmentTable, numSegments, newRanges, eventTime);
    }
}


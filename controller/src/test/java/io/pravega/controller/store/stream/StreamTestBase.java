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
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.controller.store.stream.records.CommittingTransactionsRecord;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;
import io.pravega.controller.store.stream.records.StreamTruncationRecord;
import io.pravega.test.common.AssertExtensions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.pravega.shared.segment.StreamSegmentNameUtils.computeSegmentId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class StreamTestBase {
    protected final ScheduledExecutorService executor = Executors.newScheduledThreadPool(5);

    @Before
    public abstract void setup() throws Exception;

    @After
    public abstract void tearDown() throws Exception;

    abstract void createScope(String scope);

    abstract PersistentStreamBase getStream(String scope, String stream);

    private PersistentStreamBase createStream(String scope, String name, long time, int numOfSegments, int startingSegmentNumber) {
        createScope("scope");

        PersistentStreamBase stream = getStream(scope, name);
        StreamConfiguration config = StreamConfiguration.builder().scope(scope).streamName(name)
                                                        .scalingPolicy(ScalingPolicy.fixed(numOfSegments)).build();
        stream.create(config, time, startingSegmentNumber)
              .thenCompose(x -> stream.updateState(State.ACTIVE)).join();

        return stream;
    }

    private void scaleStream(Stream stream, long time, List<Long> sealedSegments,
                             List<Map.Entry<Double, Double>> newRanges, Map<Long, Long> sealedSegmentSizesMap) {
        stream.getEpochTransition()
              .thenCompose(etr -> stream.submitScale(sealedSegments, newRanges, time, etr)
                    .thenCompose(submittedEtr -> stream.getVersionedState()
                       .thenCompose(state -> stream.updateVersionedState(state, State.SCALING))
                       .thenCompose(updatedState -> stream.startScale(true, submittedEtr, updatedState))
                       .thenCompose(startedEtr -> stream.scaleCreateNewEpoch(startedEtr)
                            .thenCompose(x -> stream.scaleOldSegmentsSealed(sealedSegmentSizesMap, startedEtr))
                            .thenCompose(x -> stream.completeScale(startedEtr)))))
              .thenCompose(x -> stream.updateState(State.ACTIVE)).join();
    }

    private UUID createAndCommitTransaction(Stream stream, int msb, long lsb) {
        return stream.generateNewTxnId(msb, lsb)
                     .thenCompose(x -> stream.createTransaction(x, 1000L, 1000L))
                     .thenCompose(y -> stream.sealTransaction(y.getId(), true, Optional.empty())
                                             .thenApply(z -> y.getId())).join();
    }

    @Test
    public void testCreateStream() {
        PersistentStreamBase stream = createStream("scope", "stream", System.currentTimeMillis(), 2, 0);

        assertEquals(State.ACTIVE, stream.getState(true).join());
        EpochRecord activeEpoch = stream.getActiveEpoch(true).join();
        assertEquals(0, activeEpoch.getEpoch());
        assertEquals(2, activeEpoch.getSegments().size());
        VersionedMetadata<StreamTruncationRecord> truncationRecord = stream.getTruncationRecord().join();
        assertEquals(StreamTruncationRecord.EMPTY, truncationRecord.getObject());
        VersionedMetadata<EpochTransitionRecord> etr = stream.getEpochTransition().join();
        assertEquals(EpochTransitionRecord.EMPTY, etr.getObject());

        VersionedMetadata<CommittingTransactionsRecord> ctr = stream.getVersionedCommitTransactionsRecord().join();
        assertEquals(CommittingTransactionsRecord.EMPTY, ctr.getObject());

        assertEquals(activeEpoch, stream.getEpochRecord(0).join());

        AssertExtensions.assertThrows("", stream.getEpochRecord(1),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException);
    }

    @Test
    public void testSegmentQueriesDuringScale() {
        // start scale and perform `getSegment`, `getActiveEpoch` and `getEpoch` during different phases of scale
        int startingSegmentNumber = new Random().nextInt(20);
        Stream stream = createStream("scope", "stream" + startingSegmentNumber, System.currentTimeMillis(),
                5, startingSegmentNumber);

        Segment segment = stream.getSegment(startingSegmentNumber).join();
        assertEquals(segment.segmentId(), startingSegmentNumber + 0L);
        assertEquals(segment.getKeyStart(), 0, 0);
        assertEquals(segment.getKeyEnd(), 1.0 / 5, 0);

        long segment5 = computeSegmentId(startingSegmentNumber + 5, 1);
        long segment6 = computeSegmentId(startingSegmentNumber + 6, 1);
        long segment7 = computeSegmentId(startingSegmentNumber + 7, 1);
        long segment8 = computeSegmentId(startingSegmentNumber + 8, 1);
        long segment9 = computeSegmentId(startingSegmentNumber + 9, 1);
        List<Long> newSegments = Lists.newArrayList(segment5, segment6, segment7, segment8, segment9);

        List<Segment> originalSegments = stream.getActiveSegments().join();
        List<Long> segmentsToSeal = originalSegments.stream().map(Segment::segmentId).collect(Collectors.toList());
        List<Map.Entry<Double, Double>> newRanges = originalSegments.stream().map(x ->
                new AbstractMap.SimpleEntry<>(x.getKeyStart(), x.getKeyEnd())).collect(Collectors.toList());
        VersionedMetadata<EpochTransitionRecord> etr = stream.getEpochTransition().join();

        // submit scale
        etr = stream.submitScale(segmentsToSeal, newRanges, System.currentTimeMillis(), etr).join();
        VersionedMetadata<State> state = stream.getVersionedState()
                                               .thenCompose(s -> stream.updateVersionedState(s, State.SCALING)).join();
        etr = stream.startScale(true, etr, state).join();
        List<Segment> newCurrentSegments = stream.getActiveSegments().join();
        assertEquals(originalSegments, newCurrentSegments);
        AssertExtensions.assertThrows("", () -> stream.getSegment(segment9),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException);

        Map<Segment, List<Long>> successorsWithPredecessors = stream.getSuccessorsWithPredecessors(0L).join();
        assertTrue(successorsWithPredecessors.isEmpty());

        // scale create new epochs
        stream.scaleCreateNewEpoch(etr).join();
        newCurrentSegments = stream.getActiveSegments().join();
        assertEquals(originalSegments, newCurrentSegments);
        segment = stream.getSegment(segment9).join();
        assertEquals(computeSegmentId(startingSegmentNumber + 9, 1), segment.segmentId());
        assertEquals(segment.getKeyStart(), 1.0 / 5 * 4, 0);
        assertEquals(segment.getKeyEnd(), 1.0, 0);

        successorsWithPredecessors = stream.getSuccessorsWithPredecessors(startingSegmentNumber + 0L).join();
        Set<Segment> successors = successorsWithPredecessors.keySet();
        assertEquals(1, successors.size());
        Segment five = successors.stream().findAny().get();
        assertEquals(computeSegmentId(startingSegmentNumber + 5, 1), five.segmentId());
        List<Long> predecessors = successorsWithPredecessors.get(five);
        assertEquals(1, predecessors.size());
        assertTrue(predecessors.contains(startingSegmentNumber + 0L));

        // scale old segments sealed
        stream.scaleOldSegmentsSealed(Collections.emptyMap(), etr).join();
        newCurrentSegments = stream.getActiveSegments().join();
        assertEquals(new HashSet<>(newSegments), newCurrentSegments.stream().map(Segment::segmentId).collect(Collectors.toSet()));

        segment = stream.getSegment(segment9).join();
        assertEquals(computeSegmentId(startingSegmentNumber + 9, 1), segment.segmentId());
        assertEquals(segment.getKeyStart(), 1.0 / 5 * 4, 0);
        assertEquals(segment.getKeyEnd(), 1.0, 0);

        // complete scale
        stream.completeScale(etr).join();
        segment = stream.getSegment(segment9).join();
        assertEquals(computeSegmentId(startingSegmentNumber + 9, 1), segment.segmentId());
        assertEquals(segment.getKeyStart(), 1.0 / 5 * 4, 0);
        assertEquals(segment.getKeyEnd(), 1.0, 0);
    }

    @Test
    public void predecessorAndSuccessorTest() {
        // multiple rows in history table, find predecessor
        // - more than one predecessor
        // - one predecessor
        // - no predecessor
        // - immediate predecessor
        // - predecessor few rows behind
        int startingSegmentNumber = new Random().nextInt(2000);
        Stream stream = createStream("scope", "stream" + startingSegmentNumber, System.currentTimeMillis(), 5, startingSegmentNumber);

        long timestamp = System.currentTimeMillis();
        List<Segment> segments = stream.getEpochRecord(0).join().getSegments().stream()
                                       .map(x -> new Segment(x.segmentId(), x.getCreationTime(), x.getKeyStart(), x.getKeyEnd()))
                                       .collect(Collectors.toList());
        Segment zero = segments.stream().filter(x -> x.segmentId() == startingSegmentNumber + 0L).findAny().get();
        Segment one = segments.stream().filter(x -> x.segmentId() == startingSegmentNumber + 1L).findAny().get();
        Segment two = segments.stream().filter(x -> x.segmentId() == startingSegmentNumber + 2L).findAny().get();
        Segment three = segments.stream().filter(x -> x.segmentId() == startingSegmentNumber + 3L).findAny().get();
        Segment four = segments.stream().filter(x -> x.segmentId() == startingSegmentNumber + 4L).findAny().get();

        // 3, 4 -> 5
        timestamp = timestamp + 1;

        List<Map.Entry<Double, Double>> newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(three.getKeyStart(), four.getKeyEnd()));

        scaleStream(stream, timestamp, Lists.newArrayList(three.segmentId(), four.segmentId()), newRanges, Collections.emptyMap());
        segments = stream.getEpochRecord(1).join().getSegments().stream()
                         .map(x -> new Segment(x.segmentId(), x.getCreationTime(), x.getKeyStart(), x.getKeyEnd()))
                         .collect(Collectors.toList());

        Segment five = segments.stream().filter(x -> x.segmentId() == computeSegmentId(5 + startingSegmentNumber, 1)).findAny().get();

        // 1 -> 6,7.. 2,5 -> 8
        timestamp = timestamp + 10;
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(one.getKeyStart(), 0.3));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.3, one.getKeyEnd()));
        newRanges.add(new AbstractMap.SimpleEntry<>(two.getKeyStart(), five.getKeyEnd()));

        scaleStream(stream, timestamp, Lists.newArrayList(one.segmentId(), two.segmentId(), five.segmentId()), newRanges,
                Collections.emptyMap());

        segments = stream.getEpochRecord(2).join().getSegments().stream()
                         .map(x -> new Segment(x.segmentId(), x.getCreationTime(), x.getKeyStart(), x.getKeyEnd()))
                         .collect(Collectors.toList());

        Segment six = segments.stream().filter(x -> x.segmentId() == computeSegmentId(6 + startingSegmentNumber, 2)).findAny().get();
        Segment seven = segments.stream().filter(x -> x.segmentId() == computeSegmentId(7 + startingSegmentNumber, 2)).findAny().get();
        Segment eight = segments.stream().filter(x -> x.segmentId() == computeSegmentId(8 + startingSegmentNumber, 2)).findAny().get();

        // 7 -> 9,10.. 8 -> 10, 11
        timestamp = timestamp + 10;
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(seven.getKeyStart(), 0.35));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.35, 0.6));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.6, 1.0));

        scaleStream(stream, timestamp, Lists.newArrayList(seven.segmentId(), eight.segmentId()), newRanges, Collections.emptyMap());

        segments = stream.getEpochRecord(3).join().getSegments().stream()
                         .map(x -> new Segment(x.segmentId(), x.getCreationTime(), x.getKeyStart(), x.getKeyEnd()))
                         .collect(Collectors.toList());

        Segment nine = segments.stream().filter(x -> x.segmentId() == computeSegmentId(9 + startingSegmentNumber, 3)).findAny().get();
        Segment ten = segments.stream().filter(x -> x.segmentId() == computeSegmentId(10 + startingSegmentNumber, 3)).findAny().get();
        Segment eleven = segments.stream().filter(x -> x.segmentId() == computeSegmentId(11 + startingSegmentNumber, 3)).findAny().get();

        // 9, 10, 11 -> 12
        timestamp = timestamp + 10;
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.3, 1.0));

        scaleStream(stream, timestamp, Lists.newArrayList(nine.segmentId(), ten.segmentId(), eleven.segmentId()), newRanges,
                Collections.emptyMap());

        segments = stream.getEpochRecord(4).join().getSegments().stream()
                         .map(x -> new Segment(x.segmentId(), x.getCreationTime(), x.getKeyStart(), x.getKeyEnd()))
                         .collect(Collectors.toList());

        Segment twelve = segments.stream().filter(x -> x.segmentId() == computeSegmentId(12 + startingSegmentNumber, 4)).findAny().get();

        // verification
        Map<Segment, List<Long>> successorsWithPredecessors;
        Set<Segment> successors;
        Set<Long> predecessors;

        // segment 0
        successorsWithPredecessors = stream.getSuccessorsWithPredecessors(zero.segmentId()).join();
        assertTrue(successorsWithPredecessors.isEmpty());

        // segment 1 --> (6, <1>), (7, <1>)
        successorsWithPredecessors = stream.getSuccessorsWithPredecessors(one.segmentId()).join();
        successors = successorsWithPredecessors.keySet();
        assertEquals(2, successors.size());
        assertTrue(successors.contains(six));
        predecessors = new HashSet<>(successorsWithPredecessors.get(six));
        assertEquals(1, predecessors.size());
        assertTrue(predecessors.contains(one.segmentId()));
        assertTrue(successors.contains(seven));
        predecessors = new HashSet<>(successorsWithPredecessors.get(seven));
        assertEquals(1, predecessors.size());
        assertTrue(predecessors.contains(one.segmentId()));

        // segment 3 --> (5, <3, 4>)
        successorsWithPredecessors = stream.getSuccessorsWithPredecessors(three.segmentId()).join();
        successors = successorsWithPredecessors.keySet();
        assertEquals(1, successors.size());
        assertTrue(successors.contains(five));
        predecessors = new HashSet<>(successorsWithPredecessors.get(five));
        assertEquals(2, predecessors.size());
        assertTrue(predecessors.contains(three.segmentId()));
        assertTrue(predecessors.contains(four.segmentId()));

        // segment 4 --> (5, <3, 4>)
        successorsWithPredecessors = stream.getSuccessorsWithPredecessors(four.segmentId()).join();
        successors = successorsWithPredecessors.keySet();
        assertEquals(1, successors.size());
        assertTrue(successors.contains(five));
        predecessors = new HashSet<>(successorsWithPredecessors.get(five));
        assertEquals(2, predecessors.size());
        assertTrue(predecessors.contains(three.segmentId()));
        assertTrue(predecessors.contains(four.segmentId()));

        // segment 5 --> (8, <2, 5>)
        successorsWithPredecessors = stream.getSuccessorsWithPredecessors(five.segmentId()).join();
        successors = successorsWithPredecessors.keySet();
        assertEquals(1, successors.size());
        assertTrue(successors.contains(eight));
        predecessors = new HashSet<>(successorsWithPredecessors.get(eight));
        assertEquals(2, predecessors.size());
        assertTrue(predecessors.contains(two.segmentId()));
        assertTrue(predecessors.contains(five.segmentId()));

        // segment 6 --> empty
        successorsWithPredecessors = stream.getSuccessorsWithPredecessors(six.segmentId()).join();
        assertTrue(successorsWithPredecessors.isEmpty());

        // segment 7 --> 9 <7>, 10 <7, 8>
        successorsWithPredecessors = stream.getSuccessorsWithPredecessors(seven.segmentId()).join();
        successors = successorsWithPredecessors.keySet();
        assertEquals(2, successors.size());
        assertTrue(successors.contains(nine));
        predecessors = new HashSet<>(successorsWithPredecessors.get(nine));
        assertEquals(1, predecessors.size());
        assertTrue(predecessors.contains(seven.segmentId()));
        assertTrue(successors.contains(ten));
        predecessors = new HashSet<>(successorsWithPredecessors.get(ten));
        assertEquals(2, predecessors.size());
        assertTrue(predecessors.contains(seven.segmentId()));
        assertTrue(predecessors.contains(eight.segmentId()));

        // segment 8 --> 10 <7, 8>, 11 <8>
        successorsWithPredecessors = stream.getSuccessorsWithPredecessors(eight.segmentId()).join();
        successors = successorsWithPredecessors.keySet();
        assertEquals(2, successors.size());
        assertTrue(successors.contains(ten));
        predecessors = new HashSet<>(successorsWithPredecessors.get(ten));
        assertEquals(2, predecessors.size());
        assertTrue(predecessors.contains(seven.segmentId()));
        assertTrue(predecessors.contains(eight.segmentId()));
        assertTrue(successors.contains(eleven));
        predecessors = new HashSet<>(successorsWithPredecessors.get(eleven));
        assertEquals(1, predecessors.size());
        assertTrue(predecessors.contains(eight.segmentId()));

        // segment 9 --> 12 <9, 10, 11>
        successorsWithPredecessors = stream.getSuccessorsWithPredecessors(nine.segmentId()).join();
        successors = successorsWithPredecessors.keySet();
        assertEquals(1, successors.size());
        assertTrue(successors.contains(twelve));
        predecessors = new HashSet<>(successorsWithPredecessors.get(twelve));
        assertEquals(3, predecessors.size());
        assertTrue(predecessors.contains(nine.segmentId()));
        assertTrue(predecessors.contains(ten.segmentId()));
        assertTrue(predecessors.contains(eleven.segmentId()));

        // segment 10 --> 12 <9, 10, 11>
        successorsWithPredecessors = stream.getSuccessorsWithPredecessors(ten.segmentId()).join();
        successors = successorsWithPredecessors.keySet();
        assertEquals(1, successors.size());
        assertTrue(successors.contains(twelve));
        predecessors = new HashSet<>(successorsWithPredecessors.get(twelve));
        assertEquals(3, predecessors.size());
        assertTrue(predecessors.contains(nine.segmentId()));
        assertTrue(predecessors.contains(ten.segmentId()));
        assertTrue(predecessors.contains(eleven.segmentId()));

        // segment 11 --> 12 <9, 10, 11>
        successorsWithPredecessors = stream.getSuccessorsWithPredecessors(eleven.segmentId()).join();
        successors = successorsWithPredecessors.keySet();
        assertEquals(1, successors.size());
        assertTrue(successors.contains(twelve));
        predecessors = new HashSet<>(successorsWithPredecessors.get(twelve));
        assertEquals(3, predecessors.size());
        assertTrue(predecessors.contains(nine.segmentId()));
        assertTrue(predecessors.contains(ten.segmentId()));
        assertTrue(predecessors.contains(eleven.segmentId()));

        // segment 12 --> empty
        successorsWithPredecessors = stream.getSuccessorsWithPredecessors(twelve.segmentId()).join();
        assertTrue(successorsWithPredecessors.isEmpty());
    }

    @Test
    public void scaleInputValidityTest() {
        int startingSegmentNumber = new Random().nextInt(2000);
        String name = "stream" + startingSegmentNumber;
        PersistentStreamBase stream = createStream("scope", name, System.currentTimeMillis(), 5, startingSegmentNumber);

        long timestamp = System.currentTimeMillis();

        final double keyRangeChunk = 1.0 / 5;
        long s0 = startingSegmentNumber;
        long s1 = 1L + startingSegmentNumber;
        long s2 = 2L + startingSegmentNumber;
        long s3 = 3L + startingSegmentNumber;
        long s4 = 4L + startingSegmentNumber;

        VersionedMetadata<EpochTransitionRecord> etr = stream.getEpochTransition().join();
        List<Map.Entry<Double, Double>> newRanges = new ArrayList<>();
        AtomicReference<List<Map.Entry<Double, Double>>> newRangesRef = new AtomicReference<>(newRanges);
        AtomicReference<VersionedMetadata<EpochTransitionRecord>> etrRef = new AtomicReference<>(etr);

        // 1. empty newRanges
        AssertExtensions.assertThrows("", () -> stream.submitScale(Lists.newArrayList(s0), newRangesRef.get(), timestamp, etrRef.get()),
                e -> Exceptions.unwrap(e) instanceof EpochTransitionOperationExceptions.InputInvalidException);

        // 2. simple mismatch
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, keyRangeChunk));
        AssertExtensions.assertThrows("", () -> stream.submitScale(Lists.newArrayList(s0, s1), newRangesRef.get(), timestamp, etrRef.get()),
                e -> Exceptions.unwrap(e) instanceof EpochTransitionOperationExceptions.InputInvalidException);

        // 3. simple valid match
        newRanges = new ArrayList<>();
        newRangesRef.set(newRanges);
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 2 * keyRangeChunk));
        etr = stream.submitScale(Lists.newArrayList(s0, s1), newRangesRef.get(),
                timestamp, etr).join();
        etr = resetScale(etr, stream);
        etrRef.set(etr);

        // 4. valid 2 disjoint merges
        newRanges = new ArrayList<>();
        newRangesRef.set(newRanges);
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 2 * keyRangeChunk));
        newRanges.add(new AbstractMap.SimpleEntry<>(3 * keyRangeChunk, 1.0));
        etr = stream.submitScale(Lists.newArrayList(s0, s1, s3, s4), newRangesRef.get(), timestamp, etrRef.get()).join();
        etr = resetScale(etr, stream);
        etrRef.set(etr);

        // 5. valid 1 merge and 1 disjoint
        newRanges = new ArrayList<>();
        newRangesRef.set(newRanges);
        newRanges.add(new AbstractMap.SimpleEntry<>(keyRangeChunk, 2 * keyRangeChunk));
        newRanges.add(new AbstractMap.SimpleEntry<>(3 * keyRangeChunk, 1.0));
        etr = stream.submitScale(Lists.newArrayList(s1, s3, s4), newRangesRef.get(), timestamp, etrRef.get()).join();
        etr = resetScale(etr, stream);
        etrRef.set(etr);

        // 6. valid 1 merge, 2 splits
        newRanges = new ArrayList<>();
        newRangesRef.set(newRanges);
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 2 * keyRangeChunk));
        newRanges.add(new AbstractMap.SimpleEntry<>(3 * keyRangeChunk, 0.7));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.7, 0.8));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.8, 0.9));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.9, 1.0));
        etr = stream.submitScale(Lists.newArrayList(s0, s1, s3, s4), newRangesRef.get(), timestamp, etrRef.get()).join();
        etr = resetScale(etr, stream);
        etrRef.set(etr);

        // 7. 1 merge, 1 split and 1 invalid split
        newRanges = new ArrayList<>();
        newRangesRef.set(newRanges);
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 2 * keyRangeChunk));
        newRanges.add(new AbstractMap.SimpleEntry<>(3 * keyRangeChunk, 0.7));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.7, 0.8));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.8, 0.9));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.9, 0.99));
        AssertExtensions.assertThrows("", () -> stream.submitScale(Lists.newArrayList(s0, s1, s3, s4), newRangesRef.get(), timestamp, etrRef.get()),
                e -> Exceptions.unwrap(e) instanceof EpochTransitionOperationExceptions.InputInvalidException);

        // 8. valid unsorted segments to seal
        newRanges = new ArrayList<>();
        newRangesRef.set(newRanges);
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 2 * keyRangeChunk));
        newRanges.add(new AbstractMap.SimpleEntry<>(3 * keyRangeChunk, 0.7));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.7, 0.8));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.8, 0.9));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.9, 1.0));
        etr = stream.submitScale(Lists.newArrayList(s4, s0, s1, s3), newRangesRef.get(), timestamp, etrRef.get()).join();
        etr = resetScale(etr, stream);
        etrRef.set(etr);

        // 9. valid unsorted new ranges
        newRanges = new ArrayList<>();
        newRangesRef.set(newRanges);
        newRanges.add(new AbstractMap.SimpleEntry<>(0.9, 1.0));
        newRanges.add(new AbstractMap.SimpleEntry<>(3 * keyRangeChunk, 0.7));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.7, 0.8));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 2 * keyRangeChunk));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.8, 0.9));
        etr = stream.submitScale(Lists.newArrayList(s4, s0, s1, s3), newRangesRef.get(), timestamp, etrRef.get()).join();
        etr = resetScale(etr, stream);
        etrRef.set(etr);

        // 10. invalid input range low == high
        newRanges = new ArrayList<>();
        newRangesRef.set(newRanges);
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 0.2));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.2));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.4));
        AssertExtensions.assertThrows("", () -> stream.submitScale(Lists.newArrayList(s0, s1), newRangesRef.get(), timestamp, etrRef.get()),
                e -> Exceptions.unwrap(e) instanceof EpochTransitionOperationExceptions.InputInvalidException);

        // 11. invalid input range low > high
        newRanges = new ArrayList<>();
        newRangesRef.set(newRanges);
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 0.2));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.3, 0.2));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.4));
        AssertExtensions.assertThrows("", () -> stream.submitScale(Lists.newArrayList(s0, s1), newRangesRef.get(), timestamp, etrRef.get()),
                e -> Exceptions.unwrap(e) instanceof EpochTransitionOperationExceptions.InputInvalidException);

        // 12. invalid overlapping key ranges
        newRanges = new ArrayList<>();
        newRangesRef.set(newRanges);
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.4));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.3, 3 * keyRangeChunk));
        AssertExtensions.assertThrows("", () -> stream.submitScale(Lists.newArrayList(s1, s2), newRangesRef.get(), timestamp, etrRef.get()),
                e -> Exceptions.unwrap(e) instanceof EpochTransitionOperationExceptions.InputInvalidException);

        // 13. invalid overlapping key ranges -- a contains b
        newRanges = new ArrayList<>();
        newRangesRef.set(newRanges);
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.4));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.3, 0.33));
        AssertExtensions.assertThrows("", () -> stream.submitScale(Lists.newArrayList(s1), newRangesRef.get(), timestamp, etrRef.get()),
                e -> Exceptions.unwrap(e) instanceof EpochTransitionOperationExceptions.InputInvalidException);

        // 14. invalid overlapping key ranges -- b contains a (with b.low == a.low)
        newRanges = new ArrayList<>();
        newRangesRef.set(newRanges);
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.33));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.4));
        AssertExtensions.assertThrows("", () -> stream.submitScale(Lists.newArrayList(s1), newRangesRef.get(), timestamp, etrRef.get()),
                e -> Exceptions.unwrap(e) instanceof EpochTransitionOperationExceptions.InputInvalidException);

        // 15. invalid overlapping key ranges b.low < a.high
        newRanges = new ArrayList<>();
        newRangesRef.set(newRanges);
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.35));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.3, 0.4));
        AssertExtensions.assertThrows("", () -> stream.submitScale(Lists.newArrayList(s1), newRangesRef.get(), timestamp, etrRef.get()),
                e -> Exceptions.unwrap(e) instanceof EpochTransitionOperationExceptions.InputInvalidException);

        // 16. invalid overlapping key ranges.. a.high < b.low
        newRanges = new ArrayList<>();
        newRangesRef.set(newRanges);
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.25));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.3, 0.4));
        AssertExtensions.assertThrows("", () -> stream.submitScale(Lists.newArrayList(s1), newRangesRef.get(), timestamp, etrRef.get()),
                e -> Exceptions.unwrap(e) instanceof EpochTransitionOperationExceptions.InputInvalidException);

        // scale the stream for inconsistent epoch transition 
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 0.4));
        scaleStream(stream, System.currentTimeMillis(), Lists.newArrayList(s0, s1), newRanges, Collections.emptyMap());

        // 17. precondition failure
        newRanges = new ArrayList<>();
        newRangesRef.set(newRanges);
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.4));
        etrRef.set(stream.getEpochTransition().join());
        AssertExtensions.assertThrows("", () -> stream.submitScale(Lists.newArrayList(s1), newRangesRef.get(), timestamp, etrRef.get()),
                e -> Exceptions.unwrap(e) instanceof EpochTransitionOperationExceptions.PreConditionFailureException);
    }

    private VersionedMetadata<EpochTransitionRecord> resetScale(VersionedMetadata<EpochTransitionRecord> etr, Stream stream) {
        stream.completeScale(etr);
        stream.updateState(State.ACTIVE);
        return stream.getEpochTransition().join();
    }

    @Test
    public void segmentQueriesDuringRollingTxn() {
        // start scale and perform `getSegment`, `getActiveEpoch` and `getEpoch` during different phases of scale
        int startingSegmentNumber = new Random().nextInt(2000);
        long time = System.currentTimeMillis();
        Stream stream = createStream("scope", "stream" + startingSegmentNumber, time,
                5, startingSegmentNumber);

        Segment segment = stream.getSegment(startingSegmentNumber).join();
        assertEquals(segment.segmentId(), startingSegmentNumber + 0L);
        assertEquals(segment.getKeyStart(), 0, 0);
        assertEquals(segment.getKeyEnd(), 1.0 / 5, 0);

        createAndCommitTransaction(stream, 0, 0L);

        List<Map.Entry<Double, Double>> newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 0.5));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.5, 1.0));
        time = time + 1;
        scaleStream(stream, time, Lists.newArrayList(startingSegmentNumber + 0L, startingSegmentNumber + 1L,
                startingSegmentNumber + 2L, startingSegmentNumber + 3L, startingSegmentNumber + 4L), newRanges,
                Collections.emptyMap());

        List<Segment> activeSegmentsBefore = stream.getActiveSegments().join();

        // start commit transactions
        VersionedMetadata<CommittingTransactionsRecord> ctr = stream.startCommittingTransactions(0).join();
        stream.getVersionedState().thenCompose(s -> stream.updateVersionedState(s, State.COMMITTING_TXN)).join();

        // start rolling transaction
        ctr = stream.startRollingTxn(1, ctr).join();

        List<Segment> activeSegments1 = stream.getActiveSegments().join();
        assertEquals(activeSegments1, activeSegmentsBefore);

        Map<Segment, List<Long>> successorsWithPredecessors = stream.getSuccessorsWithPredecessors(
                computeSegmentId(startingSegmentNumber + 5, 1)).join();
        assertTrue(successorsWithPredecessors.isEmpty());

        // rolling txn create duplicate epochs. We should be able to get successors and predecessors after this step.
        time = time + 1;
        stream.rollingTxnCreateDuplicateEpochs(Collections.emptyMap(), time, ctr).join();

        activeSegments1 = stream.getActiveSegments().join();
        assertEquals(activeSegments1, activeSegmentsBefore);

        successorsWithPredecessors = stream.getSuccessorsWithPredecessors(computeSegmentId(startingSegmentNumber + 5, 1)).join();
        Set<Segment> successors = successorsWithPredecessors.keySet();
        assertEquals(3, successors.size());
        assertTrue(successors.stream().allMatch(x -> x.getEpoch() == 2));
        assertTrue(successors.stream().anyMatch(x -> x.getNumber() == startingSegmentNumber + 0));
        assertTrue(successors.stream().anyMatch(x -> x.getNumber() == startingSegmentNumber + 1));
        assertTrue(successors.stream().anyMatch(x -> x.getNumber() == startingSegmentNumber + 2));

        successorsWithPredecessors = stream.getSuccessorsWithPredecessors(computeSegmentId(startingSegmentNumber + 0, 2)).join();
        successors = successorsWithPredecessors.keySet();
        assertEquals(1, successors.size());
        assertTrue(successors.stream().allMatch(x -> x.segmentId() == computeSegmentId(startingSegmentNumber + 5, 3)));

        stream.completeRollingTxn(Collections.emptyMap(), ctr).join();
        stream.completeCommittingTransactions(ctr).join();
    }

    @Test
    public void truncationTest() {
        int startingSegmentNumber = new Random().nextInt(2000);
        // epoch 0 --> 0, 1
        long timestamp = System.currentTimeMillis();
        PersistentStreamBase stream = createStream("scope", "stream" + startingSegmentNumber, timestamp, 2, startingSegmentNumber);
        List<Segment> activeSegments = stream.getActiveSegments().join();

        // epoch 1 --> 0, 2, 3
        List<Map.Entry<Double, Double>> newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.5, 0.75));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.75, 1.0));
        Map<Long, Long> map = new HashMap<>();
        map.put(startingSegmentNumber + 1L, 100L);
        scaleStream(stream, ++timestamp, Lists.newArrayList(startingSegmentNumber + 1L), newRanges, map);
        long twoSegmentId = computeSegmentId(startingSegmentNumber + 2, 1);
        long threeSegmentId = computeSegmentId(startingSegmentNumber + 3, 1);
        
        // epoch 2 --> 0, 2, 4, 5
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<Double, Double>(0.75, (0.75 + 1.0) / 2));
        newRanges.add(new AbstractMap.SimpleEntry<Double, Double>((0.75 + 1.0) / 2, 1.0));

        map = new HashMap<>();
        map.put(threeSegmentId, 100L);
        scaleStream(stream, ++timestamp, Lists.newArrayList(threeSegmentId), newRanges, map);
        long fourSegmentId = computeSegmentId(startingSegmentNumber + 4, 2);
        long fiveSegmentId = computeSegmentId(startingSegmentNumber + 5, 2);

        // epoch 3 --> 0, 4, 5, 6, 7
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<Double, Double>(0.5, (0.75 + 0.5) / 2));
        newRanges.add(new AbstractMap.SimpleEntry<Double, Double>((0.75 + 0.5) / 2, 0.75));

        map = new HashMap<>();
        map.put(twoSegmentId, 100L);
        scaleStream(stream, ++timestamp, Lists.newArrayList(twoSegmentId), newRanges, map);

        long sixSegmentId = computeSegmentId(startingSegmentNumber + 6, 3);
        long sevenSegmentId = computeSegmentId(startingSegmentNumber + 7, 3);

        // epoch 4 --> 4, 5, 6, 7, 8, 9
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<Double, Double>(0.0, (0.0 + 0.5) / 2));
        newRanges.add(new AbstractMap.SimpleEntry<Double, Double>((0.0 + 0.5) / 2, 0.5));

        map = new HashMap<>();
        map.put(startingSegmentNumber + 0L, 100L);

        scaleStream(stream, ++timestamp, Lists.newArrayList(startingSegmentNumber + 0L), newRanges, map);

        long eightSegmentId = computeSegmentId(startingSegmentNumber + 8, 4);
        long nineSegmentId = computeSegmentId(startingSegmentNumber + 9, 4);
        
        // first stream cut
        Map<Long, Long> streamCut1 = new HashMap<>();
        streamCut1.put(startingSegmentNumber + 0L, 1L);
        streamCut1.put(startingSegmentNumber + 1L, 1L);
        stream.startTruncation(streamCut1).join();
        VersionedMetadata<StreamTruncationRecord> versionedTruncationRecord = stream.getTruncationRecord().join();
        StreamTruncationRecord truncationRecord = versionedTruncationRecord.getObject();
        assertTrue(truncationRecord.getToDelete().isEmpty());
        assertEquals(truncationRecord.getStreamCut(), streamCut1);
        Map<Long, Integer> transform = transform(truncationRecord.getSpan());
        assertTrue(transform.get(startingSegmentNumber + 0L) == 0 &&
                transform.get(startingSegmentNumber + 1L) == 0);
        stream.completeTruncation(versionedTruncationRecord).join();

        // getActiveSegments wrt first truncation record which is on epoch 0
        Map<Long, Long> activeSegmentsWithOffset;
        // 1. truncationRecord = 0/1, 1/1
        // expected active segments with offset = 0/1, 1/1
        activeSegmentsWithOffset = stream.getSegmentsAtHead().join().entrySet().stream()
                                         .collect(Collectors.toMap(x -> x.getKey().segmentId(), x -> x.getValue()));
        assertTrue(activeSegmentsWithOffset.size() == 2 &&
                activeSegmentsWithOffset.containsKey(startingSegmentNumber + 0L) &&
                activeSegmentsWithOffset.containsKey(startingSegmentNumber + 1L) &&
                activeSegmentsWithOffset.get(startingSegmentNumber + 0L) == 1L &&
                activeSegmentsWithOffset.get(startingSegmentNumber + 1L) == 1L);
        
        // second stream cut
        Map<Long, Long> streamCut2 = new HashMap<>();
        streamCut2.put(startingSegmentNumber + 0L, 1L);
        streamCut2.put(twoSegmentId, 1L);
        streamCut2.put(fourSegmentId, 1L);
        streamCut2.put(fiveSegmentId, 1L);
        stream.startTruncation(streamCut2).join();
        versionedTruncationRecord = stream.getTruncationRecord().join();
        truncationRecord = versionedTruncationRecord.getObject();
        assertEquals(truncationRecord.getStreamCut(), streamCut2);
        assertTrue(truncationRecord.getToDelete().size() == 2
                && truncationRecord.getToDelete().contains(startingSegmentNumber + 1L)
                && truncationRecord.getToDelete().contains(threeSegmentId));
        assertTrue(truncationRecord.getStreamCut().equals(streamCut2));
        transform = transform(truncationRecord.getSpan());
        assertTrue(transform.get(startingSegmentNumber + 0L) == 2 &&
                transform.get(twoSegmentId) == 2 &&
                transform.get(fourSegmentId) == 2 &&
                transform.get(fiveSegmentId) == 2);
        stream.completeTruncation(versionedTruncationRecord).join();

        // 2. truncationRecord = 0/1, 2/1, 4/1, 5/1.
        // expected active segments = 0/1, 2/1, 4/1, 5/1
        activeSegmentsWithOffset = stream.getSegmentsAtHead().join().entrySet().stream()
                                         .collect(Collectors.toMap(x -> x.getKey().segmentId(), x -> x.getValue()));
        assertTrue(activeSegmentsWithOffset.size() == 4 &&
                activeSegmentsWithOffset.containsKey(startingSegmentNumber + 0L) &&
                activeSegmentsWithOffset.containsKey(twoSegmentId) &&
                activeSegmentsWithOffset.containsKey(fourSegmentId) &&
                activeSegmentsWithOffset.containsKey(fiveSegmentId) &&
                activeSegmentsWithOffset.get(startingSegmentNumber + 0L) == 1L &&
                activeSegmentsWithOffset.get(twoSegmentId) == 1L &&
                activeSegmentsWithOffset.get(fourSegmentId) == 1L &&
                activeSegmentsWithOffset.get(fiveSegmentId) == 1L);
        
        // third stream cut
        Map<Long, Long> streamCut3 = new HashMap<>();
        streamCut3.put(twoSegmentId, 10L);
        streamCut3.put(fourSegmentId, 10L);
        streamCut3.put(fiveSegmentId, 10L);
        streamCut3.put(eightSegmentId, 10L);
        streamCut3.put(nineSegmentId, 10L);
        stream.startTruncation(streamCut3).join();
        versionedTruncationRecord = stream.getTruncationRecord().join();
        truncationRecord = versionedTruncationRecord.getObject();
        assertEquals(truncationRecord.getStreamCut(), streamCut3);
        assertTrue(truncationRecord.getToDelete().size() == 1
                && truncationRecord.getToDelete().contains(startingSegmentNumber + 0L));
        assertTrue(truncationRecord.getStreamCut().equals(streamCut3));
        transform = transform(truncationRecord.getSpan());
        assertTrue(transform.get(twoSegmentId) == 2 &&
                transform.get(fourSegmentId) == 4 &&
                transform.get(fiveSegmentId) == 4 &&
                transform.get(eightSegmentId) == 4 &&
                transform.get(nineSegmentId) == 4);
        stream.completeTruncation(versionedTruncationRecord).join();

        // 3. truncation record 2/10, 4/10, 5/10, 8/10, 9/10
        // getActiveSegments wrt first truncation record which spans epoch 2 to 4
        // expected active segments = 2/10, 4/10, 5/10, 8/10, 9/10
        activeSegmentsWithOffset = stream.getSegmentsAtHead().join().entrySet().stream()
                                         .collect(Collectors.toMap(x -> x.getKey().segmentId(), x -> x.getValue()));
        assertTrue(activeSegmentsWithOffset.size() == 5 &&
                activeSegmentsWithOffset.containsKey(twoSegmentId) &&
                activeSegmentsWithOffset.containsKey(fourSegmentId) &&
                activeSegmentsWithOffset.containsKey(fiveSegmentId) &&
                activeSegmentsWithOffset.containsKey(eightSegmentId) &&
                activeSegmentsWithOffset.containsKey(nineSegmentId) &&
                activeSegmentsWithOffset.get(twoSegmentId) == 10L &&
                activeSegmentsWithOffset.get(fourSegmentId) == 10L &&
                activeSegmentsWithOffset.get(fiveSegmentId) == 10L &&
                activeSegmentsWithOffset.get(eightSegmentId) == 10L &&
                activeSegmentsWithOffset.get(nineSegmentId) == 10L);
        
        // behind previous
        Map<Long, Long> streamCut4 = new HashMap<>();
        streamCut4.put(twoSegmentId, 1L);
        streamCut4.put(fourSegmentId, 1L);
        streamCut4.put(fiveSegmentId, 1L);
        streamCut4.put(eightSegmentId, 1L);
        streamCut4.put(nineSegmentId, 1L);
        AssertExtensions.assertThrows("",
                () -> stream.startTruncation(streamCut4), e -> e instanceof IllegalArgumentException);

        Map<Long, Long> streamCut5 = new HashMap<>();
        streamCut3.put(twoSegmentId, 10L);
        streamCut3.put(fourSegmentId, 10L);
        streamCut3.put(fiveSegmentId, 10L);
        streamCut3.put(startingSegmentNumber + 0L, 10L);
        AssertExtensions.assertThrows("",
                () -> stream.startTruncation(streamCut5), e -> e instanceof IllegalArgumentException);
    }

    private Map<Long, Integer> transform(Map<StreamSegmentRecord, Integer> span) {
        return span.entrySet().stream().collect(Collectors.toMap(x -> x.getKey().segmentId(), x -> x.getValue()));
    }

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
    @Test
    public void testStreamCutSegmentsBetween() {
        int startingSegmentNumber = new Random().nextInt(2000);
        List<AbstractMap.SimpleEntry<Double, Double>> newRanges;
        long timestamp = System.currentTimeMillis();

        PersistentStreamBase stream = createStream("scope", "stream" + startingSegmentNumber, timestamp, 3, startingSegmentNumber);

        List<Segment> initialSegments = stream.getActiveSegments().join();
        Segment zero = initialSegments.stream().filter(x -> x.segmentId() == computeSegmentId(startingSegmentNumber + 0, 0)).findAny().get();
        Segment one = initialSegments.stream().filter(x -> x.segmentId() == computeSegmentId(startingSegmentNumber + 1, 0)).findAny().get();
        Segment two = initialSegments.stream().filter(x -> x.segmentId() == computeSegmentId(startingSegmentNumber + 2, 0)).findAny().get();
        Segment three = new Segment(computeSegmentId(startingSegmentNumber + 3, 1), timestamp, 0.0, 0.16);
        Segment four = new Segment(computeSegmentId(startingSegmentNumber + 4, 1), timestamp, 0.16, zero.getKeyEnd());
        Segment five = new Segment(computeSegmentId(startingSegmentNumber + 5, 1), timestamp, one.getKeyStart(), 0.5);
        Segment six = new Segment(computeSegmentId(startingSegmentNumber + 6, 1), timestamp, 0.5, one.getKeyEnd());
        Segment seven = new Segment(computeSegmentId(startingSegmentNumber + 7, 1), timestamp, two.getKeyStart(), 0.83);
        Segment eight = new Segment(computeSegmentId(startingSegmentNumber + 8, 1), timestamp, 0.83, two.getKeyEnd());
        Segment nine = new Segment(computeSegmentId(startingSegmentNumber + 9, 2), timestamp, 0.0, 0.5);
        Segment ten = new Segment(computeSegmentId(startingSegmentNumber + 10, 2), timestamp, 0.5, 1);

        // 2 -> 7, 8
        // 1 -> 5, 6
        // 0 -> 3, 4
        LinkedList<Segment> newsegments = new LinkedList<>();
        newsegments.add(three);
        newsegments.add(four);
        newsegments.add(five);
        newsegments.add(six);
        newsegments.add(seven);
        newsegments.add(eight);
        List<Long> segmentsToSeal = stream.getActiveSegments().join().stream().map(x -> x.segmentId()).collect(Collectors.toList());
        newRanges = newsegments.stream()
                            .map(x -> new AbstractMap.SimpleEntry<>(x.getKeyStart(), x.getKeyEnd())).collect(Collectors.toList());

        scaleStream(stream, ++timestamp, segmentsToSeal, new LinkedList<>(newRanges), Collections.emptyMap());

        // 6, 7, 8 -> 10
        // 3, 4, 5 -> 9
        newsegments = new LinkedList<>();
        newsegments.add(nine);
        newsegments.add(ten);
        segmentsToSeal = stream.getActiveSegments().join().stream().map(x -> x.segmentId()).collect(Collectors.toList());

        newRanges = newsegments.stream()
                            .map(x -> new AbstractMap.SimpleEntry<>(x.getKeyStart(), x.getKeyEnd())).collect(Collectors.toList());
        scaleStream(stream, ++timestamp, segmentsToSeal, new LinkedList<>(newRanges), Collections.emptyMap());

        // only from
        Map<Long, Long> fromStreamCut = new HashMap<>();
        fromStreamCut.put(zero.segmentId(), 0L);
        fromStreamCut.put(one.segmentId(), 0L);
        fromStreamCut.put(two.segmentId(), 0L);

        List<Segment> segmentsBetween = stream.getSegmentsBetweenStreamCuts(fromStreamCut, Collections.emptyMap()).join();
        assertEquals(11, segmentsBetween.size());

        fromStreamCut = new HashMap<>();
        fromStreamCut.put(zero.segmentId(), 0L);
        fromStreamCut.put(two.segmentId(), 0L);
        fromStreamCut.put(five.segmentId(), 0L);
        fromStreamCut.put(six.segmentId(), 0L);
        segmentsBetween = stream.getSegmentsBetweenStreamCuts(fromStreamCut, Collections.emptyMap()).join();
        assertEquals(10, segmentsBetween.size());
        assertTrue(segmentsBetween.stream().noneMatch(x -> x.segmentId() == one.segmentId()));

        fromStreamCut = new HashMap<>();
        fromStreamCut.put(zero.segmentId(), 0L);
        fromStreamCut.put(five.segmentId(), 0L);
        fromStreamCut.put(ten.segmentId(), 0L);
        segmentsBetween = stream.getSegmentsBetweenStreamCuts(fromStreamCut, Collections.emptyMap()).join();
        assertEquals(6, segmentsBetween.size());
        // 0, 3, 4, 5, 9, 10
        assertTrue(segmentsBetween.stream().noneMatch(x -> x.segmentId() == one.segmentId() || x.segmentId() == two.segmentId()
                || x.segmentId() == six.segmentId() || x.segmentId() == seven.segmentId() || x.segmentId() == eight.segmentId()));

        fromStreamCut = new HashMap<>();
        fromStreamCut.put(six.segmentId(), 0L);
        fromStreamCut.put(seven.segmentId(), 0L);
        fromStreamCut.put(eight.segmentId(), 0L);
        fromStreamCut.put(nine.segmentId(), 0L);
        segmentsBetween = stream.getSegmentsBetweenStreamCuts(fromStreamCut, Collections.emptyMap()).join();
        assertEquals(5, segmentsBetween.size());
        assertTrue(segmentsBetween.stream().noneMatch(x -> x.segmentId() == one.segmentId() || x.segmentId() == two.segmentId()
                || x.segmentId() == three.segmentId() || x.segmentId() == four.segmentId() || x.segmentId() == five.segmentId()));

        fromStreamCut = new HashMap<>();
        fromStreamCut.put(ten.segmentId(), 0L);
        fromStreamCut.put(nine.segmentId(), 0L);
        segmentsBetween = stream.getSegmentsBetweenStreamCuts(fromStreamCut, Collections.emptyMap()).join();
        assertEquals(2, segmentsBetween.size());
        assertTrue(segmentsBetween.stream().noneMatch(x -> x.segmentId() == one.segmentId() || x.segmentId() == two.segmentId()
                || x.segmentId() == three.segmentId() || x.segmentId() == four.segmentId() || x.segmentId() == five.segmentId()
                || x.segmentId() == six.segmentId() || x.segmentId() == seven.segmentId() || x.segmentId() == eight.segmentId()));

        // to before from
        fromStreamCut = new HashMap<>();
        fromStreamCut.put(three.segmentId(), 0L);
        fromStreamCut.put(four.segmentId(), 0L);
        fromStreamCut.put(one.segmentId(), 0L);
        fromStreamCut.put(two.segmentId(), 0L);

        Map<Long, Long> toStreamCut = new HashMap<>();
        toStreamCut.put(zero.segmentId(), 0L);
        toStreamCut.put(one.segmentId(), 0L);
        toStreamCut.put(two.segmentId(), 0L);
        Map<Long, Long> fromStreamCutCopy = fromStreamCut;
        AssertExtensions.assertThrows("", 
                () -> stream.getSegmentsBetweenStreamCuts(fromStreamCutCopy, toStreamCut), 
                e -> Exceptions.unwrap(e) instanceof IllegalArgumentException);

        // to and from overlap
        Map<Long, Long> fromStreamCutOverlap = new HashMap<>();
        fromStreamCutOverlap.put(three.segmentId(), 0L);
        fromStreamCutOverlap.put(four.segmentId(), 0L);
        fromStreamCutOverlap.put(one.segmentId(), 0L);
        fromStreamCutOverlap.put(two.segmentId(), 0L);

        Map<Long, Long> toStreamCutOverlap = new HashMap<>();
        toStreamCutOverlap.put(zero.segmentId(), 0L);
        toStreamCutOverlap.put(five.segmentId(), 0L);
        toStreamCutOverlap.put(six.segmentId(), 0L);
        toStreamCutOverlap.put(two.segmentId(), 0L);
        AssertExtensions.assertThrows("",
                () -> stream.getSegmentsBetweenStreamCuts(fromStreamCutOverlap, toStreamCutOverlap),
                e -> Exceptions.unwrap(e) instanceof IllegalArgumentException);

        Map<Long, Long> fromPartialOverlap = new HashMap<>();
        fromPartialOverlap.put(zero.segmentId(), 0L);
        fromPartialOverlap.put(five.segmentId(), 0L);
        fromPartialOverlap.put(six.segmentId(), 0L);
        fromPartialOverlap.put(two.segmentId(), 0L);

        Map<Long, Long> toPartialOverlap = new HashMap<>();
        toPartialOverlap.put(eight.segmentId(), 0L);
        toPartialOverlap.put(seven.segmentId(), 0L);
        toPartialOverlap.put(one.segmentId(), 0L);
        toPartialOverlap.put(three.segmentId(), 0L);
        toPartialOverlap.put(four.segmentId(), 0L);
        AssertExtensions.assertThrows("",
                () -> stream.getSegmentsBetweenStreamCuts(fromPartialOverlap, toPartialOverlap),
                e -> Exceptions.unwrap(e) instanceof IllegalArgumentException);

        // Success cases
        Map<Long, Long> fromStreamCutSuccess = new HashMap<>();
        fromStreamCutSuccess.put(zero.segmentId(), 0L);
        fromStreamCutSuccess.put(one.segmentId(), 0L);
        fromStreamCutSuccess.put(two.segmentId(), 0L);

        Map<Long, Long> toStreamCutSuccess = new HashMap<>();
        toStreamCutSuccess.put(zero.segmentId(), 0L);
        toStreamCutSuccess.put(five.segmentId(), 0L);
        toStreamCutSuccess.put(six.segmentId(), 0L);
        toStreamCutSuccess.put(two.segmentId(), 0L);
        segmentsBetween = stream.getSegmentsBetweenStreamCuts(fromStreamCutSuccess, toStreamCutSuccess).join();
        assertEquals(5, segmentsBetween.size());
        assertTrue(segmentsBetween.stream().allMatch(x -> x.segmentId() == zero.segmentId() || x.segmentId() == one.segmentId()
                || x.segmentId() == two.segmentId() || x.segmentId() == five.segmentId() || x.segmentId() == six.segmentId()));

        fromStreamCutSuccess = new HashMap<>();
        fromStreamCutSuccess.put(zero.segmentId(), 0L);
        fromStreamCutSuccess.put(five.segmentId(), 0L);
        fromStreamCutSuccess.put(six.segmentId(), 0L);
        fromStreamCutSuccess.put(two.segmentId(), 0L);

        toStreamCutSuccess = new HashMap<>();
        toStreamCutSuccess.put(nine.segmentId(), 0L);
        toStreamCutSuccess.put(ten.segmentId(), 0L);
        segmentsBetween = stream.getSegmentsBetweenStreamCuts(fromStreamCutSuccess, toStreamCutSuccess).join();
        assertEquals(10, segmentsBetween.size());
        assertTrue(segmentsBetween.stream().noneMatch(x -> x.segmentId() == one.segmentId()));

        // empty from
        toStreamCutSuccess = new HashMap<>();
        toStreamCutSuccess.put(zero.segmentId(), 0L);
        toStreamCutSuccess.put(five.segmentId(), 0L);
        toStreamCutSuccess.put(six.segmentId(), 0L);
        toStreamCutSuccess.put(two.segmentId(), 0L);
        segmentsBetween = stream.getSegmentsBetweenStreamCuts(Collections.emptyMap(), toStreamCutSuccess).join();
        assertEquals(5, segmentsBetween.size());
        assertTrue(segmentsBetween.stream().noneMatch(x -> x.segmentId() == three.segmentId() || x.segmentId() == four.segmentId()
                || x.segmentId() == seven.segmentId() || x.segmentId() == eight.segmentId() || x.segmentId() == nine.segmentId()
                || x.segmentId() == ten.segmentId()));

    }
}

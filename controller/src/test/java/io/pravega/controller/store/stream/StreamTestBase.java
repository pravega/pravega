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
package io.pravega.controller.store.stream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.TestOperationContext;
import io.pravega.controller.store.Version;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.stream.records.ActiveTxnRecord;
import io.pravega.controller.store.stream.records.CommittingTransactionsRecord;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;
import io.pravega.controller.store.stream.records.HistoryTimeSeries;
import io.pravega.controller.store.stream.records.SealedSegmentsMapShard;
import io.pravega.controller.store.stream.records.StreamConfigurationRecord;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;
import io.pravega.controller.store.stream.records.StreamTruncationRecord;
import io.pravega.controller.store.stream.records.WriterMark;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.AssertExtensions;
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.shared.NameUtils.computeSegmentId;
import static io.pravega.shared.NameUtils.getEpoch;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public abstract class StreamTestBase {
    protected final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(5, "test");

    @Before
    public abstract void setup() throws Exception;

    @After
    public void tearDownExecutor() {
        ExecutorServiceHelpers.shutdown(executor);
    }

    @After
    public abstract void tearDown() throws Exception;
    
    abstract void createScope(String scope, OperationContext context);

    abstract PersistentStreamBase getStream(String scope, String stream, int chunkSize, int shardSize);

    protected OperationContext getContext() {
        return new TestOperationContext();
    }

    private PersistentStreamBase createStream(String scope, String name, long time, int numOfSegments, int startingSegmentNumber,
                                              int chunkSize, int shardSize) {
        OperationContext context = getContext();

        createScope(scope, context);

        PersistentStreamBase stream = getStream(scope, name, chunkSize, shardSize);
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.fixed(numOfSegments)).build();
        stream.create(config, time, startingSegmentNumber, context)
              .thenCompose(x -> stream.updateState(State.ACTIVE, context)).join();

        // set minimum number of segments to 1 so that we can also test scale downs
        stream.startUpdateConfiguration(StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build(), 
                context).join();
        VersionedMetadata<StreamConfigurationRecord> configRecord = stream.getVersionedConfigurationRecord(context).join();
        stream.completeUpdateConfiguration(configRecord, context).join();

        return stream;
    }
    
    private PersistentStreamBase createStream(String scope, String name, long time, int numOfSegments, int startingSegmentNumber) {
        return createStream(scope, name, time, numOfSegments, startingSegmentNumber, HistoryTimeSeries.HISTORY_CHUNK_SIZE, 
                SealedSegmentsMapShard.SHARD_SIZE);
    }

    private void scaleStream(Stream stream, long time, List<Long> sealedSegments,
                             List<Map.Entry<Double, Double>> newRanges, Map<Long, Long> sealedSegmentSizesMap) {
        OperationContext context = getContext();

        stream.getEpochTransition(context)
              .thenCompose(etr -> stream.submitScale(sealedSegments, newRanges, time, etr, context)
                    .thenCompose(submittedEtr -> stream.getVersionedState(context)
                       .thenCompose(state -> stream.updateVersionedState(state, State.SCALING, context))
                       .thenCompose(updatedState -> stream.startScale(true, submittedEtr, updatedState, context))
                       .thenCompose(startedEtr -> stream.scaleCreateNewEpoch(startedEtr, context)
                            .thenCompose(x -> stream.scaleOldSegmentsSealed(sealedSegmentSizesMap, startedEtr, context))
                            .thenCompose(x -> stream.completeScale(startedEtr, context)))))
              .thenCompose(x -> stream.updateState(State.ACTIVE, context)).join();
    }

    private UUID createAndCommitTransaction(Stream stream, int msb, long lsb) {
        OperationContext context = getContext();
        return stream.generateNewTxnId(msb, lsb, context)
                     .thenCompose(x -> stream.createTransaction(x, 1000L, 1000L, context))
                     .thenCompose(y -> stream.sealTransaction(y.getId(), true, Optional.empty(), "", 
                             Long.MIN_VALUE, context)
                                             .thenApply(z -> y.getId())).join();
    }

    private void rollTransactions(Stream stream, long time, int epoch, int activeEpoch, Map<Long, Long> txnSizeMap, Map<Long, Long> activeSizeMap) {
        OperationContext context = getContext();
        stream.startCommittingTransactions(100, context)
                                        .thenCompose(ctr ->
                                                stream.getVersionedState(context)
                                                      .thenCompose(state -> stream.updateVersionedState(state, 
                                                              State.COMMITTING_TXN, context))
                                                .thenCompose(state -> stream.startRollingTxn(activeEpoch, ctr.getKey(), context)
                                                        .thenCompose(ctr2 -> stream.rollingTxnCreateDuplicateEpochs(
                                                                txnSizeMap, time, ctr2, context)
                                                        .thenCompose(v -> stream.completeRollingTxn(activeSizeMap, ctr2, context))
                                                                .thenCompose(v -> stream.completeCommittingTransactions(ctr2, context, Collections.emptyMap()))
                                        )))
              .thenCompose(x -> stream.updateState(State.ACTIVE, context)).join();
    }

    @Test(timeout = 30000L)
    public void testCreateStream() {
        OperationContext context = getContext();

        PersistentStreamBase stream = createStream("scope", "stream", System.currentTimeMillis(), 2, 0);

        assertEquals(State.ACTIVE, stream.getState(true, context).join());
        EpochRecord activeEpoch = stream.getActiveEpoch(true, context).join();
        assertEquals(0, activeEpoch.getEpoch());
        assertEquals(2, activeEpoch.getSegments().size());
        VersionedMetadata<StreamTruncationRecord> truncationRecord = stream.getTruncationRecord(context).join();
        assertEquals(StreamTruncationRecord.EMPTY, truncationRecord.getObject());
        VersionedMetadata<EpochTransitionRecord> etr = stream.getEpochTransition(context).join();
        assertEquals(EpochTransitionRecord.EMPTY, etr.getObject());

        VersionedMetadata<CommittingTransactionsRecord> ctr = stream.getVersionedCommitTransactionsRecord(context).join();
        assertEquals(CommittingTransactionsRecord.EMPTY, ctr.getObject());

        assertEquals(activeEpoch, stream.getEpochRecord(0, context).join());

        AssertExtensions.assertFutureThrows("", stream.getEpochRecord(1, context),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException);
    }

    @Test(timeout = 30000L)
    public void testSegmentQueriesDuringScale() {
        OperationContext context = getContext();

        // start scale and perform `getSegment`, `getActiveEpoch` and `getEpoch` during different phases of scale
        int startingSegmentNumber = new Random().nextInt(20);
        Stream stream = createStream("scope", "stream" + startingSegmentNumber, System.currentTimeMillis(),
                5, startingSegmentNumber);

        StreamSegmentRecord segment = stream.getSegment(startingSegmentNumber, context).join();
        assertEquals(segment.segmentId(), startingSegmentNumber + 0L);
        assertEquals(segment.getKeyStart(), 0, 0);
        assertEquals(segment.getKeyEnd(), 1.0 / 5, 0);

        long segment5 = computeSegmentId(startingSegmentNumber + 5, 1);
        long segment6 = computeSegmentId(startingSegmentNumber + 6, 1);
        long segment7 = computeSegmentId(startingSegmentNumber + 7, 1);
        long segment8 = computeSegmentId(startingSegmentNumber + 8, 1);
        long segment9 = computeSegmentId(startingSegmentNumber + 9, 1);
        List<Long> newSegments = Lists.newArrayList(segment5, segment6, segment7, segment8, segment9);

        List<StreamSegmentRecord> originalSegments = stream.getActiveSegments(context).join();
        List<Long> segmentsToSeal = originalSegments.stream().map(StreamSegmentRecord::segmentId).collect(Collectors.toList());
        List<Map.Entry<Double, Double>> newRanges = originalSegments.stream().map(x ->
                new AbstractMap.SimpleEntry<>(x.getKeyStart(), x.getKeyEnd())).collect(Collectors.toList());
        VersionedMetadata<EpochTransitionRecord> etr = stream.getEpochTransition(context).join();

        // submit scale
        etr = stream.submitScale(segmentsToSeal, newRanges, System.currentTimeMillis(), etr, context).join();
        VersionedMetadata<State> state = stream.getVersionedState(context)
                                               .thenCompose(s -> stream.updateVersionedState(s, State.SCALING, context)).join();
        etr = stream.startScale(true, etr, state, context).join();
        List<StreamSegmentRecord> newCurrentSegments = stream.getActiveSegments(context).join();
        assertEquals(originalSegments, newCurrentSegments);
        AssertExtensions.assertSuppliedFutureThrows("", () -> stream.getSegment(segment9, context),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException);

        Map<StreamSegmentRecord, List<Long>> successorsWithPredecessors = stream.getSuccessorsWithPredecessors(
                0L, context).join();
        assertTrue(successorsWithPredecessors.isEmpty());

        // scale create new epochs
        stream.scaleCreateNewEpoch(etr, context).join();
        newCurrentSegments = stream.getActiveSegments(context).join();
        assertEquals(originalSegments, newCurrentSegments);
        segment = stream.getSegment(segment9, context).join();
        assertEquals(computeSegmentId(startingSegmentNumber + 9, 1), segment.segmentId());
        assertEquals(segment.getKeyStart(), 1.0 / 5 * 4, 0);
        assertEquals(segment.getKeyEnd(), 1.0, 0);

        successorsWithPredecessors = stream.getSuccessorsWithPredecessors(startingSegmentNumber + 0L, context).join();
        Set<StreamSegmentRecord> successors = successorsWithPredecessors.keySet();
        assertEquals(1, successors.size());
        StreamSegmentRecord five = successors.stream().findAny().get();
        assertEquals(computeSegmentId(startingSegmentNumber + 5, 1), five.segmentId());
        List<Long> predecessors = successorsWithPredecessors.get(five);
        assertEquals(1, predecessors.size());
        assertTrue(predecessors.contains(startingSegmentNumber + 0L));

        // scale old segments sealed
        stream.scaleOldSegmentsSealed(Collections.emptyMap(), etr, context).join();
        newCurrentSegments = stream.getActiveSegments(context).join();
        assertEquals(new HashSet<>(newSegments), newCurrentSegments.stream().map(StreamSegmentRecord::segmentId)
                                                                   .collect(Collectors.toSet()));

        segment = stream.getSegment(segment9, context).join();
        assertEquals(computeSegmentId(startingSegmentNumber + 9, 1), segment.segmentId());
        assertEquals(segment.getKeyStart(), 1.0 / 5 * 4, 0);
        assertEquals(segment.getKeyEnd(), 1.0, 0);

        // complete scale
        stream.completeScale(etr, context).join();
        segment = stream.getSegment(segment9, context).join();
        assertEquals(computeSegmentId(startingSegmentNumber + 9, 1), segment.segmentId());
        assertEquals(segment.getKeyStart(), 1.0 / 5 * 4, 0);
        assertEquals(segment.getKeyEnd(), 1.0, 0);
    }

    @Test(timeout = 30000L)
    public void predecessorAndSuccessorTest() {
        // multiple rows in history table, find predecessor
        // - more than one predecessor
        // - one predecessor
        // - no predecessor
        // - immediate predecessor
        // - predecessor few rows behind
        OperationContext context = getContext();
        int startingSegmentNumber = new Random().nextInt(2000);
        Stream stream = createStream("scope", "stream" + startingSegmentNumber, System.currentTimeMillis(), 
                5, startingSegmentNumber);

        long timestamp = System.currentTimeMillis();
        List<StreamSegmentRecord> segments = stream.getEpochRecord(0, context).join().getSegments();
        StreamSegmentRecord zero = segments.stream().filter(x -> x.segmentId() == startingSegmentNumber + 0L).findAny().get();
        StreamSegmentRecord one = segments.stream().filter(x -> x.segmentId() == startingSegmentNumber + 1L).findAny().get();
        StreamSegmentRecord two = segments.stream().filter(x -> x.segmentId() == startingSegmentNumber + 2L).findAny().get();
        StreamSegmentRecord three = segments.stream().filter(x -> x.segmentId() == startingSegmentNumber + 3L).findAny().get();
        StreamSegmentRecord four = segments.stream().filter(x -> x.segmentId() == startingSegmentNumber + 4L).findAny().get();

        // 3, 4 -> 5
        timestamp = timestamp + 1;

        List<Map.Entry<Double, Double>> newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(three.getKeyStart(), four.getKeyEnd()));

        scaleStream(stream, timestamp, Lists.newArrayList(three.segmentId(), four.segmentId()), newRanges, Collections.emptyMap());
        segments = stream.getEpochRecord(1, context).join().getSegments();

        StreamSegmentRecord five = segments.stream().filter(x -> x.segmentId() == computeSegmentId(
                5 + startingSegmentNumber, 1)).findAny().get();

        // 1 -> 6,7.. 2,5 -> 8
        timestamp = timestamp + 10;
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(one.getKeyStart(), 0.3));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.3, one.getKeyEnd()));
        newRanges.add(new AbstractMap.SimpleEntry<>(two.getKeyStart(), five.getKeyEnd()));

        scaleStream(stream, timestamp, Lists.newArrayList(one.segmentId(), two.segmentId(), five.segmentId()), newRanges,
                Collections.emptyMap());

        segments = stream.getEpochRecord(2, context).join().getSegments();

        StreamSegmentRecord six = segments.stream().filter(x -> x.segmentId() == computeSegmentId(
                6 + startingSegmentNumber, 2)).findAny().get();
        StreamSegmentRecord seven = segments.stream().filter(x -> x.segmentId() == computeSegmentId(
                7 + startingSegmentNumber, 2)).findAny().get();
        StreamSegmentRecord eight = segments.stream().filter(x -> x.segmentId() == computeSegmentId(
                8 + startingSegmentNumber, 2)).findAny().get();

        // 7 -> 9,10.. 8 -> 10, 11
        timestamp = timestamp + 10;
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(seven.getKeyStart(), 0.35));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.35, 0.6));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.6, 1.0));

        scaleStream(stream, timestamp, Lists.newArrayList(seven.segmentId(), eight.segmentId()), newRanges, Collections.emptyMap());

        segments = stream.getEpochRecord(3, context).join().getSegments();
        StreamSegmentRecord nine = segments.stream().filter(x -> x.segmentId() == computeSegmentId(
                9 + startingSegmentNumber, 3)).findAny().get();
        StreamSegmentRecord ten = segments.stream().filter(x -> x.segmentId() == computeSegmentId(
                10 + startingSegmentNumber, 3)).findAny().get();
        StreamSegmentRecord eleven = segments.stream().filter(x -> x.segmentId() == computeSegmentId(
                11 + startingSegmentNumber, 3)).findAny().get();

        // 9, 10, 11 -> 12
        timestamp = timestamp + 10;
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.3, 1.0));

        scaleStream(stream, timestamp, Lists.newArrayList(nine.segmentId(), ten.segmentId(), eleven.segmentId()), newRanges,
                Collections.emptyMap());

        segments = stream.getEpochRecord(4, context).join().getSegments();

        StreamSegmentRecord twelve = segments.stream().filter(x -> x.segmentId() == computeSegmentId(
                12 + startingSegmentNumber, 4)).findAny().get();

        // verification
        Map<StreamSegmentRecord, List<Long>> successorsWithPredecessors;
        Set<StreamSegmentRecord> successors;
        Set<Long> predecessors;

        // StreamSegmentRecord 0
        successorsWithPredecessors = stream.getSuccessorsWithPredecessors(zero.segmentId(), context).join();
        assertTrue(successorsWithPredecessors.isEmpty());

        // segment 1 --> (6, <1>), (7, <1>)
        successorsWithPredecessors = stream.getSuccessorsWithPredecessors(one.segmentId(), context).join();
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
        successorsWithPredecessors = stream.getSuccessorsWithPredecessors(three.segmentId(), context).join();
        successors = successorsWithPredecessors.keySet();
        assertEquals(1, successors.size());
        assertTrue(successors.contains(five));
        predecessors = new HashSet<>(successorsWithPredecessors.get(five));
        assertEquals(2, predecessors.size());
        assertTrue(predecessors.contains(three.segmentId()));
        assertTrue(predecessors.contains(four.segmentId()));

        // segment 4 --> (5, <3, 4>)
        successorsWithPredecessors = stream.getSuccessorsWithPredecessors(four.segmentId(), context).join();
        successors = successorsWithPredecessors.keySet();
        assertEquals(1, successors.size());
        assertTrue(successors.contains(five));
        predecessors = new HashSet<>(successorsWithPredecessors.get(five));
        assertEquals(2, predecessors.size());
        assertTrue(predecessors.contains(three.segmentId()));
        assertTrue(predecessors.contains(four.segmentId()));

        // segment 5 --> (8, <2, 5>)
        successorsWithPredecessors = stream.getSuccessorsWithPredecessors(five.segmentId(), context).join();
        successors = successorsWithPredecessors.keySet();
        assertEquals(1, successors.size());
        assertTrue(successors.contains(eight));
        predecessors = new HashSet<>(successorsWithPredecessors.get(eight));
        assertEquals(2, predecessors.size());
        assertTrue(predecessors.contains(two.segmentId()));
        assertTrue(predecessors.contains(five.segmentId()));

        // segment 6 --> empty
        successorsWithPredecessors = stream.getSuccessorsWithPredecessors(six.segmentId(), context).join();
        assertTrue(successorsWithPredecessors.isEmpty());

        // segment 7 --> 9 <7>, 10 <7, 8>
        successorsWithPredecessors = stream.getSuccessorsWithPredecessors(seven.segmentId(), context).join();
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
        successorsWithPredecessors = stream.getSuccessorsWithPredecessors(eight.segmentId(), context).join();
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
        successorsWithPredecessors = stream.getSuccessorsWithPredecessors(nine.segmentId(), context).join();
        successors = successorsWithPredecessors.keySet();
        assertEquals(1, successors.size());
        assertTrue(successors.contains(twelve));
        predecessors = new HashSet<>(successorsWithPredecessors.get(twelve));
        assertEquals(3, predecessors.size());
        assertTrue(predecessors.contains(nine.segmentId()));
        assertTrue(predecessors.contains(ten.segmentId()));
        assertTrue(predecessors.contains(eleven.segmentId()));

        // segment 10 --> 12 <9, 10, 11>
        successorsWithPredecessors = stream.getSuccessorsWithPredecessors(ten.segmentId(), context).join();
        successors = successorsWithPredecessors.keySet();
        assertEquals(1, successors.size());
        assertTrue(successors.contains(twelve));
        predecessors = new HashSet<>(successorsWithPredecessors.get(twelve));
        assertEquals(3, predecessors.size());
        assertTrue(predecessors.contains(nine.segmentId()));
        assertTrue(predecessors.contains(ten.segmentId()));
        assertTrue(predecessors.contains(eleven.segmentId()));

        // segment 11 --> 12 <9, 10, 11>
        successorsWithPredecessors = stream.getSuccessorsWithPredecessors(eleven.segmentId(), context).join();
        successors = successorsWithPredecessors.keySet();
        assertEquals(1, successors.size());
        assertTrue(successors.contains(twelve));
        predecessors = new HashSet<>(successorsWithPredecessors.get(twelve));
        assertEquals(3, predecessors.size());
        assertTrue(predecessors.contains(nine.segmentId()));
        assertTrue(predecessors.contains(ten.segmentId()));
        assertTrue(predecessors.contains(eleven.segmentId()));

        // segment 12 --> empty
        successorsWithPredecessors = stream.getSuccessorsWithPredecessors(twelve.segmentId(), context).join();
        assertTrue(successorsWithPredecessors.isEmpty());
    }

    @Test(timeout = 30000L)
    public void scaleInputValidityTest() {
        OperationContext context = getContext();
        int startingSegmentNumber = new Random().nextInt(2000);
        String name = "stream" + startingSegmentNumber;
        PersistentStreamBase stream = createStream("scope", name, System.currentTimeMillis(), 
                5, startingSegmentNumber);

        long timestamp = System.currentTimeMillis();

        final double keyRangeChunk = 1.0 / 5;
        long s0 = startingSegmentNumber;
        long s1 = 1L + startingSegmentNumber;
        long s2 = 2L + startingSegmentNumber;
        long s3 = 3L + startingSegmentNumber;
        long s4 = 4L + startingSegmentNumber;

        VersionedMetadata<EpochTransitionRecord> etr = stream.getEpochTransition(context).join();
        List<Map.Entry<Double, Double>> newRanges = new ArrayList<>();
        AtomicReference<List<Map.Entry<Double, Double>>> newRangesRef = new AtomicReference<>(newRanges);
        AtomicReference<VersionedMetadata<EpochTransitionRecord>> etrRef = new AtomicReference<>(etr);

        // 1. empty newRanges
        AssertExtensions.assertSuppliedFutureThrows("", () -> stream.submitScale(Lists.newArrayList(s0), 
                newRangesRef.get(), timestamp, etrRef.get(), context),
                e -> Exceptions.unwrap(e) instanceof EpochTransitionOperationExceptions.InputInvalidException);

        // 2. simple mismatch
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, keyRangeChunk));
        AssertExtensions.assertSuppliedFutureThrows("", () -> stream.submitScale(
                Lists.newArrayList(s0, s1), newRangesRef.get(), timestamp, etrRef.get(), context),
                e -> Exceptions.unwrap(e) instanceof EpochTransitionOperationExceptions.InputInvalidException);

        // 3. simple valid match
        newRanges = new ArrayList<>();
        newRangesRef.set(newRanges);
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 2 * keyRangeChunk));
        etr = stream.submitScale(Lists.newArrayList(s0, s1), newRangesRef.get(),
                timestamp, etr, context).join();
        etr = resetScale(etr, stream);
        etrRef.set(etr);

        // 4. valid 2 disjoint merges
        newRanges = new ArrayList<>();
        newRangesRef.set(newRanges);
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 2 * keyRangeChunk));
        newRanges.add(new AbstractMap.SimpleEntry<>(3 * keyRangeChunk, 1.0));
        etr = stream.submitScale(Lists.newArrayList(s0, s1, s3, s4), newRangesRef.get(), timestamp, etrRef.get(), context).join();
        etr = resetScale(etr, stream);
        etrRef.set(etr);

        // 5. valid 1 merge and 1 disjoint
        newRanges = new ArrayList<>();
        newRangesRef.set(newRanges);
        newRanges.add(new AbstractMap.SimpleEntry<>(keyRangeChunk, 2 * keyRangeChunk));
        newRanges.add(new AbstractMap.SimpleEntry<>(3 * keyRangeChunk, 1.0));
        etr = stream.submitScale(Lists.newArrayList(s1, s3, s4), newRangesRef.get(), timestamp, etrRef.get(), context).join();
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
        etr = stream.submitScale(Lists.newArrayList(s0, s1, s3, s4), newRangesRef.get(), timestamp, etrRef.get(), context).join();
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
        AssertExtensions.assertSuppliedFutureThrows("", () -> stream.submitScale(
                Lists.newArrayList(s0, s1, s3, s4), newRangesRef.get(), timestamp, etrRef.get(), context),
                e -> Exceptions.unwrap(e) instanceof EpochTransitionOperationExceptions.InputInvalidException);

        // 8. valid unsorted segments to seal
        newRanges = new ArrayList<>();
        newRangesRef.set(newRanges);
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 2 * keyRangeChunk));
        newRanges.add(new AbstractMap.SimpleEntry<>(3 * keyRangeChunk, 0.7));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.7, 0.8));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.8, 0.9));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.9, 1.0));
        etr = stream.submitScale(Lists.newArrayList(s4, s0, s1, s3), newRangesRef.get(), timestamp, etrRef.get(), context).join();
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
        etr = stream.submitScale(Lists.newArrayList(s4, s0, s1, s3), newRangesRef.get(), timestamp, etrRef.get(), context).join();
        etr = resetScale(etr, stream);
        etrRef.set(etr);

        // 10. invalid input range low == high
        newRanges = new ArrayList<>();
        newRangesRef.set(newRanges);
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 0.2));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.2));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.4));
        AssertExtensions.assertSuppliedFutureThrows("", () -> stream.submitScale(
                Lists.newArrayList(s0, s1), newRangesRef.get(), timestamp, etrRef.get(), context),
                e -> Exceptions.unwrap(e) instanceof EpochTransitionOperationExceptions.InputInvalidException);

        // 11. invalid input range low > high
        newRanges = new ArrayList<>();
        newRangesRef.set(newRanges);
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 0.2));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.3, 0.2));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.4));
        AssertExtensions.assertSuppliedFutureThrows("", () -> stream.submitScale(
                Lists.newArrayList(s0, s1), newRangesRef.get(), timestamp, etrRef.get(), context),
                e -> Exceptions.unwrap(e) instanceof EpochTransitionOperationExceptions.InputInvalidException);

        // 12. invalid overlapping key ranges
        newRanges = new ArrayList<>();
        newRangesRef.set(newRanges);
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.4));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.3, 3 * keyRangeChunk));
        AssertExtensions.assertSuppliedFutureThrows("", () -> stream.submitScale(
                Lists.newArrayList(s1, s2), newRangesRef.get(), timestamp, etrRef.get(), context),
                e -> Exceptions.unwrap(e) instanceof EpochTransitionOperationExceptions.InputInvalidException);

        // 13. invalid overlapping key ranges -- a contains b
        newRanges = new ArrayList<>();
        newRangesRef.set(newRanges);
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.4));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.3, 0.33));
        AssertExtensions.assertSuppliedFutureThrows("", () -> stream.submitScale(
                Lists.newArrayList(s1), newRangesRef.get(), timestamp, etrRef.get(), context),
                e -> Exceptions.unwrap(e) instanceof EpochTransitionOperationExceptions.InputInvalidException);

        // 14. invalid overlapping key ranges -- b contains a (with b.low == a.low)
        newRanges = new ArrayList<>();
        newRangesRef.set(newRanges);
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.33));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.4));
        AssertExtensions.assertSuppliedFutureThrows("", () -> stream.submitScale(
                Lists.newArrayList(s1), newRangesRef.get(), timestamp, etrRef.get(), context),
                e -> Exceptions.unwrap(e) instanceof EpochTransitionOperationExceptions.InputInvalidException);

        // 15. invalid overlapping key ranges b.low < a.high
        newRanges = new ArrayList<>();
        newRangesRef.set(newRanges);
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.35));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.3, 0.4));
        AssertExtensions.assertSuppliedFutureThrows("", () -> stream.submitScale(
                Lists.newArrayList(s1), newRangesRef.get(), timestamp, etrRef.get(), context),
                e -> Exceptions.unwrap(e) instanceof EpochTransitionOperationExceptions.InputInvalidException);

        // 16. invalid overlapping key ranges.. a.high < b.low
        newRanges = new ArrayList<>();
        newRangesRef.set(newRanges);
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.25));
        newRanges.add(new AbstractMap.SimpleEntry<>(0.3, 0.4));
        AssertExtensions.assertSuppliedFutureThrows("", () -> stream.submitScale(
                Lists.newArrayList(s1), newRangesRef.get(), timestamp, etrRef.get(), context),
                e -> Exceptions.unwrap(e) instanceof EpochTransitionOperationExceptions.InputInvalidException);

        // scale the stream for inconsistent epoch transition 
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 0.4));
        scaleStream(stream, System.currentTimeMillis(), Lists.newArrayList(s0, s1), newRanges, Collections.emptyMap());

        // 17. precondition failure
        newRanges = new ArrayList<>();
        newRangesRef.set(newRanges);
        newRanges.add(new AbstractMap.SimpleEntry<>(0.2, 0.4));
        etrRef.set(stream.getEpochTransition(context).join());
        AssertExtensions.assertSuppliedFutureThrows("", () -> stream.submitScale(
                Lists.newArrayList(s1), newRangesRef.get(), timestamp, etrRef.get(), context),
                e -> Exceptions.unwrap(e) instanceof EpochTransitionOperationExceptions.PreConditionFailureException);

        etrRef.set(stream.getEpochTransition(context).join());

        // get current number of segments.
        List<Long> segments = stream.getActiveSegments(context).join().stream()
                                    .map(StreamSegmentRecord::segmentId).collect(Collectors.toList());

        // set minimum number of segments to segments.size. 
        stream.startUpdateConfiguration(
                StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(segments.size())).build(), context).join();
        VersionedMetadata<StreamConfigurationRecord> configRecord = stream.getVersionedConfigurationRecord(context).join();
        stream.completeUpdateConfiguration(configRecord, context).join();

        // attempt a scale down which should be rejected in submit scale. 
        newRanges = new ArrayList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 1.0));
        newRangesRef.set(newRanges);

        AssertExtensions.assertSuppliedFutureThrows("", () -> stream.submitScale(segments, newRangesRef.get(), 
                timestamp, etrRef.get(), context),
                e -> Exceptions.unwrap(e) instanceof EpochTransitionOperationExceptions.PreConditionFailureException);
    }
    
    private VersionedMetadata<EpochTransitionRecord> resetScale(VersionedMetadata<EpochTransitionRecord> etr, Stream stream) {
        OperationContext context = getContext();
        stream.completeScale(etr, context).join();
        stream.updateState(State.ACTIVE, context).join();
        return stream.getEpochTransition(context).join();
    }

    @Test(timeout = 30000L)
    public void segmentQueriesDuringRollingTxn() {
        OperationContext context = getContext();

        // start scale and perform `getSegment`, `getActiveEpoch` and `getEpoch` during different phases of scale
        int startingSegmentNumber = new Random().nextInt(2000);
        long time = System.currentTimeMillis();
        Stream stream = createStream("scope", "stream" + startingSegmentNumber, time,
                5, startingSegmentNumber);

        StreamSegmentRecord segment = stream.getSegment(startingSegmentNumber, context).join();
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

        List<StreamSegmentRecord> activeSegmentsBefore = stream.getActiveSegments(context).join();

        // start commit transactions
        VersionedMetadata<CommittingTransactionsRecord> ctr = stream.startCommittingTransactions(100, context).join().getKey();
        stream.getVersionedState(context).thenCompose(s -> stream.updateVersionedState(s, State.COMMITTING_TXN, context)).join();

        // start rolling transaction
        ctr = stream.startRollingTxn(1, ctr, context).join();

        List<StreamSegmentRecord> activeSegments1 = stream.getActiveSegments(context).join();
        assertEquals(activeSegments1, activeSegmentsBefore);

        Map<StreamSegmentRecord, List<Long>> successorsWithPredecessors = stream.getSuccessorsWithPredecessors(
                computeSegmentId(startingSegmentNumber + 5, 1), context).join();
        assertTrue(successorsWithPredecessors.isEmpty());

        // rolling txn create duplicate epochs. We should be able to get successors and predecessors after this step.
        time = time + 1;
        stream.rollingTxnCreateDuplicateEpochs(Collections.emptyMap(), time, ctr, context).join();

        activeSegments1 = stream.getActiveSegments(context).join();
        assertEquals(activeSegments1, activeSegmentsBefore);

        successorsWithPredecessors = stream.getSuccessorsWithPredecessors(computeSegmentId(
                startingSegmentNumber + 5, 1), context).join();
        Set<StreamSegmentRecord> successors = successorsWithPredecessors.keySet();
        assertEquals(3, successors.size());
        assertTrue(successors.stream().allMatch(x -> x.getCreationEpoch() == 2));
        assertTrue(successors.stream().anyMatch(x -> x.getSegmentNumber() == startingSegmentNumber + 0));
        assertTrue(successors.stream().anyMatch(x -> x.getSegmentNumber() == startingSegmentNumber + 1));
        assertTrue(successors.stream().anyMatch(x -> x.getSegmentNumber() == startingSegmentNumber + 2));

        successorsWithPredecessors = stream.getSuccessorsWithPredecessors(computeSegmentId(
                startingSegmentNumber + 0, 2), context).join();
        successors = successorsWithPredecessors.keySet();
        assertEquals(1, successors.size());
        assertTrue(successors.stream().allMatch(x -> x.segmentId() == computeSegmentId(
                startingSegmentNumber + 5, 3)));

        stream.completeRollingTxn(Collections.emptyMap(), ctr, context).join();
        stream.completeCommittingTransactions(ctr, context, Collections.emptyMap()).join();
    }

    @Test(timeout = 30000L)
    public void truncationTest() {
        OperationContext context = getContext();
        int startingSegmentNumber = new Random().nextInt(2000);
        // epoch 0 --> 0, 1
        long timestamp = System.currentTimeMillis();
        PersistentStreamBase stream = createStream("scope", "stream" + startingSegmentNumber, timestamp,
                2, startingSegmentNumber);
        List<StreamSegmentRecord> activeSegments = stream.getActiveSegments(context).join();

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
        stream.startTruncation(streamCut1, context).join();
        VersionedMetadata<StreamTruncationRecord> versionedTruncationRecord = stream.getTruncationRecord(context).join();
        StreamTruncationRecord truncationRecord = versionedTruncationRecord.getObject();
        assertTrue(truncationRecord.getToDelete().isEmpty());
        assertEquals(truncationRecord.getStreamCut(), streamCut1);
        Map<Long, Integer> transform = transform(truncationRecord.getSpan());
        assertTrue(transform.get(startingSegmentNumber + 0L) == 0 &&
                transform.get(startingSegmentNumber + 1L) == 0);
        stream.completeTruncation(versionedTruncationRecord, context).join();

        // getActiveSegments wrt first truncation record which is on epoch 0
        Map<Long, Long> activeSegmentsWithOffset;
        // 1. truncationRecord = 0/1, 1/1
        // expected active segments with offset = 0/1, 1/1
        activeSegmentsWithOffset = stream.getSegmentsAtHead(context).join().entrySet().stream()
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
        stream.startTruncation(streamCut2, context).join();
        versionedTruncationRecord = stream.getTruncationRecord(context).join();
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
        stream.completeTruncation(versionedTruncationRecord, context).join();

        // 2. truncationRecord = 0/1, 2/1, 4/1, 5/1.
        // expected active segments = 0/1, 2/1, 4/1, 5/1
        activeSegmentsWithOffset = stream.getSegmentsAtHead(context).join().entrySet().stream()
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
        stream.startTruncation(streamCut3, context).join();
        versionedTruncationRecord = stream.getTruncationRecord(context).join();
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
        stream.completeTruncation(versionedTruncationRecord, context).join();

        // 3. truncation record 2/10, 4/10, 5/10, 8/10, 9/10
        // getActiveSegments wrt first truncation record which spans epoch 2 to 4
        // expected active segments = 2/10, 4/10, 5/10, 8/10, 9/10
        activeSegmentsWithOffset = stream.getSegmentsAtHead(context).join().entrySet().stream()
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
        AssertExtensions.assertSuppliedFutureThrows("",
                () -> stream.startTruncation(streamCut4, context), e -> e instanceof IllegalArgumentException);

        Map<Long, Long> streamCut5 = new HashMap<>();
        streamCut5.put(twoSegmentId, 10L);
        streamCut5.put(fourSegmentId, 10L);
        streamCut5.put(fiveSegmentId, 10L);
        streamCut5.put(startingSegmentNumber + 0L, 10L);
        AssertExtensions.assertSuppliedFutureThrows("",
                () -> stream.startTruncation(streamCut5, context), e -> e instanceof IllegalArgumentException);
    }

    private Map<Long, Integer> transform(Map<StreamSegmentRecord, Integer> span) {
        return span.entrySet().stream().collect(Collectors.toMap(x -> x.getKey().segmentId(), x -> x.getValue()));
    }

    // region multiple chunks test
    private PersistentStreamBase createScaleAndRollStreamForMultiChunkTests(String name, String scope, int startingSegmentNumber, Supplier<Long> time) {
        OperationContext context = getContext();
        createScope(scope, context);
        PersistentStreamBase stream = createStream(scope, name, time.get(), 5, startingSegmentNumber, 2, 2);
        UUID txnId = createAndCommitTransaction(stream, 0, 0L);
        // scale the stream 5 times so that over all we have 6 epochs and hence 3 chunks.  
        for (int i = 0; i < 5; i++) {
            StreamSegmentRecord first = stream.getActiveSegments(context).join().get(0);
            ArrayList<Long> sealedSegments = Lists.newArrayList(first.segmentId());
            List<Map.Entry<Double, Double>> newRanges = new LinkedList<>();
            newRanges.add(new AbstractMap.SimpleEntry<>(first.getKeyStart(), first.getKeyEnd()));
            Map<Long, Long> sealedSizeMap = new HashMap<>();
            sealedSizeMap.put(first.segmentId(), 100L);
            scaleStream(stream, time.get(), sealedSegments, newRanges, sealedSizeMap);
        }

        EpochRecord activeEpoch = stream.getActiveEpoch(true, context).join();
        // now roll transaction so that we have 2 more epochs added for overall 8 epochs and 4 chunks 
        Map<Long, Long> map1 = stream.getEpochRecord(0, context).join().getSegmentIds().stream()
                                     .collect(Collectors.toMap(x -> computeSegmentId(NameUtils.getSegmentNumber(x),
                                             activeEpoch.getEpoch() + 1), x -> 100L));
        Map<Long, Long> map2 = activeEpoch.getSegmentIds().stream()
                                     .collect(Collectors.toMap(x -> x, x -> 100L));

        rollTransactions(stream, time.get(), 0, activeEpoch.getEpoch(), map1, map2);

        // scale the stream 5 times so that over all we have 13 epochs and hence 7 chunks.  
        for (int i = 0; i < 5; i++) {
            StreamSegmentRecord first = stream.getActiveSegments(context).join().get(0);
            ArrayList<Long> sealedSegments = Lists.newArrayList(first.segmentId());
            List<Map.Entry<Double, Double>> newRanges = new LinkedList<>();
            newRanges.add(new AbstractMap.SimpleEntry<>(first.getKeyStart(), first.getKeyEnd()));
            Map<Long, Long> sealedSizeMap = new HashMap<>();
            sealedSizeMap.put(first.segmentId(), 100L);
            scaleStream(stream, time.get(), sealedSegments, newRanges, sealedSizeMap);
        }
        return stream;
    }

    /**
     * Stream history.
     * epoch0 = 0, 1, 2, 3, 4
     * epoch1 = 5, 1, 2, 3, 4
     * epoch2 = 5, 6, 2, 3, 4
     * epoch3 = 5, 6, 7, 3, 4
     * epoch4 = 5, 6, 7, 8, 4
     * epoch5 = 5, 6, 7, 8, 9
     * epoch6 = 0`, 1`, 2`, 3`, 4`
     * epoch7 = 5`, 6`, 7`, 8`, 9`
     * epoch8 = 10, 6`, 7`, 8`, 9`
     * epoch9 = 10, 11, 7`, 8`, 9`
     * epoch10 = 10, 11, 12, 8`, 9`
     * epoch11 = 10, 11, 12, 13, 9` 
     * epoch12 = 10, 11, 12, 13, 14
     */
    @Test(timeout = 30000L)
    public void testFetchEpochs() {
        String scope = "fetchEpoch";
        String name = "fetchEpoch";
        PersistentStreamBase stream = createScaleAndRollStreamForMultiChunkTests(name, scope, 
                new Random().nextInt(2000), System::currentTimeMillis);

        OperationContext context = getContext();
        List<EpochRecord> epochs = stream.fetchEpochs(0, 12, true, context).join();
        assertEquals(13, epochs.size());
        assertTrue(epochs.stream().anyMatch(x -> x.getEpoch() == 0));
        assertTrue(epochs.stream().anyMatch(x -> x.getEpoch() == 1));
        assertTrue(epochs.stream().anyMatch(x -> x.getEpoch() == 2));
        assertTrue(epochs.stream().anyMatch(x -> x.getEpoch() == 3));
        assertTrue(epochs.stream().anyMatch(x -> x.getEpoch() == 4));
        assertTrue(epochs.stream().anyMatch(x -> x.getEpoch() == 5));
        assertTrue(epochs.stream().anyMatch(x -> x.getEpoch() == 6));
        assertTrue(epochs.stream().anyMatch(x -> x.getEpoch() == 7));
        assertTrue(epochs.stream().anyMatch(x -> x.getEpoch() == 8));
        assertTrue(epochs.stream().anyMatch(x -> x.getEpoch() == 9));
        assertTrue(epochs.stream().anyMatch(x -> x.getEpoch() == 10));
        assertTrue(epochs.stream().anyMatch(x -> x.getEpoch() == 11));
        assertTrue(epochs.stream().anyMatch(x -> x.getEpoch() == 12));
        
        epochs = stream.fetchEpochs(12, 13, true, context).join();
        assertEquals(1, epochs.size());
        assertEquals(stream.getEpochRecord(12, context).join(), epochs.get(0));

        // now try to fetch an epoch that will fall in a chunk that does not exist
        AssertExtensions.assertFutureThrows("", stream.fetchEpochs(12, 14, true, context),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException);
    }

    /**
     * Stream history.
     * epoch0 = 0, 1, 2, 3, 4
     * epoch1 = 5, 1, 2, 3, 4
     * epoch2 = 5, 6, 2, 3, 4
     * epoch3 = 5, 6, 7, 3, 4
     * epoch4 = 5, 6, 7, 8, 4
     * epoch5 = 5, 6, 7, 8, 9
     * epoch6 = 0`, 1`, 2`, 3`, 4`
     * epoch7 = 5`, 6`, 7`, 8`, 9`
     * epoch8 = 10, 6`, 7`, 8`, 9`
     * epoch9 = 10, 11, 7`, 8`, 9`
     * epoch10 = 10, 11, 12, 8`, 9`
     * epoch11 = 10, 11, 12, 13, 9` 
     * epoch12 = 10, 11, 12, 13, 14
     */
    @Test(timeout = 30000L)
    public void testFindEpochAtTime() {
        String scope = "findEpochsAtTime";
        String name = "findEpochsAtTime";
        AtomicLong timeFunc = new AtomicLong(100L);
        PersistentStreamBase stream = createScaleAndRollStreamForMultiChunkTests(name, scope, new Random().nextInt(2000), timeFunc::incrementAndGet);
        OperationContext context = getContext();
        List<EpochRecord> epochs = stream.fetchEpochs(0, 12, true, context).join();
        int epoch = stream.findEpochAtTime(0L, true, context).join();
        assertEquals(0, epoch);
        epoch = stream.findEpochAtTime(101L, true, context).join();
        assertEquals(0, epoch);
        epoch = stream.findEpochAtTime(102L, true, context).join();
        assertEquals(1, epoch);
        epoch = stream.findEpochAtTime(103L, true, context).join();
        assertEquals(2, epoch);
        epoch = stream.findEpochAtTime(104L, true, context).join();
        assertEquals(3, epoch);
        epoch = stream.findEpochAtTime(105L, true, context).join();
        assertEquals(4, epoch);
        epoch = stream.findEpochAtTime(106L, true, context).join();
        assertEquals(5, epoch);
        epoch = stream.findEpochAtTime(107L, true, context).join();
        assertEquals(6, epoch);
        epoch = stream.findEpochAtTime(108L, true, context).join();
        assertEquals(7, epoch);
        epoch = stream.findEpochAtTime(109L, true, context).join();
        assertEquals(8, epoch);
        epoch = stream.findEpochAtTime(110L, true, context).join();
        assertEquals(9, epoch);
        epoch = stream.findEpochAtTime(111L, true, context).join();
        assertEquals(10, epoch);
        epoch = stream.findEpochAtTime(112L, true, context).join();
        assertEquals(11, epoch);
        epoch = stream.findEpochAtTime(113L, true, context).join();
        assertEquals(12, epoch);
        epoch = stream.findEpochAtTime(114L, true, context).join();
        assertEquals(12, epoch);
        epoch = stream.findEpochAtTime(1000L, true, context).join();
        assertEquals(12, epoch);
    }

    /**
     * Stream history.
     * epoch0 = 0, 1, 2, 3, 4
     * epoch1 = 5, 1, 2, 3, 4
     * epoch2 = 5, 6, 2, 3, 4
     * epoch3 = 5, 6, 7, 3, 4
     * epoch4 = 5, 6, 7, 8, 4
     * epoch5 = 5, 6, 7, 8, 9
     * epoch6 = 0`, 1`, 2`, 3`, 4`
     * epoch7 = 5`, 6`, 7`, 8`, 9`
     * epoch8 = 10, 6`, 7`, 8`, 9`
     * epoch9 = 10, 11, 7`, 8`, 9`
     * epoch10 = 10, 11, 12, 8`, 9`
     * epoch11 = 10, 11, 12, 13, 9` 
     * epoch12 = 10, 11, 12, 13, 14
     */
    @Test(timeout = 30000L)
    public void testSealedSegmentSizesMapShards() {
        String scope = "sealedSizeTest";
        String name = "sealedSizeTest";
        int startingSegmentNumber = new Random().nextInt(2000);
        PersistentStreamBase stream = createScaleAndRollStreamForMultiChunkTests(name, scope, startingSegmentNumber, System::currentTimeMillis);
        OperationContext context = getContext();
        SealedSegmentsMapShard shard0 = stream.getSealedSegmentSizeMapShard(0, context).join();
        // 5 segments created in epoch 0 and 1 segment in epoch 1
        assertTrue(shard0.getSealedSegmentsSizeMap().keySet().stream().allMatch(x -> getEpoch(x) == 0 || getEpoch(x) == 1));
        assertEquals(5, shard0.getSealedSegmentsSizeMap().keySet().stream().filter(x -> getEpoch(x) == 0)
                              .collect(Collectors.toList()).size());
        assertEquals(1, shard0.getSealedSegmentsSizeMap().keySet().stream().filter(x -> getEpoch(x) == 1)
                              .collect(Collectors.toList()).size());

        // 1 segment created in epoch 2 and 1 segment in epoch 3
        SealedSegmentsMapShard shard1 = stream.getSealedSegmentSizeMapShard(1, context).join();
        assertTrue(shard1.getSealedSegmentsSizeMap().keySet().stream().allMatch(x -> getEpoch(x) == 2 || getEpoch(x) == 3));
        assertEquals(1, shard1.getSealedSegmentsSizeMap().keySet().stream().filter(x -> getEpoch(x) == 2)
                              .collect(Collectors.toList()).size());
        assertEquals(1, shard1.getSealedSegmentsSizeMap().keySet().stream().filter(x -> getEpoch(x) == 3)
                              .collect(Collectors.toList()).size());

        // 1 segment created in epoch 3 and 1 segment in epoch 4
        SealedSegmentsMapShard shard2 = stream.getSealedSegmentSizeMapShard(2, context).join();
        assertTrue(shard2.getSealedSegmentsSizeMap().keySet().stream().allMatch(x -> getEpoch(x) == 4 || getEpoch(x) == 5));
        assertEquals(1, shard2.getSealedSegmentsSizeMap().keySet().stream().filter(x -> getEpoch(x) == 4).count());
        assertEquals(1, shard2.getSealedSegmentsSizeMap().keySet().stream().filter(x -> getEpoch(x) == 5).count());

        // rolling transaction, 10 segments created across two epochs mapped to the shard
        SealedSegmentsMapShard shard3 = stream.getSealedSegmentSizeMapShard(3, context).join();
        assertTrue(shard3.getSealedSegmentsSizeMap().keySet().stream().allMatch(x -> getEpoch(x) == 6 || getEpoch(x) == 7));
        assertEquals(5, shard3.getSealedSegmentsSizeMap().keySet().stream().filter(x -> getEpoch(x) == 6).count());
        assertEquals(5, shard3.getSealedSegmentsSizeMap().keySet().stream().filter(x -> getEpoch(x) == 7).count());

        // 1 segment created in epoch 8 and 1 segment in epoch 9 but they are not sealed yet
        SealedSegmentsMapShard shard4 = stream.getSealedSegmentSizeMapShard(4, context).join();
        assertTrue(shard4.getSealedSegmentsSizeMap().isEmpty());

        // 1 segment created in epoch 10 and 1 segment in epoch 11 but they are not sealed yet
        SealedSegmentsMapShard shard5 = stream.getSealedSegmentSizeMapShard(5, context).join();
        assertTrue(shard5.getSealedSegmentsSizeMap().isEmpty());

        // 1 segment created in epoch 12 but nothing is sealed yet
        SealedSegmentsMapShard shard6 = stream.getSealedSegmentSizeMapShard(6, context).join();
        assertTrue(shard6.getSealedSegmentsSizeMap().isEmpty());

        // now seal all of them again
        EpochRecord activeEpoch = stream.getActiveEpoch(true, context).join();
        List<Map.Entry<Double, Double>> newRanges = new LinkedList<>();
        newRanges.add(new AbstractMap.SimpleEntry<>(0.0, 1.0));
        Map<Long, Long> sealedSizeMap = new HashMap<>();
        activeEpoch.getSegments().forEach(x -> sealedSizeMap.put(x.segmentId(), 100L));
        scaleStream(stream, System.currentTimeMillis(), Lists.newArrayList(activeEpoch.getSegmentIds()), newRanges, sealedSizeMap);

        // 1 segment created in epoch 8 and 1 segment in epoch 9 
        shard4 = stream.getSealedSegmentSizeMapShard(4, context).join();
        assertTrue(shard4.getSealedSegmentsSizeMap().keySet().stream().allMatch(x -> getEpoch(x) == 8 || getEpoch(x) == 9));
        assertEquals(1, shard4.getSealedSegmentsSizeMap().keySet().stream().filter(x -> getEpoch(x) == 8).collect(Collectors.toList()).size());
        assertEquals(1, shard4.getSealedSegmentsSizeMap().keySet().stream().filter(x -> getEpoch(x) == 9).collect(Collectors.toList()).size());

        // 1 segment created in epoch 10 and 1 segment in epoch 11 
        shard5 = stream.getSealedSegmentSizeMapShard(5, context).join();
        assertTrue(shard5.getSealedSegmentsSizeMap().keySet().stream().allMatch(x -> getEpoch(x) == 10 || getEpoch(x) == 11));
        assertEquals(1, shard5.getSealedSegmentsSizeMap().keySet().stream().filter(x -> getEpoch(x) == 10).collect(Collectors.toList()).size());
        assertEquals(1, shard5.getSealedSegmentsSizeMap().keySet().stream().filter(x -> getEpoch(x) == 11).collect(Collectors.toList()).size());

        // 1 segment created in epoch 12 
        shard6 = stream.getSealedSegmentSizeMapShard(6, context).join();
        assertTrue(shard6.getSealedSegmentsSizeMap().keySet().stream().allMatch(x -> getEpoch(x) == 12));
        assertEquals(1, shard6.getSealedSegmentsSizeMap().keySet().stream().filter(x -> getEpoch(x) == 12).collect(Collectors.toList()).size());

    }

    /**
     * Stream history.
     * epoch0 = 0, 1, 2, 3, 4
     * epoch1 = 5, 1, 2, 3, 4
     * epoch2 = 5, 6, 2, 3, 4
     * epoch3 = 5, 6, 7, 3, 4
     * epoch4 = 5, 6, 7, 8, 4
     * epoch5 = 5, 6, 7, 8, 9
     * epoch6 = 0`, 1`, 2`, 3`, 4`
     * epoch7 = 5`, 6`, 7`, 8`, 9`
     * epoch8 = 10, 6`, 7`, 8`, 9`
     * epoch9 = 10, 11, 7`, 8`, 9`
     * epoch10 = 10, 11, 12, 8`, 9`
     * epoch11 = 10, 11, 12, 13, 9` 
     * epoch12 = 10, 11, 12, 13, 14
     */
    @Test(timeout = 30000L)
    public void testStreamCutsWithMultipleChunks() {
        String scope = "streamCutTest";
        String name = "streamCutTest";
        int startingSegmentNumber = new Random().nextInt(2000);
        PersistentStreamBase stream = createScaleAndRollStreamForMultiChunkTests(name, scope, startingSegmentNumber, System::currentTimeMillis);

        OperationContext context = getContext();
        EpochRecord epoch0 = stream.getEpochRecord(0, context).join(); // 0, 1, 2, 3, 4
        EpochRecord epoch1 = stream.getEpochRecord(1, context).join(); // 5, 1, 2, 3, 4
        EpochRecord epoch2 = stream.getEpochRecord(2, context).join(); // 5, 6, 2, 3, 4
        EpochRecord epoch3 = stream.getEpochRecord(3, context).join(); // 5, 6, 7, 3, 4
        EpochRecord epoch4 = stream.getEpochRecord(4, context).join(); // 5, 6, 7, 8, 4
        EpochRecord epoch5 = stream.getEpochRecord(5, context).join(); // 5, 6, 7, 8, 9
        EpochRecord epoch6 = stream.getEpochRecord(6, context).join(); // 0`, 1`, 2`, 3`, 4`
        EpochRecord epoch7 = stream.getEpochRecord(7, context).join(); // 5`, 6`, 7`, 8`, 9`
        EpochRecord epoch8 = stream.getEpochRecord(8, context).join(); // 10, 6`, 7`, 8`, 9`
        EpochRecord epoch9 = stream.getEpochRecord(9, context).join(); // 10, 11, 7`, 8`, 9`
        EpochRecord epoch10 = stream.getEpochRecord(10, context).join(); // 10, 11, 12, 8`, 9`
        EpochRecord epoch11 = stream.getEpochRecord(11, context).join(); // 10, 11, 12, 13, 9` 
        EpochRecord epoch12 = stream.getEpochRecord(12, context).join(); // 10, 11, 12, 13, 14
                
        List<Map.Entry<Double, Double>> keyRanges = epoch0.getSegments().stream()
                                                          .map(x -> new AbstractMap.SimpleEntry<>(x.getKeyStart(), x.getKeyEnd()))
                                                          .collect(Collectors.toList());
        
        // create a streamCut1 using 0, 6, 7, 8, 9`
        HashMap<Long, Long> streamCut1 = new HashMap<>();
        // segment 0 from epoch 0 // sealed in epoch 1
        streamCut1.put(epoch0.getSegments().stream().filter(x -> x.overlaps(keyRanges.get(0).getKey(), 
                keyRanges.get(0).getValue())).findAny().get().segmentId(), 10L);
        // segment 6 from epoch 2 // sealed in epoch 6
        streamCut1.put(epoch2.getSegments().stream().filter(x -> x.overlaps(keyRanges.get(1).getKey(), 
                keyRanges.get(1).getValue())).findAny().get().segmentId(), 10L);
        // segment 7 from epoch 3 // sealed in epoch 6
        streamCut1.put(epoch3.getSegments().stream().filter(x -> x.overlaps(keyRanges.get(2).getKey(),
                keyRanges.get(2).getValue())).findAny().get().segmentId(), 10L);
        // segment 8 from epoch 5 // sealed in epoch 6
        streamCut1.put(epoch5.getSegments().stream().filter(x -> x.overlaps(keyRanges.get(3).getKey(), 
                keyRanges.get(3).getValue())).findAny().get().segmentId(), 10L);
        // segment 9` from epoch 7 // created in epoch 7
        streamCut1.put(epoch7.getSegments().stream().filter(x -> x.overlaps(keyRanges.get(4).getKey(), 
                keyRanges.get(4).getValue())).findAny().get().segmentId(), 10L);
        Map<StreamSegmentRecord, Integer> span1 = stream.computeStreamCutSpan(streamCut1, context).join();
        
        assertEquals(0, span1.entrySet().stream()
                            .filter(x -> x.getKey().getSegmentNumber() == startingSegmentNumber + 0)
                            .findAny().get().getValue().intValue());
        assertEquals(5, span1.entrySet().stream()
                            .filter(x -> x.getKey().getSegmentNumber() == startingSegmentNumber + 6)
                            .findAny().get().getValue().intValue());
        assertEquals(5, span1.entrySet().stream()
                            .filter(x -> x.getKey().getSegmentNumber() == startingSegmentNumber + 7)
                            .findAny().get().getValue().intValue());
        assertEquals(5, span1.entrySet().stream()
                            .filter(x -> x.getKey().getSegmentNumber() == startingSegmentNumber + 8)
                            .findAny().get().getValue().intValue());
        assertEquals(7, span1.entrySet().stream()
                            .filter(x -> x.getKey().getSegmentNumber() == startingSegmentNumber + 9)
                            .findAny().get().getValue().intValue());

        // create a streamCut2 5, 6`, 12, 8`, 14
        HashMap<Long, Long> streamCut2 = new HashMap<>();
        // segment 5 from epoch 1 // sealed in epoch 6 
        streamCut2.put(epoch1.getSegments().stream().filter(x -> x.overlaps(keyRanges.get(0).getKey(), 
                keyRanges.get(0).getValue())).findAny().get().segmentId(), 10L);
        // segment 6` from epoch 7 // sealed in epoch 9
        streamCut2.put(epoch7.getSegments().stream().filter(x -> x.overlaps(keyRanges.get(1).getKey(), 
                keyRanges.get(1).getValue())).findAny().get().segmentId(), 10L);
        // segment 12 from epoch 10 // never sealed
        streamCut2.put(epoch10.getSegments().stream().filter(x -> x.overlaps(keyRanges.get(2).getKey(), 
                keyRanges.get(2).getValue())).findAny().get().segmentId(), 10L);
        // segment 8` from epoch 7 // sealed in epoch 11
        streamCut2.put(epoch7.getSegments().stream().filter(x -> x.overlaps(keyRanges.get(3).getKey(), 
                keyRanges.get(3).getValue())).findAny().get().segmentId(), 10L);
        // segment 14 from epoch 12 // never sealed
        streamCut2.put(epoch12.getSegments().stream().filter(x -> x.overlaps(keyRanges.get(4).getKey(), 
                keyRanges.get(4).getValue())).findAny().get().segmentId(), 10L);
        Map<StreamSegmentRecord, Integer> span2 = stream.computeStreamCutSpan(streamCut2, context).join();
        
        assertEquals(5, span2.entrySet().stream()
                            .filter(x -> x.getKey().getSegmentNumber() == startingSegmentNumber + 5)
                            .findAny().get().getValue().intValue());
        assertEquals(8, span2.entrySet().stream()
                            .filter(x -> x.getKey().getSegmentNumber() == startingSegmentNumber + 6)
                            .findAny().get().getValue().intValue());
        assertEquals(12, span2.entrySet().stream()
                            .filter(x -> x.getKey().getSegmentNumber() == startingSegmentNumber + 12)
                            .findAny().get().getValue().intValue());
        assertEquals(10, span2.entrySet().stream()
                            .filter(x -> x.getKey().getSegmentNumber() == startingSegmentNumber + 8)
                            .findAny().get().getValue().intValue());
        assertEquals(12, span2.entrySet().stream()
                            .filter(x -> x.getKey().getSegmentNumber() == startingSegmentNumber + 14)
                            .findAny().get().getValue().intValue());

        Set<StreamSegmentRecord> segmentsBetween = stream.segmentsBetweenStreamCutSpans(span1, span2, context).join();
        Set<Long> segmentIdsBetween = segmentsBetween.stream().map(x -> x.segmentId()).collect(Collectors.toSet());
        // create a streamCut1 using 0, 6, 7, 8, 9`
        // create a streamCut2 5, 6`, 12, 8`, 14

        // 0, 5, 6, 1`, 6`, 7, 2`, 7`, 12, 8, 3`, 8`, 9`, 14
        Set<Long> expected = new HashSet<>();
        expected.add(NameUtils.computeSegmentId(startingSegmentNumber + 0, 0));
        expected.add(NameUtils.computeSegmentId(startingSegmentNumber + 5, 1));
        expected.add(NameUtils.computeSegmentId(startingSegmentNumber + 6, 2));
        expected.add(NameUtils.computeSegmentId(startingSegmentNumber + 1, 6));
        expected.add(NameUtils.computeSegmentId(startingSegmentNumber + 6, 7));
        expected.add(NameUtils.computeSegmentId(startingSegmentNumber + 7, 3));
        expected.add(NameUtils.computeSegmentId(startingSegmentNumber + 2, 6));
        expected.add(NameUtils.computeSegmentId(startingSegmentNumber + 7, 7));
        expected.add(NameUtils.computeSegmentId(startingSegmentNumber + 12, 10));
        expected.add(NameUtils.computeSegmentId(startingSegmentNumber + 8, 4));
        expected.add(NameUtils.computeSegmentId(startingSegmentNumber + 3, 6));
        expected.add(NameUtils.computeSegmentId(startingSegmentNumber + 8, 7));
        expected.add(NameUtils.computeSegmentId(startingSegmentNumber + 9, 7));
        expected.add(NameUtils.computeSegmentId(startingSegmentNumber + 14, 12));
        assertEquals(expected, segmentIdsBetween);

        // Note: all sealed segments have sizes 100L. So expected size = 1400 - 10x5 - 90 x 5 = 900

        long sizeBetween = stream.sizeBetweenStreamCuts(streamCut1, streamCut2, segmentsBetween, context).join();
        assertEquals(900L, sizeBetween);
    }
    // endregion
    
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
    @Test(timeout = 30000L)
    public void testStreamCutSegmentsBetween() {
        int startingSegmentNumber = new Random().nextInt(2000);
        List<AbstractMap.SimpleEntry<Double, Double>> newRanges;
        long timestamp = System.currentTimeMillis();

        PersistentStreamBase stream = createStream("scope", "stream" + startingSegmentNumber, timestamp, 
                3, startingSegmentNumber);

        OperationContext context = getContext();
        List<StreamSegmentRecord> initialSegments = stream.getActiveSegments(context).join();
        StreamSegmentRecord zero = initialSegments.stream().filter(x -> x.segmentId() == computeSegmentId(
                startingSegmentNumber + 0, 0)).findAny().get();
        StreamSegmentRecord one = initialSegments.stream().filter(x -> x.segmentId() == computeSegmentId(
                startingSegmentNumber + 1, 0)).findAny().get();
        StreamSegmentRecord two = initialSegments.stream().filter(x -> x.segmentId() == computeSegmentId(
                startingSegmentNumber + 2, 0)).findAny().get();
        StreamSegmentRecord three = new StreamSegmentRecord(startingSegmentNumber + 3, 1, 
                timestamp, 0.0, 0.16);
        StreamSegmentRecord four = new StreamSegmentRecord(startingSegmentNumber + 4, 1, 
                timestamp, 0.16, zero.getKeyEnd());
        StreamSegmentRecord five = new StreamSegmentRecord(startingSegmentNumber + 5, 1, 
                timestamp, one.getKeyStart(), 0.5);
        StreamSegmentRecord six = new StreamSegmentRecord(startingSegmentNumber + 6, 1,
                timestamp, 0.5, one.getKeyEnd());
        StreamSegmentRecord seven = new StreamSegmentRecord(startingSegmentNumber + 7, 1,
                timestamp, two.getKeyStart(), 0.83);
        StreamSegmentRecord eight = new StreamSegmentRecord(startingSegmentNumber + 8, 1, 
                timestamp, 0.83, two.getKeyEnd());
        StreamSegmentRecord nine = new StreamSegmentRecord(startingSegmentNumber + 9, 2, 
                timestamp, 0.0, 0.5);
        StreamSegmentRecord ten = new StreamSegmentRecord(startingSegmentNumber + 10, 2, 
                timestamp, 0.5, 1);

        // 2 -> 7, 8
        // 1 -> 5, 6
        // 0 -> 3, 4
        LinkedList<StreamSegmentRecord> newsegments = new LinkedList<>();
        newsegments.add(three);
        newsegments.add(four);
        newsegments.add(five);
        newsegments.add(six);
        newsegments.add(seven);
        newsegments.add(eight);
        List<Long> segmentsToSeal = stream.getActiveSegments(context).join().stream().map(x -> x.segmentId()).collect(Collectors.toList());
        newRanges = newsegments.stream()
                            .map(x -> new AbstractMap.SimpleEntry<>(x.getKeyStart(), x.getKeyEnd())).collect(Collectors.toList());

        scaleStream(stream, ++timestamp, segmentsToSeal, new LinkedList<>(newRanges), Collections.emptyMap());

        // 6, 7, 8 -> 10
        // 3, 4, 5 -> 9
        newsegments = new LinkedList<>();
        newsegments.add(nine);
        newsegments.add(ten);
        segmentsToSeal = stream.getActiveSegments(context).join().stream().map(x -> x.segmentId()).collect(Collectors.toList());

        newRanges = newsegments.stream()
                            .map(x -> new AbstractMap.SimpleEntry<>(x.getKeyStart(), x.getKeyEnd())).collect(Collectors.toList());
        scaleStream(stream, ++timestamp, segmentsToSeal, new LinkedList<>(newRanges), Collections.emptyMap());

        // only from
        Map<Long, Long> fromStreamCut = new HashMap<>();
        fromStreamCut.put(zero.segmentId(), 0L);
        fromStreamCut.put(one.segmentId(), 0L);
        fromStreamCut.put(two.segmentId(), 0L);

        List<StreamSegmentRecord> segmentsBetween = stream.getSegmentsBetweenStreamCuts(fromStreamCut, Collections.emptyMap(), 
                context).join();
        assertEquals(11, segmentsBetween.size());

        fromStreamCut = new HashMap<>();
        fromStreamCut.put(zero.segmentId(), 0L);
        fromStreamCut.put(two.segmentId(), 0L);
        fromStreamCut.put(five.segmentId(), 0L);
        fromStreamCut.put(six.segmentId(), 0L);
        segmentsBetween = stream.getSegmentsBetweenStreamCuts(fromStreamCut, Collections.emptyMap(), context).join();
        assertEquals(10, segmentsBetween.size());
        assertTrue(segmentsBetween.stream().noneMatch(x -> x.segmentId() == one.segmentId()));

        fromStreamCut = new HashMap<>();
        fromStreamCut.put(zero.segmentId(), 0L);
        fromStreamCut.put(five.segmentId(), 0L);
        fromStreamCut.put(ten.segmentId(), 0L);
        segmentsBetween = stream.getSegmentsBetweenStreamCuts(fromStreamCut, Collections.emptyMap(), context).join();
        assertEquals(6, segmentsBetween.size());
        // 0, 3, 4, 5, 9, 10
        assertTrue(segmentsBetween.stream().noneMatch(x -> x.segmentId() == one.segmentId() || x.segmentId() == two.segmentId()
                || x.segmentId() == six.segmentId() || x.segmentId() == seven.segmentId() || x.segmentId() == eight.segmentId()));

        fromStreamCut = new HashMap<>();
        fromStreamCut.put(six.segmentId(), 0L);
        fromStreamCut.put(seven.segmentId(), 0L);
        fromStreamCut.put(eight.segmentId(), 0L);
        fromStreamCut.put(nine.segmentId(), 0L);
        segmentsBetween = stream.getSegmentsBetweenStreamCuts(fromStreamCut, Collections.emptyMap(), context).join();
        assertEquals(5, segmentsBetween.size());
        assertTrue(segmentsBetween.stream().noneMatch(x -> x.segmentId() == one.segmentId() || x.segmentId() == two.segmentId()
                || x.segmentId() == three.segmentId() || x.segmentId() == four.segmentId() || x.segmentId() == five.segmentId()));

        fromStreamCut = new HashMap<>();
        fromStreamCut.put(ten.segmentId(), 0L);
        fromStreamCut.put(nine.segmentId(), 0L);
        segmentsBetween = stream.getSegmentsBetweenStreamCuts(fromStreamCut, Collections.emptyMap(), context).join();
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
        AssertExtensions.assertSuppliedFutureThrows("", 
                () -> stream.getSegmentsBetweenStreamCuts(fromStreamCutCopy, toStreamCut, context), 
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
        AssertExtensions.assertSuppliedFutureThrows("",
                () -> stream.getSegmentsBetweenStreamCuts(fromStreamCutOverlap, toStreamCutOverlap, context),
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
        AssertExtensions.assertSuppliedFutureThrows("",
                () -> stream.getSegmentsBetweenStreamCuts(fromPartialOverlap, toPartialOverlap, context),
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
        segmentsBetween = stream.getSegmentsBetweenStreamCuts(fromStreamCutSuccess, toStreamCutSuccess, context).join();
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
        segmentsBetween = stream.getSegmentsBetweenStreamCuts(fromStreamCutSuccess, toStreamCutSuccess, context).join();
        assertEquals(10, segmentsBetween.size());
        assertTrue(segmentsBetween.stream().noneMatch(x -> x.segmentId() == one.segmentId()));

        // empty from
        toStreamCutSuccess = new HashMap<>();
        toStreamCutSuccess.put(zero.segmentId(), 0L);
        toStreamCutSuccess.put(five.segmentId(), 0L);
        toStreamCutSuccess.put(six.segmentId(), 0L);
        toStreamCutSuccess.put(two.segmentId(), 0L);
        segmentsBetween = stream.getSegmentsBetweenStreamCuts(Collections.emptyMap(), toStreamCutSuccess, context).join();
        assertEquals(5, segmentsBetween.size());
        assertTrue(segmentsBetween.stream().noneMatch(x -> x.segmentId() == three.segmentId() || x.segmentId() == four.segmentId()
                || x.segmentId() == seven.segmentId() || x.segmentId() == eight.segmentId() || x.segmentId() == nine.segmentId()
                || x.segmentId() == ten.segmentId()));

    }

    @Test(timeout = 30000L)
    public void testWriterMark() {
        OperationContext context = getContext();
        PersistentStreamBase stream = spy(createStream("writerMark", "writerMark", System.currentTimeMillis(), 3, 0));

        Map<String, WriterMark> marks = stream.getAllWriterMarks(context).join();
        assertTrue(marks.isEmpty());

        // call noteWritermark --> this should call createMarkerRecord
        String writer = "writer";
        long timestamp = 0L;
        Map<Long, Long> position = Collections.singletonMap(0L, 1L);
        ImmutableMap<Long, Long> immutablePos = ImmutableMap.copyOf(position);

        stream.noteWriterMark(writer, timestamp, position, context).join();

        marks = stream.getAllWriterMarks(context).join();
        assertEquals(marks.size(), 1);
        verify(stream, times(1)).createWriterMarkRecord(writer, timestamp, immutablePos, context);

        VersionedMetadata<WriterMark> mark = stream.getWriterMarkRecord(writer, context).join();
        Version version = mark.getVersion();

        // call noteWritermark --> this should call update
        stream.noteWriterMark(writer, timestamp, position, context).join();
        marks = stream.getAllWriterMarks(context).join();
        assertEquals(marks.size(), 1);
        mark = stream.getWriterMarkRecord(writer, context).join();
        assertNotEquals(mark.getVersion(), version);
        verify(stream, times(1)).updateWriterMarkRecord(anyString(), anyLong(), any(), anyBoolean(),
                any(), any());

        AssertExtensions.assertFutureThrows("", stream.createWriterMarkRecord(writer, timestamp, immutablePos, context),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataExistsException);

        // update 
        mark = stream.getWriterMarkRecord(writer, context).join();

        stream.updateWriterMarkRecord(writer, timestamp, immutablePos, true, mark.getVersion(), context).join();

        // verify bad version on update
        AssertExtensions.assertFutureThrows("", stream.updateWriterMarkRecord(writer, timestamp, immutablePos, 
                true, mark.getVersion(), context),
                e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException);

        mark = stream.getWriterMarkRecord(writer, context).join();

        // update deleted writer --> data not found
        stream.removeWriter(writer, mark.getObject(), context).join();
        marks = stream.getAllWriterMarks(context).join();
        assertEquals(marks.size(), 0);
        AssertExtensions.assertFutureThrows("", stream.updateWriterMarkRecord(writer, timestamp, immutablePos, 
                true, mark.getVersion(), context),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException);

        // create writer record
        stream.createWriterMarkRecord(writer, timestamp, immutablePos, context).join();

        // Mock to throw DataNotFound for getWriterMark. This should result in noteWriterMark to attempt to create. 
        // That should fail with DataExists resulting in recursive call into noteWriterMark to do get and update. 
        AtomicBoolean callRealMethod = new AtomicBoolean(false);
        doAnswer(x -> {
            if (callRealMethod.compareAndSet(false, true)) {
                return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, "writer mark"));
            } else {
                return x.callRealMethod();
            }
        }).when(stream).getWriterMarkRecord(writer, context);

        timestamp = 1L;
        position = Collections.singletonMap(0L, 2L);

        AssertExtensions.assertFutureThrows("Expecting WriteConflict", stream.noteWriterMark(writer, timestamp, 
                position, context),
            e -> Exceptions.unwrap(e) instanceof StoreException.WriteConflictException);
    }

    @Test(timeout = 30000L)
    public void testTransactionMark() {
        PersistentStreamBase streamObj = createStream("txnMark", "txnMark", System.currentTimeMillis(), 3, 0);

        UUID txnId = new UUID(0L, 0L);
        OperationContext context = getContext();
        VersionedTransactionData tx01 = streamObj.createTransaction(txnId, 100, 100, context).join();

        String writer1 = "writer1";
        long time = 1L;
        streamObj.sealTransaction(txnId, true, Optional.of(tx01.getVersion()), writer1, time, context).join();
        streamObj.startCommittingTransactions(100, context).join();
        TxnWriterMark writerMarks = new TxnWriterMark(time, Collections.singletonMap(0L, 1L), txnId);
        Map<String, TxnWriterMark> marksForWriters = Collections.singletonMap(writer1, writerMarks);
        streamObj.generateMarksForTransactions(context, marksForWriters).join();

        // verify that writer mark is created in the store
        WriterMark mark = streamObj.getWriterMark(writer1, context).join();
        assertEquals(mark.getTimestamp(), time);

        // idempotent call to generateMarksForTransactions
        streamObj.generateMarksForTransactions(context, marksForWriters).join();
        mark = streamObj.getWriterMark(writer1, context).join();
        assertEquals(mark.getTimestamp(), time);

        // complete txn commit explicitly such that activeTxnRecord no longer exists and then invoke generateMark
        streamObj.commitTransaction(txnId, context).join();
        AssertExtensions.assertFutureThrows("", streamObj.getActiveTx(0, txnId, context),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException);

        streamObj.generateMarksForTransactions(context, marksForWriters).join();
        mark = streamObj.getWriterMark(writer1, context).join();
        assertEquals(mark.getTimestamp(), time);

    }
    
    @Test(timeout = 30000L)
    public void testTransactionMarkFromSingleWriter() {
        PersistentStreamBase streamObj = spy(createStream("txnMark", "txnMark", System.currentTimeMillis(), 1, 0));

        String writer = "writer";

        UUID txnId1 = new UUID(0L, 0L);
        UUID txnId2 = new UUID(0L, 1L);
        UUID txnId3 = new UUID(0L, 2L);
        UUID txnId4 = new UUID(0L, 3L);
        long time = 1L;
        // create 4 transactions with same writer id. 
        // two of the transactions should have same highest time. 
        OperationContext context = getContext();
        VersionedTransactionData tx01 = streamObj.createTransaction(txnId1, 100, 100, context).join();
        streamObj.sealTransaction(txnId1, true, Optional.of(tx01.getVersion()), writer, time, context).join();

        VersionedTransactionData tx02 = streamObj.createTransaction(txnId2, 100, 100, context).join();
        streamObj.sealTransaction(txnId2, true, Optional.of(tx02.getVersion()), writer, time + 1L, context).join();

        VersionedTransactionData tx03 = streamObj.createTransaction(txnId3, 100, 100, context).join();
        streamObj.sealTransaction(txnId3, true, Optional.of(tx03.getVersion()), writer, time + 4L, context).join();

        VersionedTransactionData tx04 = streamObj.createTransaction(txnId4, 100, 100, context).join();
        streamObj.sealTransaction(txnId4, true, Optional.of(tx04.getVersion()), writer, time + 4L, context).join();

        streamObj.startCommittingTransactions(100, context).join();
        TxnWriterMark writerMarks = new TxnWriterMark(time + 4L,
                Collections.singletonMap(0L, 1L), txnId4);
        Map<String, TxnWriterMark> marksForWriters = Collections.singletonMap(writer, writerMarks);

        streamObj.generateMarksForTransactions(context, marksForWriters).join();

        // verify that writer mark is created in the store
        WriterMark mark = streamObj.getWriterMark(writer, context).join();
        assertEquals(mark.getTimestamp(), time + 4L);
        
        // verify that only one call to note time is made
        verify(streamObj, times(1)).noteWriterMark(anyString(), anyLong(), any(), any());
    }
    
    @Test(timeout = 30000L)
    public void testgetTransactions() {
        PersistentStreamBase streamObj = spy(createStream("txn", "txn", System.currentTimeMillis(), 
                1, 0));
        
        UUID txnId1 = new UUID(0L, 0L);
        UUID txnId2 = new UUID(0L, 1L);
        UUID txnId3 = new UUID(0L, 2L);
        UUID txnId4 = new UUID(0L, 3L);
        List<UUID> txns = Lists.newArrayList(txnId1, txnId2, txnId3, txnId4);
        // create 1 2 and 4. dont create 3.
        OperationContext context = getContext();
        streamObj.createTransaction(txnId1, 1000L, 1000L, context).join();
        streamObj.createTransaction(txnId2, 1000L, 1000L, context).join();
        streamObj.sealTransaction(txnId2, true, Optional.empty(), "w", 1000L, context).join();
        streamObj.createTransaction(txnId4, 1000L, 1000L, context).join();
        streamObj.sealTransaction(txnId4, false, Optional.empty(), "w", 1000L, context).join();
        List<ActiveTxnRecord> transactions = 
                streamObj.getTransactionRecords(0, txns.stream().map(UUID::toString).collect(Collectors.toList()), 
                        context).join();
        assertEquals(4, transactions.size());
        assertEquals(transactions.get(0).getTxnStatus(), TxnStatus.OPEN);
        assertEquals(transactions.get(1).getTxnStatus(), TxnStatus.COMMITTING);
        assertEquals(transactions.get(2), ActiveTxnRecord.EMPTY);
        assertEquals(transactions.get(3).getTxnStatus(), TxnStatus.ABORTING);
    }

    @Test
    public void testListCompletedTnxns() {
        OperationContext context = getContext();
        createScope("listCompletedScope", context);
        PersistentStreamBase stream = createStream("listCompletedScope", "listCompletedStream", System.currentTimeMillis(), 5, new Random().nextInt(2000), 2, 2);
        UUID txnId1 = null;
        for (int i = 0; i < 600; i++) {
            txnId1 = createAndCommitTransaction(stream, 0, Long.valueOf(i));
        }

        Map<UUID, TxnStatus> list = stream.listCompletedTxns(context).join();
        assertEquals(0, list.size());

        // start commit transactions
        VersionedMetadata<CommittingTransactionsRecord> ctr = stream.startCommittingTransactions(600, context).join().getKey();
        stream.getVersionedState(context).thenCompose(s -> stream.updateVersionedState(s, State.COMMITTING_TXN, context)).join();

        // start rolling transaction
        ctr = stream.startRollingTxn(1, ctr, context).join();

        stream.completeRollingTxn(Collections.emptyMap(), ctr, context).join();
        stream.completeCommittingTransactions(ctr, context, Collections.emptyMap()).join();

        list = stream.listCompletedTxns(context).join();
        //verify no of record returned by api
        assertEquals(500, list.size());

        //verify api contains recent record
        assertTrue(list.keySet().contains(txnId1));
    }
}

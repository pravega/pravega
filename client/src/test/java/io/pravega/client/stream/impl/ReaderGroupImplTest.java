/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl.ReaderGroupStateInitSerializer;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl.ReaderGroupStateUpdatesSerializer;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.state.InitialUpdate;
import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.state.Update;
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReaderSegmentDistribution;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.ReaderGroupState.ClearCheckpointsBefore;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.InlineExecutor;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Cleanup;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static io.pravega.test.common.AssertExtensions.assertSuppliedFutureThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ReaderGroupImplTest {

    private static final String SCOPE = "scope";
    private static final String GROUP_NAME = "readerGroup";
    private ReaderGroupImpl readerGroup;
    @Mock
    private SynchronizerConfig synchronizerConfig;
    @Mock
    private SynchronizerClientFactory clientFactory;
    @Mock
    private Controller controller;
    @Mock
    private ConnectionPool connectionPool;
    @Mock
    private StateSynchronizer<ReaderGroupState> synchronizer;
    @Mock
    private ReaderGroupState state;
    @Mock
    private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

    private Serializer<InitialUpdate<ReaderGroupState>> initSerializer = new ReaderGroupStateInitSerializer();
    private Serializer<Update<ReaderGroupState>> updateSerializer = new ReaderGroupStateUpdatesSerializer();

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception {
        when(clientFactory.createStateSynchronizer(anyString(), any(Serializer.class), any(Serializer.class),
                                                   any(SynchronizerConfig.class))).thenReturn(synchronizer);
        when(synchronizer.getState()).thenReturn(state);
        readerGroup = new ReaderGroupImpl(SCOPE, GROUP_NAME, synchronizerConfig, initSerializer,
                updateSerializer, clientFactory, controller, connectionPool);
    }

    @Test(expected = IllegalArgumentException.class)
    public void resetReadersToStreamCutDuplicateStreamCut() {
        readerGroup.resetReaderGroup(ReaderGroupConfig.builder().startFromStreamCuts(ImmutableMap.<Stream, StreamCut>builder()
                .put(createStream("s1"), createStreamCut("s1", 2))
                .put(createStream("s2"), createStreamCut("s1", 3)).build())
        .build());
    }

    @Test(expected = IllegalArgumentException.class)
    public void resetReadersToStreamMissingStreamCut() {
        readerGroup.resetReaderGroup(ReaderGroupConfig.builder().startFromStreamCuts(ImmutableMap.<Stream, StreamCut>builder()
                .put(createStream("s1"), createStreamCut("s2", 2)).build()).build());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void resetReadersToStreamCut() {
        readerGroup.resetReaderGroup(ReaderGroupConfig.builder().startFromStreamCuts(ImmutableMap.<Stream,
                StreamCut>builder()
                .put(createStream("s1"), createStreamCut("s1", 2))
                .put(createStream("s2"), createStreamCut("s2", 3)).build())
                                                      .build());
        verify(synchronizer, times(1)).updateStateUnconditionally(any(Update.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void resetReadersToCheckpoint() {
        Map<Segment, Long> positions = new HashMap<>();
        IntStream.of(2).forEach(segNum -> positions.put(new Segment(SCOPE, "s1", segNum), 10L));
        Checkpoint checkpoint = new CheckpointImpl("testChkPoint", positions);
        readerGroup.resetReaderGroup(ReaderGroupConfig.builder().startFromCheckpoint(checkpoint).build());
        verify(synchronizer, times(1)).updateStateUnconditionally(any(Update.class));
    }

    @Test
    public void resetReadersFromSubscriberToNonSubscriber() {
        // Setup mocks
        when(controller.addSubscriber(SCOPE, "s1", GROUP_NAME)).thenReturn(CompletableFuture.completedFuture(true));
        when(controller.addSubscriber(SCOPE, "s2", GROUP_NAME)).thenReturn(CompletableFuture.completedFuture(true));
        when(controller.deleteSubscriber(SCOPE, "s1", GROUP_NAME)).thenReturn(CompletableFuture.completedFuture(true));
        when(controller.deleteSubscriber(SCOPE, "s2", GROUP_NAME)).thenReturn(CompletableFuture.completedFuture(true));

        ReaderGroupConfig firstConfig = ReaderGroupConfig.builder().startFromStreamCuts(ImmutableMap.<Stream,
                StreamCut>builder()
                .put(createStream("s1"), createStreamCut("s1", 2))
                .put(createStream("s2"), createStreamCut("s2", 3)).build())
                .subscribeForRetention()
                .build();

        // Subscriber ReaderGroupConfig
        readerGroup.resetReaderGroup(firstConfig);

        // Setup mocks for reset call
        when(synchronizer.getState()).thenReturn(state);
        when(state.getConfig()).thenReturn(firstConfig);

        // Non subscriber ReaderGroupConfig
        readerGroup.resetReaderGroup(ReaderGroupConfig.builder().startFromStreamCuts(ImmutableMap.<Stream,
                StreamCut>builder()
                .put(createStream("s3"), createStreamCut("s3", 2))
                .put(createStream("s2"), createStreamCut("s2", 3)).build())
                .build());
        verify(controller, times(1)).addSubscriber(SCOPE, "s1", GROUP_NAME);
        verify(controller, times(1)).addSubscriber(SCOPE, "s2", GROUP_NAME);
        verify(controller, times(1)).deleteSubscriber(SCOPE, "s1", GROUP_NAME);
        verify(controller, times(1)).deleteSubscriber(SCOPE, "s2", GROUP_NAME);
        verify(synchronizer, times(2)).updateStateUnconditionally(any(Update.class));
    }

    @Test
    public void resetReadersFromNonSubscriberToSubscriber() {
        // Setup mocks
        when(controller.addSubscriber(SCOPE, "s3", GROUP_NAME)).thenReturn(CompletableFuture.completedFuture(true));
        when(controller.addSubscriber(SCOPE, "s2", GROUP_NAME)).thenReturn(CompletableFuture.completedFuture(true));

        ReaderGroupConfig firstConfig = ReaderGroupConfig.builder().startFromStreamCuts(ImmutableMap.<Stream,
                StreamCut>builder()
                .put(createStream("s1"), createStreamCut("s1", 2))
                .put(createStream("s2"), createStreamCut("s2", 3)).build())
                .build();

        // Non subscriber ReaderGroupConfig
        readerGroup.resetReaderGroup(firstConfig);

        when(synchronizer.getState()).thenReturn(state);
        when(state.getConfig()).thenReturn(firstConfig);

        // Subscriber ReaderGroupConfig
        readerGroup.resetReaderGroup(ReaderGroupConfig.builder().startFromStreamCuts(ImmutableMap.<Stream,
                StreamCut>builder()
                .put(createStream("s3"), createStreamCut("s3", 2))
                .put(createStream("s2"), createStreamCut("s2", 3)).build())
                .subscribeForRetention()
                .build());
        verify(controller, times(1)).addSubscriber(SCOPE, "s3", GROUP_NAME);
        verify(controller, times(1)).addSubscriber(SCOPE, "s2", GROUP_NAME);
        verify(synchronizer, times(2)).updateStateUnconditionally(any(Update.class));
    }

    @Test
    public void getUnreadBytesBasedOnLastCheckpointPosition() {
        final String stream = "s1";
        final StreamCut startStreamCut = getStreamCut(stream, 10L, 1, 2);
        final StreamCut endStreamCut = getStreamCut(stream, 25L, 1, 2);
        //setup mocks
        when(state.getPositionsForLastCompletedCheckpoint())
                .thenReturn(Optional.of(ImmutableMap.of(Stream.of(SCOPE, stream), startStreamCut.asImpl().getPositions())));
        when(state.getEndSegments()).thenReturn(endStreamCut.asImpl().getPositions());
        when(synchronizer.getState()).thenReturn(state);
        ImmutableSet<Segment> segmentSet = ImmutableSet.<Segment>builder()
                .addAll(startStreamCut.asImpl().getPositions().keySet()).addAll(endStreamCut.asImpl().getPositions().keySet()).build();
        when(controller.getSegments(startStreamCut, endStreamCut))
                .thenReturn(CompletableFuture.completedFuture(new StreamSegmentSuccessors(segmentSet, "")));

        assertEquals(30L, readerGroup.unreadBytes());
    }

    @Test
    public void getUnreadBytesBasedOnLastPosition() {
        final String stream = "s1";
        final StreamCut startStreamCut = getStreamCut(stream, 10L, 1, 2);
        final StreamCut endStreamCut = getStreamCut(stream, 30L, 1, 2);

        //setup mocks
        when(state.getPositionsForLastCompletedCheckpoint()).thenReturn(Optional.empty()); // simulate zero checkpoints.
        Map<SegmentWithRange, Long> positions = startStreamCut.asImpl()
                                                              .getPositions()
                                                              .entrySet()
                                                              .stream()
                                                              .collect(Collectors.toMap(e -> new SegmentWithRange(e.getKey(), null),
                                                                                        e -> e.getValue()));
        when(state.getPositions()).thenReturn(ImmutableMap.of(Stream.of(SCOPE, stream), positions));
        when(state.getEndSegments()).thenReturn(endStreamCut.asImpl().getPositions());
        when(synchronizer.getState()).thenReturn(state);
        ImmutableSet<Segment> r = ImmutableSet.<Segment>builder()
                                              .addAll(startStreamCut.asImpl().getPositions().keySet())
                                              .addAll(endStreamCut.asImpl().getPositions().keySet())
                                              .build();
        when(controller.getSegments(startStreamCut,
                                    endStreamCut)).thenReturn(CompletableFuture.completedFuture(new StreamSegmentSuccessors(r, "")));

        assertEquals(40L, readerGroup.unreadBytes());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void initiateCheckpointFailure() {
        when(synchronizer.updateState(any(StateSynchronizer.UpdateGeneratorFunction.class))).thenReturn(false);
        CompletableFuture<Checkpoint> result = readerGroup.initiateCheckpoint("test", scheduledThreadPoolExecutor);
        assertTrue("expecting a checkpoint failure", result.isCompletedExceptionally());
        try {
            result.get();
        } catch (InterruptedException | ExecutionException e) {
            assertTrue("expecting MaxNumberOfCheckpointsExceededException", e.getCause() instanceof MaxNumberOfCheckpointsExceededException);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void initiateCheckpointSuccess() {
        when(synchronizer.updateState(any(StateSynchronizer.UpdateGeneratorFunction.class))).thenReturn(true);
        CompletableFuture<Checkpoint> result = readerGroup.initiateCheckpoint("test", scheduledThreadPoolExecutor);
        assertFalse("not expecting a checkpoint failure", result.isCompletedExceptionally());
    }

    @Test(timeout = 10000)
    public void generateStreamCutSuccess() {
        when(synchronizer.getState()).thenReturn(state);
        when(state.isCheckpointComplete(any(String.class))).thenReturn(false).thenReturn(true);
        when(state.getStreamCutsForCompletedCheckpoint(anyString())).thenReturn(Optional.of(ImmutableMap.of(createStream("s1"),
                                                                                                                            createStreamCut("s1", 2))));
        @Cleanup("shutdown")
        InlineExecutor executor = new InlineExecutor();
        CompletableFuture<Map<Stream, StreamCut>> result = readerGroup.generateStreamCuts(executor);
        assertEquals(createStreamCut("s1", 2), result.join().get(createStream("s1")));
    }

    @Test(timeout = 10000)
    public void generateStreamCutsError() {
        when(synchronizer.getState()).thenReturn(state);
        when(state.isCheckpointComplete(any(String.class))).thenReturn(true);
        when(state.getStreamCutsForCompletedCheckpoint(anyString())).thenReturn(Optional.empty()); //mock empty.
        @Cleanup("shutdown")
        InlineExecutor executor = new InlineExecutor();
        CompletableFuture<Map<Stream, StreamCut>> result = readerGroup.generateStreamCuts(executor);
        assertSuppliedFutureThrows("CheckpointFailedException is expected", () -> readerGroup.generateStreamCuts(executor),
                     t -> t instanceof CheckpointFailedException);
    }

    @Test(timeout = 1000)
    public void getEndSegmentsForStream() {
        Map<Segment, Long> endSegmentMap = ReaderGroupImpl.getEndSegmentsForStreams(ReaderGroupConfig.builder().stream(Stream.of(SCOPE, "s1"),
                                                                                                            getStreamCut("s1", 0L, 0),
                                                                                                            getStreamCut("s1", -1L, 0))
                                                                                          .build());
        assertEquals(Long.MAX_VALUE, endSegmentMap.get(new Segment(SCOPE, "s1", 0)).longValue());
    }

    private StreamCut createStreamCut(String streamName, int numberOfSegments) {
        Map<Segment, Long> positions = new HashMap<>();
        IntStream.of(numberOfSegments).forEach(segNum -> positions.put(new Segment(SCOPE, streamName, segNum), 10L));
        return new StreamCutImpl(createStream(streamName), positions);
    }

    private Stream createStream(String streamName) {
        return Stream.of(SCOPE, streamName);
    }

    private StreamCut getStreamCut(String streamName, long offset, int... segmentNumbers) {
        ImmutableMap.Builder<Segment, Long> builder = ImmutableMap.<Segment, Long>builder();
        Arrays.stream(segmentNumbers).forEach(seg -> {
            builder.put(new Segment(SCOPE, streamName, seg), offset);
        });

        return new StreamCutImpl(Stream.of(SCOPE, streamName), builder.build());
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void testFutureCancelation() throws Exception {
        AtomicBoolean completed = new AtomicBoolean(false);
        when(synchronizer.updateState(any(StateSynchronizer.UpdateGeneratorFunction.class))).thenReturn(true);
        when(state.isCheckpointComplete("test")).thenReturn(false).thenReturn(true);
        Mockito.doAnswer(invocation -> {
            completed.set(true);
            return null;
        }).when(synchronizer).updateStateUnconditionally(eq(new ClearCheckpointsBefore("test")));
        @Cleanup("shutdown")
        InlineExecutor executor = new InlineExecutor();
        CompletableFuture<Checkpoint> result = readerGroup.initiateCheckpoint("test", executor);
        assertFalse(result.isDone());
        result.cancel(false);
        AssertExtensions.assertEventuallyEquals(true, completed::get, 5000);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void readerGroupSegmentDistribution() {
        ReaderGroupState state = mock(ReaderGroupState.class);
        when(synchronizer.getState()).thenReturn(state);

        Set<String> readers = new HashSet<>();
        readers.add("1");
        readers.add("2");
        readers.add("3");
        when(state.getOnlineReaders()).thenReturn(readers);

        SegmentWithRange segment = mock(SegmentWithRange.class);
        Map<SegmentWithRange, Long> map = Collections.singletonMap(segment, 0L);
        when(state.getAssignedSegments(anyString())).thenReturn(map);
        
        when(state.getNumberOfUnassignedSegments()).thenReturn(2);
        
        ReaderSegmentDistribution readerSegmentDistribution = readerGroup.getReaderSegmentDistribution();

        Map<String, Integer> distribution = readerSegmentDistribution.getReaderSegmentDistribution();
        assertEquals(3, distribution.size());
        assertTrue(distribution.containsKey("1"));
        assertTrue(distribution.containsKey("2"));
        assertTrue(distribution.containsKey("3"));
        assertEquals(2, readerSegmentDistribution.getUnassignedSegments());
        assertEquals(1, distribution.get("1").intValue());
        assertEquals(1, distribution.get("2").intValue());
        assertEquals(1, distribution.get("3").intValue());
    }
}

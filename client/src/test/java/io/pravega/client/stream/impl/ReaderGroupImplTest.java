/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
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
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl.ReaderGroupStateInitSerializer;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl.ReaderGroupStateUpdatesSerializer;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.state.InitialUpdate;
import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.state.Update;
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
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
    private ClientFactory clientFactory;
    @Mock
    private Controller controller;
    @Mock
    private ConnectionFactory connectionFactory;
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
        readerGroup = new ReaderGroupImpl(SCOPE, GROUP_NAME, synchronizerConfig, initSerializer,
                updateSerializer, clientFactory, controller, connectionFactory);
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
        when(state.getPositions()).thenReturn(ImmutableMap.of(Stream.of(SCOPE, stream), startStreamCut.asImpl().getPositions()));
        when(state.getEndSegments()).thenReturn(endStreamCut.asImpl().getPositions());
        when(synchronizer.getState()).thenReturn(state);
        ImmutableSet<Segment> r = ImmutableSet.<Segment>builder()
                .addAll(startStreamCut.asImpl().getPositions().keySet()).addAll(endStreamCut.asImpl().getPositions().keySet()).build();
        when(controller.getSegments(startStreamCut, endStreamCut))
                .thenReturn(CompletableFuture.completedFuture(new StreamSegmentSuccessors(r, "")));

        assertEquals(40L, readerGroup.unreadBytes());
    }

    @Test
    public void initiateCheckpointFailure() {
        when(synchronizer.updateState(any(StateSynchronizer.UpdateGeneratorFunction.class))).thenReturn(false);
        CompletableFuture<Checkpoint> result = readerGroup.initiateCheckpoint("test", scheduledThreadPoolExecutor);
        assertTrue("expecting a checkpoint failure", result.isCompletedExceptionally());
        try {
            result.get();
        } catch (InterruptedException | ExecutionException e) {
            assertTrue("expecting checkpoint failed exception", e.getCause() instanceof CheckpointFailedException);
        }
    }

    @Test
    public void initiateCheckpointSuccess() {
        when(synchronizer.updateState(any(StateSynchronizer.UpdateGeneratorFunction.class))).thenReturn(true);
        CompletableFuture<Checkpoint> result = readerGroup.initiateCheckpoint("test", scheduledThreadPoolExecutor);
        assertFalse("not expecting a checkpoint failure", result.isCompletedExceptionally());
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
}

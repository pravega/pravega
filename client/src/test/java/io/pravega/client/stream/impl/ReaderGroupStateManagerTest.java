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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl.ReaderGroupStateInitSerializer;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl.ReaderGroupStateUpdatesSerializer;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.stream.Position;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReaderNotInReaderGroupException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ReaderGroupState.CreateCheckpoint;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.client.stream.mock.MockSegmentStreamFactory;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.test.common.AssertExtensions;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import lombok.Cleanup;
import lombok.val;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import static java.util.Collections.singletonList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(BlockJUnit4ClassRunner.class)
public class ReaderGroupStateManagerTest {
    private static final int SERVICE_PORT = 12345;

    private static class MockControllerWithSuccessors extends MockController {
        private StreamSegmentsWithPredecessors successors;

        public MockControllerWithSuccessors(String endpoint, int port, ConnectionFactory connectionFactory, StreamSegmentsWithPredecessors successors) {
            super(endpoint, port, connectionFactory, false);
            this.successors = successors;
        }

        @Override
        public CompletableFuture<StreamSegmentsWithPredecessors> getSuccessors(Segment segment) {
            return completedFuture(successors);
        }
    }
    
    @Test(timeout = 10000)
    public void testCompaction() throws ReaderNotInReaderGroupException {
        String scope = "scope";
        String stream = "stream";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, false);
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        @Cleanup
        SynchronizerClientFactory clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory, streamFactory,
                                                            streamFactory, streamFactory, streamFactory);

        SynchronizerConfig config = SynchronizerConfig.builder().build();
        @Cleanup
        StateSynchronizer<ReaderGroupState> state1 = createState(stream, clientFactory, config);
        Segment s1 = new Segment(scope, stream, 1);
        Segment s2 = new Segment(scope, stream, 2);
        Map<Segment, Long> segments = new HashMap<>();
        segments.put(s1, 1L);
        segments.put(s2, 2L);
        AtomicLong clock = new AtomicLong();
        state1.initialize(new ReaderGroupState.ReaderGroupStateInit(
                ReaderGroupConfig.builder().stream(Stream.of(scope, stream)).build(), segments, Collections.emptyMap()));
        ReaderGroupStateManager r1 = new ReaderGroupStateManager("r1", state1, controller, clock::get);
        r1.initializeReader(0);
        r1.acquireNewSegmentsIfNeeded(0);
        assertTrue(state1.getState().getUnassignedSegments().isEmpty());
        state1.compact(s -> new ReaderGroupState.CompactReaderGroupState(s));
        clock.addAndGet(ReaderGroupStateManager.UPDATE_WINDOW.toNanos());
        r1.acquireNewSegmentsIfNeeded(0);
        state1.compact(s -> new ReaderGroupState.CompactReaderGroupState(s));
        clock.addAndGet(ReaderGroupStateManager.UPDATE_WINDOW.toNanos());
        @Cleanup
        StateSynchronizer<ReaderGroupState> state2 = createState(stream, clientFactory, config);
        ReaderGroupStateManager r2 = new ReaderGroupStateManager("r2", state2, controller, clock::get);
        r2.initializeReader(0);
        assertEquals(state1.getState().getPositions(), state2.getState().getPositions());
        state1.fetchUpdates();
        assertTrue(r1.releaseSegment(s1, 1, 1));
        state2.fetchUpdates();
        assertFalse(state2.getState().getUnassignedSegments().isEmpty());
        assertFalse(r2.acquireNewSegmentsIfNeeded(0).isEmpty());
        state2.fetchUpdates();
        assertTrue(state2.getState().getUnassignedSegments().isEmpty());
        assertEquals(Collections.singleton(s2), state2.getState().getSegments("r1"));
        assertEquals(Collections.singleton(s1), state2.getState().getSegments("r2"));
        state2.compact(s -> new ReaderGroupState.CompactReaderGroupState(s));
        r1.findSegmentToReleaseIfRequired();
        r1.acquireNewSegmentsIfNeeded(0);
        r2.getCheckpoint();
        @Cleanup
        StateSynchronizer<ReaderGroupState> state3 = createState(stream, clientFactory, config);
        state3.fetchUpdates();
        assertEquals(state3.getState().getPositions(), state1.getState().getPositions());
        assertEquals(state3.getState().getPositions(), state2.getState().getPositions());
    }

    private StateSynchronizer<ReaderGroupState> createState(String stream, SynchronizerClientFactory clientFactory,
                                                            SynchronizerConfig config) {
        return clientFactory.createStateSynchronizer(stream, new ReaderGroupStateUpdatesSerializer(),
                                                     new ReaderGroupStateInitSerializer(), config);
    }
    
    @Test(timeout = 20000)
    public void testSegmentSplit() throws ReaderNotInReaderGroupException {
        String scope = "scope";
        String stream = "stream";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        Segment initialSegment = new Segment(scope, stream, 0);
        Segment successorA = new Segment(scope, stream, 1);
        Segment successorB = new Segment(scope, stream, 2);
        MockController controller = new MockControllerWithSuccessors(endpoint.getEndpoint(), endpoint.getPort(),
                connectionFactory,
                new StreamSegmentsWithPredecessors(
                        ImmutableMap.of(new SegmentWithRange(successorA, 0.0, 0.5), singletonList(0L),
                                        new SegmentWithRange(successorB, 0.5, 1.0), singletonList(0L)), ""));
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        @Cleanup
        SynchronizerClientFactory clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory, streamFactory,
                streamFactory, streamFactory, streamFactory);
        SynchronizerConfig config = SynchronizerConfig.builder().build();
        @Cleanup
        StateSynchronizer<ReaderGroupState> stateSynchronizer = createState(stream, clientFactory, config);
        Map<Segment, Long> segments = new HashMap<>();
        segments.put(initialSegment, 1L);
        stateSynchronizer.initialize(new ReaderGroupState.ReaderGroupStateInit(
                ReaderGroupConfig.builder().stream(Stream.of(scope, stream)).build(), segments, Collections.emptyMap()));
        val readerState = new ReaderGroupStateManager("testReader", stateSynchronizer, controller, null);
        readerState.initializeReader(0);
        Map<Segment, Long> newSegments = readerState.acquireNewSegmentsIfNeeded(0);
        assertEquals(1, newSegments.size());
        assertEquals(Long.valueOf(1), newSegments.get(initialSegment));

        readerState.handleEndOfSegment(initialSegment);
        newSegments = readerState.acquireNewSegmentsIfNeeded(0);
        assertEquals(2, newSegments.size());
        assertEquals(Long.valueOf(0), newSegments.get(successorA));
        assertEquals(Long.valueOf(0), newSegments.get(successorB));

        newSegments = readerState.acquireNewSegmentsIfNeeded(0);
        assertTrue(newSegments.isEmpty());
    }

    @Test(timeout = 20000)
    public void testSegmentMerge() throws ReaderNotInReaderGroupException {
        String scope = "scope";
        String stream = "stream";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        Segment initialSegmentA = new Segment(scope, stream, 0L);
        Segment initialSegmentB = new Segment(scope, stream, 1L);
        Segment successor = new Segment(scope, stream, 2L);
        MockController controller = new MockControllerWithSuccessors(endpoint.getEndpoint(), endpoint.getPort(),
                connectionFactory, new StreamSegmentsWithPredecessors(
                        Collections.singletonMap(new SegmentWithRange(successor, 0.0, 1.0), ImmutableList.of(0L, 1L)), ""));
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        @Cleanup
        SynchronizerClientFactory clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory, streamFactory, streamFactory, streamFactory, streamFactory);
        SynchronizerConfig config = SynchronizerConfig.builder().build();
        @Cleanup
        StateSynchronizer<ReaderGroupState> stateSynchronizer = createState(stream, clientFactory, config);
        Map<Segment, Long> segments = new HashMap<>();
        segments.put(initialSegmentA, 1L);
        segments.put(initialSegmentB, 2L);
        stateSynchronizer.initialize(new ReaderGroupState.ReaderGroupStateInit(
                ReaderGroupConfig.builder().stream(Stream.of(scope, stream)).build(), segments, Collections.emptyMap()));
        val readerState = new ReaderGroupStateManager("testReader", stateSynchronizer, controller, null);
        readerState.initializeReader(0);
        Map<Segment, Long> newSegments = readerState.acquireNewSegmentsIfNeeded(0);
        assertEquals(2, newSegments.size());
        assertEquals(Long.valueOf(1), newSegments.get(initialSegmentA));
        assertEquals(Long.valueOf(2), newSegments.get(initialSegmentB));
        
        readerState.handleEndOfSegment(initialSegmentA);
        newSegments = readerState.acquireNewSegmentsIfNeeded(0);
        assertTrue(newSegments.isEmpty());
        
        readerState.handleEndOfSegment(initialSegmentB);
        newSegments = readerState.acquireNewSegmentsIfNeeded(0);
        assertEquals(1, newSegments.size());
        assertEquals(Long.valueOf(0), newSegments.get(successor));
        
        newSegments = readerState.acquireNewSegmentsIfNeeded(0);
        assertTrue(newSegments.isEmpty());
    }
    
    @Test(timeout = 10000)
    public void testAddReader() throws ReaderNotInReaderGroupException {
        String scope = "scope";
        String stream = "stream";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, false);
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        @Cleanup
        SynchronizerClientFactory clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory, streamFactory, streamFactory, streamFactory, streamFactory);

        SynchronizerConfig config = SynchronizerConfig.builder().build();
        @Cleanup
        StateSynchronizer<ReaderGroupState> stateSynchronizer = createState(stream, clientFactory, config);
        Map<Segment, Long> segments = new HashMap<>();
        segments.put(new Segment(scope, stream, 0), 1L);
        stateSynchronizer.initialize(new ReaderGroupState.ReaderGroupStateInit(ReaderGroupConfig.builder().stream(Stream.of(scope, stream)).build(), segments, Collections.emptyMap()));
        ReaderGroupStateManager readerState = new ReaderGroupStateManager("testReader",
                stateSynchronizer,
                controller,
                null);
        readerState.initializeReader(0);
        Segment toRelease = readerState.findSegmentToReleaseIfRequired();
        assertNull(toRelease);
        Map<Segment, Long> newSegments = readerState.acquireNewSegmentsIfNeeded(0);
        assertFalse(newSegments.isEmpty());
        assertEquals(1, newSegments.size());
        assertTrue(newSegments.containsKey(new Segment(scope, stream, 0)));
        assertEquals(1, newSegments.get(new Segment(scope, stream, 0)).longValue());
    }
    
    @Test(timeout = 10000)
    public void testRemoveReader() throws ReaderNotInReaderGroupException {
        String scope = "scope";
        String stream = "stream";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, false);
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        @Cleanup
        SynchronizerClientFactory clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory, streamFactory, streamFactory, streamFactory, streamFactory);

        SynchronizerConfig config = SynchronizerConfig.builder().build();
        @Cleanup
        StateSynchronizer<ReaderGroupState> stateSynchronizer = createState(stream, clientFactory, config);
        AtomicLong clock = new AtomicLong();
        Map<Segment, Long> segments = new HashMap<>();
        segments.put(new Segment(scope, stream, 0), 123L);
        segments.put(new Segment(scope, stream, 1), 456L);
        stateSynchronizer.initialize(new ReaderGroupState.ReaderGroupStateInit(ReaderGroupConfig.builder().stream(Stream.of(scope, stream)).build(), segments, Collections.emptyMap()));
        ReaderGroupStateManager readerState1 = new ReaderGroupStateManager("testReader",
                stateSynchronizer,
                controller,
                clock::get);
        readerState1.initializeReader(0);
        Segment toRelease = readerState1.findSegmentToReleaseIfRequired();
        assertNull(toRelease);
        Map<Segment, Long> newSegments = readerState1.acquireNewSegmentsIfNeeded(0);
        assertFalse(newSegments.isEmpty());
        assertEquals(2, newSegments.size());
        
        ReaderGroupStateManager readerState2 = new ReaderGroupStateManager("testReader2",
                stateSynchronizer,
                controller,
                clock::get);
        readerState2.initializeReader(0);
        clock.addAndGet(ReaderGroupStateManager.UPDATE_WINDOW.toNanos());
        assertNotNull(readerState1.findSegmentToReleaseIfRequired());
        boolean released = readerState1.releaseSegment(new Segment(scope, stream, 0), 789L, 0L);
        assertTrue(released);
        newSegments = readerState2.acquireNewSegmentsIfNeeded(0);
        assertEquals(1, newSegments.size());
        assertEquals(Long.valueOf(789L), newSegments.get(new Segment(scope, stream, 0)));
        
        ReaderGroupStateManager.readerShutdown("testReader2", null, stateSynchronizer);
        AssertExtensions.assertThrows(ReaderNotInReaderGroupException.class,
                () -> readerState2.releaseSegment(new Segment(scope, stream, 0), 711L, 0L));

        clock.addAndGet(ReaderGroupStateManager.UPDATE_WINDOW.toNanos());
        newSegments = readerState1.acquireNewSegmentsIfNeeded(0);
        assertEquals(1, newSegments.size());
        assertEquals(Long.valueOf(789L), newSegments.get(new Segment(scope, stream, 0)));

        AssertExtensions.assertThrows(ReaderNotInReaderGroupException.class,
                () -> readerState2.acquireNewSegmentsIfNeeded(0L));
    }

    @Test(timeout = 10000)
    public void testRemoveReaderWithNullPosition() throws ReaderNotInReaderGroupException {

        String scope = "scope";
        String stream = "stream";
        SynchronizerConfig synchronizerConfig = SynchronizerConfig.builder().build();
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        AtomicLong clock = new AtomicLong();
        Map<Segment, Long> segmentMap = ImmutableMap.<Segment, Long>builder().put(new Segment(scope, stream, 0), 123L)
                                                                           .put(new Segment(scope, stream, 1), 456L)
                                                                           .build();
        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder().stream(Stream.of(scope, stream)).build();

        // Setup mocks
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, false);
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();

        @Cleanup
        SynchronizerClientFactory clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory, streamFactory, streamFactory, streamFactory, streamFactory);

        // Create Reader Group State corresponding to testReader1.
        @Cleanup
        StateSynchronizer<ReaderGroupState> stateSynchronizer1 = createState(stream, clientFactory, synchronizerConfig);
        stateSynchronizer1.initialize(new ReaderGroupState.ReaderGroupStateInit(readerGroupConfig, segmentMap, Collections.emptyMap()));
        ReaderGroupStateManager readerState1 = new ReaderGroupStateManager("testReader1",
                stateSynchronizer1,
                controller,
                clock::get);

        readerState1.initializeReader(0); // Initialize readerState1 from stateSynchronizer1

        // Validations.
        assertNull(readerState1.findSegmentToReleaseIfRequired()); // No segments to release.
        Map<Segment, Long> newSegments = readerState1.acquireNewSegmentsIfNeeded(0); // Acquire Segments and update StateSynchronizer stream.
        assertFalse(newSegments.isEmpty());
        assertEquals(2, newSegments.size()); // Verify testReader1 has acquired the segments.

        // Create ReaderGroupState corresponding to testReader2
        @Cleanup
        StateSynchronizer<ReaderGroupState> stateSynchronizer2 = createState(stream, clientFactory, synchronizerConfig);
        ReaderGroupStateManager readerState2 = new ReaderGroupStateManager("testReader2",
                stateSynchronizer2,
                controller,
                clock::get);
        readerState2.initializeReader(0); // Initialize readerState2 from stateSynchronizer2.

        // Try acquiring segments for testReader2.
        newSegments = readerState2.acquireNewSegmentsIfNeeded(0);
        assertTrue(newSegments.isEmpty()); // No new segments are acquired since testReader1 already owns it and release timer did not complete.

        // Trigger testReader1 shutdown.
        ReaderGroupStateManager.readerShutdown("testReader1", null, stateSynchronizer1);
        // Advance clock by ReaderGroup refresh time.
        clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(readerGroupConfig.getGroupRefreshTimeMillis()));

        // Try acquiring segments for testReader2, we should acquire the segments owned by testReader1.
        newSegments = readerState2.acquireNewSegmentsIfNeeded(0);
        assertFalse(newSegments.isEmpty());
        assertEquals(2, newSegments.size());
    }

    @Test(timeout = 5000)
    public void testReleaseAndAcquireTimes() throws ReaderNotInReaderGroupException {
        String scope = "scope";
        String stream = "stream";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, false);
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        @Cleanup
        SynchronizerClientFactory clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory, streamFactory, streamFactory, streamFactory, streamFactory);

        SynchronizerConfig config = SynchronizerConfig.builder().build();
        @Cleanup
        StateSynchronizer<ReaderGroupState> state = createState(stream, clientFactory, config);
        AtomicLong clock = new AtomicLong();
        Map<Segment, Long> segments = new HashMap<>();
        segments.put(new Segment(scope, stream, 0), 0L);
        segments.put(new Segment(scope, stream, 1), 1L);
        segments.put(new Segment(scope, stream, 2), 2L);
        segments.put(new Segment(scope, stream, 3), 3L);
        state.initialize(new ReaderGroupState.ReaderGroupStateInit(ReaderGroupConfig.builder().stream(Stream.of(scope, stream)).build(),
                segments, Collections.emptyMap()));

        ReaderGroupStateManager reader1 = new ReaderGroupStateManager("reader1", state, controller, clock::get);
        reader1.initializeReader(100);

        ReaderGroupStateManager reader2 = new ReaderGroupStateManager("reader2", state, controller, clock::get);
        reader2.initializeReader(100);
        
        Map<Segment, Long> newSegments = reader1.acquireNewSegmentsIfNeeded(123);
        assertEquals(0, newSegments.size());
        newSegments = reader2.acquireNewSegmentsIfNeeded(123);
        assertEquals(0, newSegments.size());
        
        clock.addAndGet(ReaderGroupStateManager.UPDATE_WINDOW.toNanos());
        newSegments = reader1.acquireNewSegmentsIfNeeded(123);
        assertEquals(2, newSegments.size());
        
        Duration r1aqt = ReaderGroupStateManager.calculateAcquireTime("reader1", state.getState());
        Duration r2aqt = ReaderGroupStateManager.calculateAcquireTime("reader2", state.getState());
        assertTrue(r1aqt.toMillis() > r2aqt.toMillis());
        
        Duration r1rlt = ReaderGroupStateManager.calculateReleaseTime("reader1", state.getState());
        Duration r2rlt = ReaderGroupStateManager.calculateReleaseTime("reader2", state.getState());
        assertTrue(r1rlt.toMillis() < r2rlt.toMillis());
        
        reader1.releaseSegment(newSegments.keySet().iterator().next(), 0, 123);
        newSegments = reader2.acquireNewSegmentsIfNeeded(123);
        assertEquals(2, newSegments.size());
        
        r1aqt = ReaderGroupStateManager.calculateAcquireTime("reader1", state.getState());
        r2aqt = ReaderGroupStateManager.calculateAcquireTime("reader2", state.getState());
        assertTrue(r1aqt.toMillis() < r2aqt.toMillis());
        
        r1rlt = ReaderGroupStateManager.calculateReleaseTime("reader1", state.getState());
        r2rlt = ReaderGroupStateManager.calculateReleaseTime("reader2", state.getState());
        assertTrue(r1rlt.toMillis() > r2rlt.toMillis());
    }

    @Test(timeout = 5000)
    public void testAcquireRace() throws ReaderNotInReaderGroupException {
        String scope = "scope";
        String stream = "stream";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, false);
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        @Cleanup
        SynchronizerClientFactory clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory, streamFactory, streamFactory, streamFactory, streamFactory);

        SynchronizerConfig config = SynchronizerConfig.builder().build();
        @Cleanup
        StateSynchronizer<ReaderGroupState> state1 = createState(stream, clientFactory, config);
        @Cleanup
        StateSynchronizer<ReaderGroupState> state2 = createState(stream, clientFactory, config);
        AtomicLong clock = new AtomicLong();
        Map<Segment, Long> segments = new HashMap<>();
        segments.put(new Segment(scope, stream, 0), 0L);
        segments.put(new Segment(scope, stream, 1), 1L);
        segments.put(new Segment(scope, stream, 2), 2L);
        segments.put(new Segment(scope, stream, 3), 3L);
        state1.initialize(new ReaderGroupState.ReaderGroupStateInit(ReaderGroupConfig.builder().stream(Stream.of(scope, stream)).build(), segments, Collections.emptyMap()));

        ReaderGroupStateManager reader1 = new ReaderGroupStateManager("reader1", state1, controller, clock::get);
        reader1.initializeReader(0);

        ReaderGroupStateManager reader2 = new ReaderGroupStateManager("reader2", state2, controller, clock::get);
        reader2.initializeReader(0);

        Map<Segment, Long> segments1 = reader1.acquireNewSegmentsIfNeeded(0);
        assertFalse(segments1.isEmpty());
        assertEquals(2, segments1.size());
        assertTrue(reader1.acquireNewSegmentsIfNeeded(0).isEmpty());
        clock.addAndGet(ReaderGroupStateManager.UPDATE_WINDOW.toNanos());
        assertNull(reader1.findSegmentToReleaseIfRequired());

        Map<Segment, Long> segments2 = reader2.acquireNewSegmentsIfNeeded(0);
        assertFalse(segments2.isEmpty());
        assertEquals(2, segments2.size());
        assertNull(reader2.findSegmentToReleaseIfRequired());

        segments1 = reader1.acquireNewSegmentsIfNeeded(0);
        assertTrue(segments1.isEmpty());
    }

    @Test(timeout = 10000)
    public void testSegmentsAssigned() throws ReaderNotInReaderGroupException {
        String scope = "scope";
        String stream = "stream";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, false);
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        @Cleanup
        SynchronizerClientFactory clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory, streamFactory, streamFactory, streamFactory, streamFactory);

        SynchronizerConfig config = SynchronizerConfig.builder().build();
        @Cleanup
        StateSynchronizer<ReaderGroupState> stateSynchronizer = createState(stream, clientFactory, config);
        AtomicLong clock = new AtomicLong();
        Map<Segment, Long> segments = new HashMap<>();
        segments.put(new Segment(scope, stream, 0), 0L);
        segments.put(new Segment(scope, stream, 1), 1L);
        segments.put(new Segment(scope, stream, 2), 2L);
        segments.put(new Segment(scope, stream, 3), 3L);
        stateSynchronizer.initialize(new ReaderGroupState.ReaderGroupStateInit(ReaderGroupConfig.builder().stream(Stream.of(scope, stream)).build(),
                segments, Collections.emptyMap()));

        ReaderGroupStateManager reader1 = new ReaderGroupStateManager("reader1", stateSynchronizer, controller,
                clock::get);
        reader1.initializeReader(0);

        ReaderGroupStateManager reader2 = new ReaderGroupStateManager("reader2", stateSynchronizer, controller,
                clock::get);
        reader2.initializeReader(0);

        Map<Segment, Long> segments1 = reader1.acquireNewSegmentsIfNeeded(0);
        assertFalse(segments1.isEmpty());
        assertEquals(2, segments1.size());
        assertTrue(reader1.acquireNewSegmentsIfNeeded(0).isEmpty());
        assertNull(reader1.findSegmentToReleaseIfRequired());

        Map<Segment, Long> segments2 = reader2.acquireNewSegmentsIfNeeded(0);
        assertFalse(segments2.isEmpty());
        assertEquals(2, segments2.size());
        assertTrue(reader2.acquireNewSegmentsIfNeeded(0).isEmpty());
        assertNull(reader2.findSegmentToReleaseIfRequired());

        assertTrue(Sets.intersection(segments1.keySet(), segments2.keySet()).isEmpty());

        clock.addAndGet(ReaderGroupStateManager.UPDATE_WINDOW.toNanos());

        assertFalse(reader1.releaseSegment(new Segment(scope, stream, 0), 0, 0));

        clock.addAndGet(ReaderGroupStateManager.UPDATE_WINDOW.toNanos());

        assertTrue(reader1.acquireNewSegmentsIfNeeded(0).isEmpty());
        assertNull(reader1.findSegmentToReleaseIfRequired());
        assertTrue(reader2.acquireNewSegmentsIfNeeded(0).isEmpty());
        assertNull(reader2.findSegmentToReleaseIfRequired());

        reader1.readerShutdown(new PositionImpl(segments1));

        Map<Segment, Long> segmentsRecovered = reader2.acquireNewSegmentsIfNeeded(0);
        assertFalse(segmentsRecovered.isEmpty());
        assertEquals(2, segmentsRecovered.size());
        assertEquals(segments1, segmentsRecovered);
        assertTrue(reader2.acquireNewSegmentsIfNeeded(0).isEmpty());
        assertNull(reader2.findSegmentToReleaseIfRequired());

        segments2.putAll(segmentsRecovered);
        reader2.readerShutdown(new PositionImpl(segments2));

        reader1.initializeReader(0);
        segments1 = reader1.acquireNewSegmentsIfNeeded(0);
        assertEquals(4, segments1.size());
        assertEquals(segments2, segments1);
    }

    @Test(timeout = 20000)
    public void testReleaseWhenReadersAdded() throws ReaderNotInReaderGroupException {
        String scope = "scope";
        String stream = "stream";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, false);
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        @Cleanup
        SynchronizerClientFactory clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory, streamFactory, streamFactory, streamFactory, streamFactory);
        SynchronizerConfig config = SynchronizerConfig.builder().build();
        @Cleanup
        StateSynchronizer<ReaderGroupState> stateSynchronizer = createState(stream, clientFactory, config);
        AtomicLong clock = new AtomicLong();
        Map<Segment, Long> segments = new HashMap<>();
        segments.put(new Segment(scope, stream, 0), 0L);
        segments.put(new Segment(scope, stream, 1), 1L);
        segments.put(new Segment(scope, stream, 2), 2L);
        segments.put(new Segment(scope, stream, 3), 3L);
        segments.put(new Segment(scope, stream, 4), 4L);
        segments.put(new Segment(scope, stream, 5), 5L);
        stateSynchronizer.initialize(new ReaderGroupState.ReaderGroupStateInit( ReaderGroupConfig.builder().stream(Stream.of(scope, stream)).build(), segments, Collections.emptyMap()));

        ReaderGroupStateManager reader1 = new ReaderGroupStateManager("reader1", stateSynchronizer, controller,
                clock::get);
        reader1.initializeReader(0);
        Map<Segment, Long> segments1 = reader1.acquireNewSegmentsIfNeeded(0);
        assertEquals(6, segments1.size());

        ReaderGroupStateManager reader2 = new ReaderGroupStateManager("reader2", stateSynchronizer, controller,
                clock::get);
        reader2.initializeReader(0);
        assertTrue(reader2.acquireNewSegmentsIfNeeded(0).isEmpty());

        assertNull(reader1.findSegmentToReleaseIfRequired());

        clock.addAndGet(ReaderGroupStateManager.UPDATE_WINDOW.toNanos());

        assertNotNull(reader1.findSegmentToReleaseIfRequired());
        reader1.releaseSegment(new Segment(scope, stream, 3), 3, 0);

        assertNull(reader1.findSegmentToReleaseIfRequired());

        clock.addAndGet(ReaderGroupStateManager.UPDATE_WINDOW.toNanos());

        assertNotNull(reader1.findSegmentToReleaseIfRequired());
        reader1.releaseSegment(new Segment(scope, stream, 4), 4, 0);

        assertNull(reader1.findSegmentToReleaseIfRequired());

        clock.addAndGet(ReaderGroupStateManager.UPDATE_WINDOW.toNanos());

        assertNotNull(reader1.findSegmentToReleaseIfRequired());
        reader1.releaseSegment(new Segment(scope, stream, 5), 5, 0);

        assertNull(reader1.findSegmentToReleaseIfRequired());

        clock.addAndGet(ReaderGroupStateManager.UPDATE_WINDOW.toNanos());

        assertNull(reader1.findSegmentToReleaseIfRequired());

        Map<Segment, Long> segments2 = reader2.acquireNewSegmentsIfNeeded(0);
        assertEquals(3, segments2.size());
        clock.addAndGet(ReaderGroupStateManager.UPDATE_WINDOW.toNanos());
        
        ReaderGroupStateManager reader3 = new ReaderGroupStateManager("reader3", stateSynchronizer, controller,
                clock::get);
        reader3.initializeReader(0);
        assertTrue(reader3.acquireNewSegmentsIfNeeded(0).isEmpty());

        assertNotNull(reader1.findSegmentToReleaseIfRequired());
        reader1.releaseSegment(new Segment(scope, stream, 0), 0, 0);
        assertNull(reader1.findSegmentToReleaseIfRequired());

        assertNotNull(reader2.findSegmentToReleaseIfRequired());
        reader2.releaseSegment(new Segment(scope, stream, 3), 3, 0);
        assertNull(reader2.findSegmentToReleaseIfRequired());

        Map<Segment, Long> segments3 = reader3.acquireNewSegmentsIfNeeded(0);
        assertEquals(2, segments3.size());

        clock.addAndGet(ReaderGroupStateManager.UPDATE_WINDOW.toNanos());

        assertTrue(reader3.acquireNewSegmentsIfNeeded(0).isEmpty());
        assertNull(reader1.findSegmentToReleaseIfRequired());
        assertNull(reader2.findSegmentToReleaseIfRequired());
        assertNull(reader3.findSegmentToReleaseIfRequired());
    }

    @Test(timeout = 10000)
    public void testCheckpoint() throws ReaderNotInReaderGroupException {
        String scope = "scope";
        String stream = "stream";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        Segment initialSegment = new Segment(scope, stream, 0);
        Segment successorA = new Segment(scope, stream, 1);
        Segment successorB = new Segment(scope, stream, 2);
        MockController controller = new MockControllerWithSuccessors(endpoint.getEndpoint(), endpoint.getPort(),
                connectionFactory,
                new StreamSegmentsWithPredecessors(
                        ImmutableMap.of(new SegmentWithRange(successorA, 0.0, 0.5), singletonList(0L),
                                        new SegmentWithRange(successorB, 0.5, 1.0), singletonList(0L)), ""));
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        @Cleanup
        SynchronizerClientFactory clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory, streamFactory,
                                                            streamFactory, streamFactory, streamFactory);
        SynchronizerConfig config = SynchronizerConfig.builder().build();
        @Cleanup
        StateSynchronizer<ReaderGroupState> stateSynchronizer = createState(stream, clientFactory, config);
        Map<Segment, Long> segments = new HashMap<>();
        segments.put(initialSegment, 1L);
        stateSynchronizer.initialize(new ReaderGroupState.ReaderGroupStateInit(ReaderGroupConfig.builder().stream(Stream.of(scope, stream)).build(),
                segments, Collections.emptyMap()));
        val readerState = new ReaderGroupStateManager("testReader", stateSynchronizer, controller, null);
        readerState.initializeReader(0);
        assertNull(readerState.getCheckpoint());
        stateSynchronizer.updateStateUnconditionally(new CreateCheckpoint("CP1"));
        stateSynchronizer.fetchUpdates();
        assertEquals("CP1", readerState.getCheckpoint());
        assertEquals("CP1", readerState.getCheckpoint());
        readerState.checkpoint("CP1", new PositionImpl(Collections.emptyMap()));
        assertNull(readerState.getCheckpoint());
        stateSynchronizer.updateStateUnconditionally(new CreateCheckpoint("CP2"));
        stateSynchronizer.updateStateUnconditionally(new CreateCheckpoint("CP3"));
        stateSynchronizer.fetchUpdates();
        assertEquals("CP2", readerState.getCheckpoint());
        readerState.checkpoint("CP2", new PositionImpl(Collections.emptyMap()));
        assertEquals("CP3", readerState.getCheckpoint());
        readerState.checkpoint("CP3", new PositionImpl(Collections.emptyMap()));
        assertNull(readerState.getCheckpoint());
    }
    
    @Test(timeout = 10000)
    public void testCheckpointContainsAllShards() throws ReaderNotInReaderGroupException {
        String scope = "scope";
        String stream = "stream";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        Segment segment0 = new Segment(scope, stream, 0);
        Segment segment1 = new Segment(scope, stream, 1);
        Segment segment2 = new Segment(scope, stream, 2);
        MockController controller = new MockControllerWithSuccessors(endpoint.getEndpoint(), endpoint.getPort(),
                connectionFactory, new StreamSegmentsWithPredecessors(ImmutableMap.of(), ""));
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        @Cleanup
        SynchronizerClientFactory clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory, streamFactory,
                                                            streamFactory, streamFactory, streamFactory);
        SynchronizerConfig config = SynchronizerConfig.builder().build();
        @Cleanup
        StateSynchronizer<ReaderGroupState> stateSynchronizer = createState(stream, clientFactory, config);
        Map<Segment, Long> segments = ImmutableMap.of(segment0, 0L, segment1, 1L, segment2, 2L);
        stateSynchronizer.initialize(new ReaderGroupState.ReaderGroupStateInit(ReaderGroupConfig.builder().stream(Stream.of(scope, stream)).build(),
                segments, Collections.emptyMap()));
        val readerState1 = new ReaderGroupStateManager("reader1", stateSynchronizer, controller, null);
        readerState1.initializeReader(0);
        val readerState2 = new ReaderGroupStateManager("reader2", stateSynchronizer, controller, null);
        readerState2.initializeReader(0);
        
        assertEquals(segments, stateSynchronizer.getState().getUnassignedSegments());
        stateSynchronizer.updateStateUnconditionally(new CreateCheckpoint("CP1"));
        stateSynchronizer.fetchUpdates();
        assertEquals("CP1", readerState1.getCheckpoint());
        assertEquals(Collections.emptyMap(), readerState1.acquireNewSegmentsIfNeeded(1));
        assertEquals(Collections.emptyMap(), readerState2.acquireNewSegmentsIfNeeded(2));
        assertEquals("CP1", readerState2.getCheckpoint());
        readerState1.checkpoint("CP1", new PositionImpl(Collections.emptyMap()));
        readerState2.checkpoint("CP1", new PositionImpl(Collections.emptyMap()));
        assertTrue(stateSynchronizer.getState().isCheckpointComplete("CP1"));
        assertEquals(segments, stateSynchronizer.getState().getPositionsForCompletedCheckpoint("CP1"));
    }

    @Test(timeout = 10000)
    public void testCheckpointUpdatesAssignedSegments() throws ReaderNotInReaderGroupException {
        String scope = "scope";
        String stream = "stream";
        String checkpointId = "checkpoint";
        String reader1 = "reader1";
        String reader2 = "reader2";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        Segment segment0 = new Segment(scope, stream, 0);
        Segment segment1 = new Segment(scope, stream, 1);
        Segment segment2 = new Segment(scope, stream, 2);
        MockController controller = new MockControllerWithSuccessors(endpoint.getEndpoint(), endpoint.getPort(),
                connectionFactory, new StreamSegmentsWithPredecessors(ImmutableMap.of(), ""));
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        @Cleanup
        SynchronizerClientFactory clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory, streamFactory,
                streamFactory, streamFactory, streamFactory);
        SynchronizerConfig config = SynchronizerConfig.builder().build();
        @Cleanup
        StateSynchronizer<ReaderGroupState> stateSynchronizer = createState(stream, clientFactory, config);
        Map<Segment, Long> segments = ImmutableMap.of(segment0, 0L, segment1, 1L, segment2, 2L);
        stateSynchronizer.initialize(new ReaderGroupState.ReaderGroupStateInit(ReaderGroupConfig.builder().stream(Stream.of(scope, stream)).build(),
                segments, Collections.emptyMap()));
        val readerState1 = new ReaderGroupStateManager(reader1, stateSynchronizer, controller, null);
        readerState1.initializeReader(0);
        val readerState2 = new ReaderGroupStateManager(reader2, stateSynchronizer, controller, null);
        readerState2.initializeReader(0);

        // Assert that readers initially got no assigned segments.
        assertNull(readerState1.findSegmentToReleaseIfRequired());
        assertNull(readerState2.findSegmentToReleaseIfRequired());

        // Assert that both readers have acquired all the segments.
        assertEquals(segments.size(), readerState1.acquireNewSegmentsIfNeeded(0).size() +
                readerState2.acquireNewSegmentsIfNeeded(0).size());
        assertEquals(Collections.emptyMap(), stateSynchronizer.getState().getUnassignedSegments());

        // Initialize checkpoint in state synchronizer.
        stateSynchronizer.updateStateUnconditionally(new CreateCheckpoint(checkpointId));
        stateSynchronizer.fetchUpdates();
        assertEquals(checkpointId, readerState1.getCheckpoint());
        assertEquals(checkpointId, readerState2.getCheckpoint());

        // Create some positions for all the segments in the stream > than the initial ones.
        Map<Segment, Long> checkpointPositions = new HashMap<>();
        IntStream.range(0, segments.size()).forEach(segNum -> checkpointPositions.put(new Segment(scope, stream, segNum), 10L));

        // This should update assigned segments offsets with the checkpoint positions.
        readerState1.checkpoint(checkpointId, new PositionImpl(checkpointPositions));
        readerState2.checkpoint(checkpointId, new PositionImpl(checkpointPositions));
        assertTrue(stateSynchronizer.getState().isCheckpointComplete(checkpointId));

        // Verify that assigned getPositions() retrieves the updated segment offsets.
        Map<Stream, Map<Segment, Long>> readergroupPositions = new HashMap<>();
        readergroupPositions.put(Stream.of(scope, stream), checkpointPositions);
        assertEquals(stateSynchronizer.getState().getPositions(), readergroupPositions);
    }
    
    @Test(timeout = 10000)
    public void testSegmentsCannotBeReleasedWithoutCheckpoint() throws ReaderNotInReaderGroupException {
        String scope = "scope";
        String stream = "stream";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        Segment segment0 = new Segment(scope, stream, 0);
        Segment segment1 = new Segment(scope, stream, 1);
        Segment segment2 = new Segment(scope, stream, 2);
        MockController controller = new MockControllerWithSuccessors(endpoint.getEndpoint(), endpoint.getPort(),
                connectionFactory, new StreamSegmentsWithPredecessors(ImmutableMap.of(), ""));
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        @Cleanup
        SynchronizerClientFactory clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory, streamFactory,
                                                            streamFactory, streamFactory, streamFactory);
        SynchronizerConfig config = SynchronizerConfig.builder().build();
        @Cleanup
        StateSynchronizer<ReaderGroupState> stateSynchronizer = createState(stream, clientFactory, config);
        AtomicLong clock = new AtomicLong();
        Map<Segment, Long> segments = ImmutableMap.of(segment0, 0L, segment1, 1L, segment2, 2L);
        stateSynchronizer.initialize(new ReaderGroupState.ReaderGroupStateInit(ReaderGroupConfig.builder().stream(Stream.of(scope, stream)).build(),
                segments, Collections.emptyMap()));
        val readerState1 = new ReaderGroupStateManager("reader1", stateSynchronizer, controller, clock::get);
        readerState1.initializeReader(0);
        val readerState2 = new ReaderGroupStateManager("reader2", stateSynchronizer, controller, clock::get);
        readerState2.initializeReader(0);
        
        assertEquals(segments, stateSynchronizer.getState().getUnassignedSegments());
        stateSynchronizer.updateStateUnconditionally(new CreateCheckpoint("CP1"));
        stateSynchronizer.fetchUpdates();
        assertEquals("CP1", readerState1.getCheckpoint());
        assertEquals(Collections.emptyMap(), readerState1.acquireNewSegmentsIfNeeded(1));
        assertEquals(Collections.emptyMap(), readerState2.acquireNewSegmentsIfNeeded(2));
        assertEquals("CP1", readerState2.getCheckpoint());
        readerState1.checkpoint("CP1", new PositionImpl(Collections.emptyMap()));
        readerState2.checkpoint("CP1", new PositionImpl(Collections.emptyMap()));
        assertEquals(segments, stateSynchronizer.getState().getPositionsForCompletedCheckpoint("CP1"));
        Map<Segment, Long> segments1 = readerState1.acquireNewSegmentsIfNeeded(1);
        Map<Segment, Long> segments2 = readerState2.acquireNewSegmentsIfNeeded(2);
        assertFalse(segments1.isEmpty());
        assertFalse(segments2.isEmpty());
        assertEquals(0, stateSynchronizer.getState().getNumberOfUnassignedSegments());
        
        //Induce imbalance
        for (Entry<Segment, Long> entry : segments1.entrySet()) {            
            stateSynchronizer.updateStateUnconditionally(new ReaderGroupState.ReleaseSegment("reader1", entry.getKey(), entry.getValue()));
            stateSynchronizer.updateStateUnconditionally(new ReaderGroupState.AcquireSegment("reader2", entry.getKey()));
        }
        stateSynchronizer.updateStateUnconditionally(new CreateCheckpoint("CP2"));
        stateSynchronizer.fetchUpdates();
        
        clock.addAndGet(ReaderGroupStateManager.UPDATE_WINDOW.toNanos());
        assertNull(readerState1.findSegmentToReleaseIfRequired());
        assertNull(readerState2.findSegmentToReleaseIfRequired());
        clock.addAndGet(ReaderGroupStateManager.UPDATE_WINDOW.toNanos());
        assertFalse(readerState2.releaseSegment(segments2.keySet().iterator().next(), 20, 2));
        clock.addAndGet(ReaderGroupStateManager.UPDATE_WINDOW.toNanos());
        readerState1.checkpoint("CP2", new PositionImpl(Collections.emptyMap()));
        readerState2.checkpoint("CP2", new PositionImpl(segments));
        assertEquals(segments, stateSynchronizer.getState().getPositionsForCompletedCheckpoint("CP2"));
        Segment toRelease = readerState2.findSegmentToReleaseIfRequired();
        assertNotNull(toRelease);
        assertTrue(readerState2.releaseSegment(toRelease, 10, 1));
        assertEquals(1, stateSynchronizer.getState().getNumberOfUnassignedSegments());
    }
    
    @Test(timeout = 10000)
    public void testReleaseCompletedSegment() throws ReaderNotInReaderGroupException {
        String scope = "scope";
        String stream = "stream";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, false);
        controller.createScope(scope);
        controller.createStream(scope, stream, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(2)).build());
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        @Cleanup
        SynchronizerClientFactory clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory, streamFactory, streamFactory, streamFactory, streamFactory);

        SynchronizerConfig config = SynchronizerConfig.builder().build();
        @Cleanup
        StateSynchronizer<ReaderGroupState> stateSynchronizer = createState(stream, clientFactory, config);
        AtomicLong clock = new AtomicLong();
        Map<Segment, Long> segments = new HashMap<>();
        segments.put(new Segment(scope, stream, 0), 123L);
        segments.put(new Segment(scope, stream, 1), 456L);
        stateSynchronizer.initialize(new ReaderGroupState.ReaderGroupStateInit(ReaderGroupConfig.builder().stream(Stream.of(scope, stream)).build(), segments, Collections.emptyMap()));
        ReaderGroupStateManager readerState1 = new ReaderGroupStateManager("testReader", stateSynchronizer, controller,
                                                                           clock::get);
        readerState1.initializeReader(0);
        Segment toRelease = readerState1.findSegmentToReleaseIfRequired();
        assertNull(toRelease);
        Map<Segment, Long> newSegments = readerState1.acquireNewSegmentsIfNeeded(0);
        assertFalse(newSegments.isEmpty());
        assertEquals(2, newSegments.size());

        ReaderGroupStateManager readerState2 = new ReaderGroupStateManager("testReader2", stateSynchronizer, controller,
                                                                           clock::get);
        readerState2.initializeReader(0);
        clock.addAndGet(ReaderGroupStateManager.UPDATE_WINDOW.toNanos());
        Position pos = new PositionImpl(ImmutableMap.of(new Segment(scope, stream, 0), -1L,
                                                        new Segment(scope, stream, 1), 789L));
        ReaderGroupStateManager.readerShutdown("testReader", pos, stateSynchronizer);

        newSegments = readerState2.acquireNewSegmentsIfNeeded(0);
        assertEquals(1, newSegments.size());
        assertEquals(Long.valueOf(789L), newSegments.get(new Segment(scope, stream, 1)));
        assertEquals(0, stateSynchronizer.getState().getNumberOfUnassignedSegments());
        AssertExtensions.assertThrows(ReaderNotInReaderGroupException.class,
                                      () -> readerState1.acquireNewSegmentsIfNeeded(0L));
    }
}

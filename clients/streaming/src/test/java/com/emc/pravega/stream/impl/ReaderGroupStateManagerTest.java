/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.state.StateSynchronizer;
import com.emc.pravega.state.SynchronizerConfig;
import com.emc.pravega.stream.ReaderGroupConfig;
import com.emc.pravega.stream.ReinitializationRequiredException;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.impl.ReaderGroupState.CreateCheckpoint;
import com.emc.pravega.stream.mock.MockConnectionFactoryImpl;
import com.emc.pravega.stream.mock.MockController;
import com.emc.pravega.stream.mock.MockSegmentStreamFactory;
import com.emc.pravega.testcommon.TestUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

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

    @Test
    public void testSegmentSplit() throws ReinitializationRequiredException {
        String scope = "scope";
        String stream = "stream";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", TestUtils.randomPort());
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl(endpoint);
        Segment initialSegment = new Segment(scope, stream, 0);
        Segment successorA = new Segment(scope, stream, 1);
        Segment successorB = new Segment(scope, stream, 2);
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory) {
            @Override
            public CompletableFuture<Map<Segment, List<Integer>>> getSuccessors(Segment segment) {
                assertEquals(initialSegment, segment);
                return completedFuture(ImmutableMap.of(successorA, singletonList(0), successorB, singletonList(0)));
            }
        };
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        @Cleanup
        ClientFactory clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory, streamFactory, streamFactory);
        SynchronizerConfig config = SynchronizerConfig.builder().build();
        StateSynchronizer<ReaderGroupState> stateSynchronizer = clientFactory.createStateSynchronizer(stream,
                                                                                                      new JavaSerializer<>(),
                                                                                                      new JavaSerializer<>(),
                                                                                                      config);
        Map<Segment, Long> segments = new HashMap<>();
        segments.put(initialSegment, 1L);
        ReaderGroupStateManager.initializeReaderGroup(stateSynchronizer,
                                                      ReaderGroupConfig.builder().build(),
                                                      segments);
        val readerState = new ReaderGroupStateManager("testReader", stateSynchronizer, controller, null);
        readerState.initializeReader();
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

    @Test
    public void testSegmentMerge() throws ReinitializationRequiredException {
        String scope = "scope";
        String stream = "stream";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", TestUtils.randomPort());
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl(endpoint);
        Segment initialSegmentA = new Segment(scope, stream, 0);
        Segment initialSegmentB = new Segment(scope, stream, 1);
        Segment successor = new Segment(scope, stream, 2);
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory) {
            @Override
            public CompletableFuture<Map<Segment, List<Integer>>> getSuccessors(Segment segment) {
                if (segment.getSegmentNumber() == 0) {
                    assertEquals(initialSegmentA, segment);
                } else {
                    assertEquals(initialSegmentB, segment);
                }
                return completedFuture(Collections.singletonMap(successor, ImmutableList.of(0, 1)));
            }
        };
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        @Cleanup
        ClientFactory clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory, streamFactory, streamFactory);
        SynchronizerConfig config = SynchronizerConfig.builder().build();
        StateSynchronizer<ReaderGroupState> stateSynchronizer = clientFactory.createStateSynchronizer(stream,
                                                                                                      new JavaSerializer<>(),
                                                                                                      new JavaSerializer<>(),
                                                                                                      config);
        Map<Segment, Long> segments = new HashMap<>();
        segments.put(initialSegmentA, 1L);
        segments.put(initialSegmentB, 2L);
        ReaderGroupStateManager.initializeReaderGroup(stateSynchronizer,
                                                      ReaderGroupConfig.builder().build(),
                                                      segments);
        val readerState = new ReaderGroupStateManager("testReader", stateSynchronizer, controller, null);
        readerState.initializeReader();
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
    
    @Test
    public void testAddReader() throws ReinitializationRequiredException {
        String scope = "scope";
        String stream = "stream";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", TestUtils.randomPort());
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl(endpoint);
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory);
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        @Cleanup
        ClientFactory clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory, streamFactory, streamFactory);

        SynchronizerConfig config = SynchronizerConfig.builder().build();
        StateSynchronizer<ReaderGroupState> stateSynchronizer = clientFactory.createStateSynchronizer(stream,
                                                                                                      new JavaSerializer<>(),
                                                                                                      new JavaSerializer<>(),
                                                                                                      config);
        Map<Segment, Long> segments = new HashMap<>();
        segments.put(new Segment(scope, stream, 0), 1L);
        ReaderGroupStateManager.initializeReaderGroup(stateSynchronizer,
                                                      ReaderGroupConfig.builder().build(),
                                                      segments);
        ReaderGroupStateManager readerState = new ReaderGroupStateManager("testReader",
                stateSynchronizer,
                controller,
                null);
        readerState.initializeReader();
        Segment toRelease = readerState.findSegmentToReleaseIfRequired();
        assertNull(toRelease);
        Map<Segment, Long> newSegments = readerState.acquireNewSegmentsIfNeeded(0);
        assertFalse(newSegments.isEmpty());
        assertEquals(1, newSegments.size());
        assertTrue(newSegments.containsKey(new Segment(scope, stream, 0)));
        assertEquals(1, newSegments.get(new Segment(scope, stream, 0)).longValue());
    }

    @Test
    public void testSegmentsAssigned() throws ReinitializationRequiredException {
        String scope = "scope";
        String stream = "stream";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", TestUtils.randomPort());
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl(endpoint);
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory);
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        @Cleanup
        ClientFactory clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory, streamFactory, streamFactory);

        SynchronizerConfig config = SynchronizerConfig.builder().build();
        StateSynchronizer<ReaderGroupState> stateSynchronizer = clientFactory.createStateSynchronizer(stream,
                                                                                                      new JavaSerializer<>(),
                                                                                                      new JavaSerializer<>(),
                                                                                                      config);
        AtomicLong clock = new AtomicLong();
        Map<Segment, Long> segments = new HashMap<>();
        segments.put(new Segment(scope, stream, 0), 0L);
        segments.put(new Segment(scope, stream, 1), 1L);
        segments.put(new Segment(scope, stream, 2), 2L);
        segments.put(new Segment(scope, stream, 3), 3L);
        ReaderGroupStateManager.initializeReaderGroup(stateSynchronizer,
                                                      ReaderGroupConfig.builder().build(),
                                                      segments);

        ReaderGroupStateManager reader1 = new ReaderGroupStateManager("reader1",
                stateSynchronizer,
                controller,
                clock::get);
        reader1.initializeReader();

        ReaderGroupStateManager reader2 = new ReaderGroupStateManager("reader2",
                stateSynchronizer,
                controller,
                clock::get);
        reader2.initializeReader();

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

        clock.addAndGet(ReaderGroupStateManager.UPDATE_TIME.toNanos());

        assertFalse(reader1.releaseSegment(new Segment(scope, stream, 0), 0, 0));

        clock.addAndGet(ReaderGroupStateManager.UPDATE_TIME.toNanos());

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

        reader1.initializeReader();
        segments1 = reader1.acquireNewSegmentsIfNeeded(0);
        assertEquals(4, segments1.size());
        assertEquals(segments2, segments1);
    }

    @Test
    public void testReleaseWhenReadersAdded() throws ReinitializationRequiredException {
        String scope = "scope";
        String stream = "stream";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", TestUtils.randomPort());
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl(endpoint);
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory);
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        @Cleanup
        ClientFactory clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory, streamFactory, streamFactory);
        SynchronizerConfig config = SynchronizerConfig.builder().build();
        StateSynchronizer<ReaderGroupState> stateSynchronizer = clientFactory.createStateSynchronizer(stream,
                                                                                                      new JavaSerializer<>(),
                                                                                                      new JavaSerializer<>(),
                                                                                                      config);
        AtomicLong clock = new AtomicLong();
        Map<Segment, Long> segments = new HashMap<>();
        segments.put(new Segment(scope, stream, 0), 0L);
        segments.put(new Segment(scope, stream, 1), 1L);
        segments.put(new Segment(scope, stream, 2), 2L);
        segments.put(new Segment(scope, stream, 3), 3L);
        segments.put(new Segment(scope, stream, 4), 4L);
        segments.put(new Segment(scope, stream, 5), 5L);
        ReaderGroupStateManager.initializeReaderGroup(stateSynchronizer,
                                                      ReaderGroupConfig.builder().build(),
                                                      segments);

        ReaderGroupStateManager reader1 = new ReaderGroupStateManager("reader1",
                stateSynchronizer,
                controller,
                clock::get);
        reader1.initializeReader();
        Map<Segment, Long> segments1 = reader1.acquireNewSegmentsIfNeeded(0);
        assertEquals(6, segments1.size());

        ReaderGroupStateManager reader2 = new ReaderGroupStateManager("reader2",
                stateSynchronizer,
                controller,
                clock::get);
        reader2.initializeReader();
        assertTrue(reader2.acquireNewSegmentsIfNeeded(0).isEmpty());

        assertNull(reader1.findSegmentToReleaseIfRequired());

        clock.addAndGet(ReaderGroupStateManager.UPDATE_TIME.toNanos());

        assertNotNull(reader1.findSegmentToReleaseIfRequired());
        reader1.releaseSegment(new Segment(scope, stream, 3), 3, 0);

        assertNull(reader1.findSegmentToReleaseIfRequired());

        clock.addAndGet(ReaderGroupStateManager.UPDATE_TIME.toNanos());

        assertNotNull(reader1.findSegmentToReleaseIfRequired());
        reader1.releaseSegment(new Segment(scope, stream, 4), 4, 0);

        assertNull(reader1.findSegmentToReleaseIfRequired());

        clock.addAndGet(ReaderGroupStateManager.UPDATE_TIME.toNanos());

        assertNotNull(reader1.findSegmentToReleaseIfRequired());
        reader1.releaseSegment(new Segment(scope, stream, 5), 5, 0);

        assertNull(reader1.findSegmentToReleaseIfRequired());

        clock.addAndGet(ReaderGroupStateManager.UPDATE_TIME.toNanos());

        assertNull(reader1.findSegmentToReleaseIfRequired());

        Map<Segment, Long> segments2 = reader2.acquireNewSegmentsIfNeeded(0);
        assertEquals(3, segments2.size());

        ReaderGroupStateManager reader3 = new ReaderGroupStateManager("reader3",
                stateSynchronizer,
                controller,
                clock::get);
        reader3.initializeReader();
        assertTrue(reader3.acquireNewSegmentsIfNeeded(0).isEmpty());

        assertNotNull(reader1.findSegmentToReleaseIfRequired());
        reader1.releaseSegment(new Segment(scope, stream, 0), 0, 0);
        assertNull(reader1.findSegmentToReleaseIfRequired());

        assertNotNull(reader2.findSegmentToReleaseIfRequired());
        reader2.releaseSegment(new Segment(scope, stream, 3), 3, 0);
        assertNull(reader2.findSegmentToReleaseIfRequired());

        Map<Segment, Long> segments3 = reader3.acquireNewSegmentsIfNeeded(0);
        assertEquals(2, segments3.size());

        clock.addAndGet(ReaderGroupStateManager.UPDATE_TIME.toNanos());

        assertTrue(reader3.acquireNewSegmentsIfNeeded(0).isEmpty());
        assertNull(reader1.findSegmentToReleaseIfRequired());
        assertNull(reader2.findSegmentToReleaseIfRequired());
        assertNull(reader3.findSegmentToReleaseIfRequired());
    }

    @Test
    public void testCheckpoint() throws ReinitializationRequiredException {
        String scope = "scope";
        String stream = "stream";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", TestUtils.randomPort());
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl(endpoint);
        Segment initialSegment = new Segment(scope, stream, 0);
        Segment successorA = new Segment(scope, stream, 1);
        Segment successorB = new Segment(scope, stream, 2);
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory) {
            @Override
            public CompletableFuture<Map<Segment, List<Integer>>> getSuccessors(Segment segment) {
                assertEquals(initialSegment, segment);
                return completedFuture(ImmutableMap.of(successorA, singletonList(0), successorB, singletonList(0)));
            }
        };
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        @Cleanup
        ClientFactory clientFactory = new ClientFactoryImpl(scope,
                                                            controller,
                                                            connectionFactory,
                                                            streamFactory,
                                                            streamFactory);
        SynchronizerConfig config = SynchronizerConfig.builder().build();
        StateSynchronizer<ReaderGroupState> stateSynchronizer = clientFactory.createStateSynchronizer(stream,
                                                                                                      new JavaSerializer<>(),
                                                                                                      new JavaSerializer<>(),
                                                                                                      config);
        Map<Segment, Long> segments = new HashMap<>();
        segments.put(initialSegment, 1L);
        ReaderGroupStateManager.initializeReaderGroup(stateSynchronizer, ReaderGroupConfig.builder().build(), segments);
        val readerState = new ReaderGroupStateManager("testReader", stateSynchronizer, controller, null);
        readerState.initializeReader();
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
    
}

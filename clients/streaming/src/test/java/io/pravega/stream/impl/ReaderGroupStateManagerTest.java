/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.stream.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.pravega.ClientFactory;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.state.StateSynchronizer;
import io.pravega.state.SynchronizerConfig;
import io.pravega.stream.ReaderGroupConfig;
import io.pravega.stream.ReinitializationRequiredException;
import io.pravega.stream.Segment;
import io.pravega.stream.SegmentWithRange;
import io.pravega.stream.StreamSegmentsWithPredecessors;
import io.pravega.stream.impl.ReaderGroupState.CreateCheckpoint;
import io.pravega.stream.mock.MockConnectionFactoryImpl;
import io.pravega.stream.mock.MockController;
import io.pravega.stream.mock.MockSegmentStreamFactory;
import io.pravega.test.common.AssertExtensions;
import lombok.Cleanup;
import lombok.val;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

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

    @Test(timeout = 20000)
    public void testSegmentSplit() throws ReinitializationRequiredException {
        String scope = "scope";
        String stream = "stream";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl(endpoint);
        Segment initialSegment = new Segment(scope, stream, 0);
        Segment successorA = new Segment(scope, stream, 1);
        Segment successorB = new Segment(scope, stream, 2);
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory) {
            @Override
            public CompletableFuture<StreamSegmentsWithPredecessors> getSuccessors(Segment segment) {
                assertEquals(initialSegment, segment);
                return completedFuture(new StreamSegmentsWithPredecessors(
                        ImmutableMap.of(new SegmentWithRange(successorA, 0.0, 0.5), singletonList(0),
                                new SegmentWithRange(successorB, 0.5, 1.0), singletonList(0))));
            }
        };
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        @Cleanup
        ClientFactory clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory, streamFactory,
                streamFactory);
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

    @Test(timeout = 20000)
    public void testSegmentMerge() throws ReinitializationRequiredException {
        String scope = "scope";
        String stream = "stream";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl(endpoint);
        Segment initialSegmentA = new Segment(scope, stream, 0);
        Segment initialSegmentB = new Segment(scope, stream, 1);
        Segment successor = new Segment(scope, stream, 2);
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory) {
            @Override
            public CompletableFuture<StreamSegmentsWithPredecessors> getSuccessors(Segment segment) {
                if (segment.getSegmentNumber() == 0) {
                    assertEquals(initialSegmentA, segment);
                } else {
                    assertEquals(initialSegmentB, segment);
                }
                return completedFuture(new StreamSegmentsWithPredecessors(Collections.singletonMap(new
                        SegmentWithRange(successor, 0.0, 1.0), ImmutableList.of(0, 1))));
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
    
    @Test(timeout = 10000)
    public void testAddReader() throws ReinitializationRequiredException {
        String scope = "scope";
        String stream = "stream";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
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
    
    @Test(timeout = 10000)
    public void testRemoveReader() throws ReinitializationRequiredException {
        String scope = "scope";
        String stream = "stream";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
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
        segments.put(new Segment(scope, stream, 0), 123L);
        segments.put(new Segment(scope, stream, 1), 456L);
        ReaderGroupStateManager.initializeReaderGroup(stateSynchronizer,
                                                      ReaderGroupConfig.builder().build(),
                                                      segments);
        ReaderGroupStateManager readerState1 = new ReaderGroupStateManager("testReader",
                stateSynchronizer,
                controller,
                clock::get);
        readerState1.initializeReader();
        Segment toRelease = readerState1.findSegmentToReleaseIfRequired();
        assertNull(toRelease);
        Map<Segment, Long> newSegments = readerState1.acquireNewSegmentsIfNeeded(0);
        assertFalse(newSegments.isEmpty());
        assertEquals(2, newSegments.size());
        
        ReaderGroupStateManager readerState2 = new ReaderGroupStateManager("testReader2",
                stateSynchronizer,
                controller,
                clock::get);
        readerState2.initializeReader();

        boolean released = readerState1.releaseSegment(new Segment(scope, stream, 0), 789L, 0L);
        assertTrue(released);
        newSegments = readerState2.acquireNewSegmentsIfNeeded(0);
        assertEquals(1, newSegments.size());
        assertEquals(Long.valueOf(789L), newSegments.get(new Segment(scope, stream, 0)));
        
        ReaderGroupStateManager.readerShutdown("testReader2", null, stateSynchronizer);
        AssertExtensions.assertThrows(ReinitializationRequiredException.class,
                () -> readerState2.releaseSegment(new Segment(scope, stream, 0), 711L, 0L));

        clock.addAndGet(ReaderGroupStateManager.UPDATE_WINDOW.toNanos());
        newSegments = readerState1.acquireNewSegmentsIfNeeded(0);
        assertEquals(1, newSegments.size());
        assertEquals(Long.valueOf(789L), newSegments.get(new Segment(scope, stream, 0)));

        AssertExtensions.assertThrows(ReinitializationRequiredException.class,
                () -> readerState2.acquireNewSegmentsIfNeeded(0L));
    }

    @Test(timeout = 10000)
    public void testSegmentsAssigned() throws ReinitializationRequiredException {
        String scope = "scope";
        String stream = "stream";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
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

        reader1.initializeReader();
        segments1 = reader1.acquireNewSegmentsIfNeeded(0);
        assertEquals(4, segments1.size());
        assertEquals(segments2, segments1);
    }

    @Test(timeout = 20000)
    public void testReleaseWhenReadersAdded() throws ReinitializationRequiredException {
        String scope = "scope";
        String stream = "stream";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
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

        clock.addAndGet(ReaderGroupStateManager.UPDATE_WINDOW.toNanos());

        assertTrue(reader3.acquireNewSegmentsIfNeeded(0).isEmpty());
        assertNull(reader1.findSegmentToReleaseIfRequired());
        assertNull(reader2.findSegmentToReleaseIfRequired());
        assertNull(reader3.findSegmentToReleaseIfRequired());
    }

    @Test(timeout = 10000)
    public void testCheckpoint() throws ReinitializationRequiredException {
        String scope = "scope";
        String stream = "stream";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", SERVICE_PORT);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl(endpoint);
        Segment initialSegment = new Segment(scope, stream, 0);
        Segment successorA = new Segment(scope, stream, 1);
        Segment successorB = new Segment(scope, stream, 2);
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory) {
            @Override
            public CompletableFuture<StreamSegmentsWithPredecessors> getSuccessors(Segment segment) {
                assertEquals(initialSegment, segment);
                return completedFuture(new StreamSegmentsWithPredecessors(ImmutableMap.of(
                        new SegmentWithRange(successorA, 0.0, 0.5), singletonList(0),
                        new SegmentWithRange(successorB, 0.5, 1.0), singletonList(0))));
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

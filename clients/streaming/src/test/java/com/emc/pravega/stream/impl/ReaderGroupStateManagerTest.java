/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.state.StateSynchronizer;
import com.emc.pravega.state.SynchronizerConfig;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.mock.MockConnectionFactoryImpl;
import com.emc.pravega.stream.mock.MockController;
import com.emc.pravega.stream.mock.MockSegmentStreamFactory;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import static java.util.Collections.singletonList;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import lombok.val;

public class ReaderGroupStateManagerTest {

    @Test
    public void testSegmentSplit() {
        String scope = "scope";
        String stream = "stream";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 1234);
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
        ClientFactory clientFactory = new ClientFactoryImpl(scope, controller, streamFactory, streamFactory);
        SynchronizerConfig config = new SynchronizerConfig(null, null);
        StateSynchronizer<ReaderGroupState> stateSynchronizer = clientFactory.createStateSynchronizer(stream,
                                                                                                      new JavaSerializer<>(),
                                                                                                      new JavaSerializer<>(),
                                                                                                      config);
        Map<Segment, Long> segments = new HashMap<>();
        segments.put(initialSegment, 1L);
        ReaderGroupStateManager.initializeReadererGroup(stateSynchronizer, segments);
        val readerState = new ReaderGroupStateManager("testReader", stateSynchronizer, controller, null);
        readerState.initializeReader();
        Map<Segment, Long> newSegments = readerState.aquireNewSegmentsIfNeeded(0);
        assertEquals(1, newSegments.size());
        assertEquals(Long.valueOf(1), newSegments.get(initialSegment));
        
        readerState.handleEndOfSegment(initialSegment);
        newSegments = readerState.aquireNewSegmentsIfNeeded(0);
        assertEquals(2, newSegments.size());
        assertEquals(Long.valueOf(0), newSegments.get(successorA));
        assertEquals(Long.valueOf(0), newSegments.get(successorB));
        
        newSegments = readerState.aquireNewSegmentsIfNeeded(0);
        assertTrue(newSegments.isEmpty());
    }

    @Test
    public void testAddReader() {
        String scope = "scope";
        String stream = "stream";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 1234);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl(endpoint);
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory);
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        ClientFactory clientFactory = new ClientFactoryImpl(scope, controller, streamFactory, streamFactory);

        SynchronizerConfig config = new SynchronizerConfig(null, null);
        StateSynchronizer<ReaderGroupState> stateSynchronizer = clientFactory.createStateSynchronizer(stream,
                                                                                                      new JavaSerializer<>(),
                                                                                                      new JavaSerializer<>(),
                                                                                                      config);
        Map<Segment, Long> segments = new HashMap<>();
        segments.put(new Segment(scope, stream, 0), 1L);
        ReaderGroupStateManager.initializeReadererGroup(stateSynchronizer, segments);
        ReaderGroupStateManager readerState = new ReaderGroupStateManager("testReader",
                stateSynchronizer,
                controller,
                null);
        readerState.initializeReader();
        Segment toRelease = readerState.findSegmentToReleaseIfRequired();
        assertNull(toRelease);
        Map<Segment, Long> newSegments = readerState.aquireNewSegmentsIfNeeded(0);
        assertFalse(newSegments.isEmpty());
        assertEquals(1, newSegments.size());
        assertTrue(newSegments.containsKey(new Segment(scope, stream, 0)));
        assertEquals(1, newSegments.get(new Segment(scope, stream, 0)).longValue());
    }

    @Test
    public void testSegmentsAssigned() {
        String scope = "scope";
        String stream = "stream";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 1234);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl(endpoint);
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory);
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        ClientFactory clientFactory = new ClientFactoryImpl(scope, controller, streamFactory, streamFactory);

        SynchronizerConfig config = new SynchronizerConfig(null, null);
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
        ReaderGroupStateManager.initializeReadererGroup(stateSynchronizer, segments);

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

        Map<Segment, Long> segments1 = reader1.aquireNewSegmentsIfNeeded(0);
        assertFalse(segments1.isEmpty());
        assertEquals(2, segments1.size());
        assertTrue(reader1.aquireNewSegmentsIfNeeded(0).isEmpty());
        assertNull(reader1.findSegmentToReleaseIfRequired());

        Map<Segment, Long> segments2 = reader2.aquireNewSegmentsIfNeeded(0);
        assertFalse(segments2.isEmpty());
        assertEquals(2, segments2.size());
        assertTrue(reader2.aquireNewSegmentsIfNeeded(0).isEmpty());
        assertNull(reader2.findSegmentToReleaseIfRequired());

        assertTrue(Sets.intersection(segments1.keySet(), segments2.keySet()).isEmpty());

        clock.addAndGet(ReaderGroupStateManager.UPDATE_TIME.toNanos());

        assertFalse(reader1.releaseSegment(new Segment(scope, stream, 0), 0, 0));

        clock.addAndGet(ReaderGroupStateManager.UPDATE_TIME.toNanos());

        assertTrue(reader1.aquireNewSegmentsIfNeeded(0).isEmpty());
        assertNull(reader1.findSegmentToReleaseIfRequired());
        assertTrue(reader2.aquireNewSegmentsIfNeeded(0).isEmpty());
        assertNull(reader2.findSegmentToReleaseIfRequired());

        reader1.readerShutdown(new PositionImpl(segments1));

        Map<Segment, Long> segmentsRecovered = reader2.aquireNewSegmentsIfNeeded(0);
        assertFalse(segmentsRecovered.isEmpty());
        assertEquals(2, segmentsRecovered.size());
        assertEquals(segments1, segmentsRecovered);
        assertTrue(reader2.aquireNewSegmentsIfNeeded(0).isEmpty());
        assertNull(reader2.findSegmentToReleaseIfRequired());

        segments2.putAll(segmentsRecovered);
        reader2.readerShutdown(new PositionImpl(segments2));

        reader1.initializeReader();
        segments1 = reader1.aquireNewSegmentsIfNeeded(0);
        assertEquals(4, segments1.size());
        assertEquals(segments2, segments1);
    }

    @Test
    public void testReleaseWhenReadersAdded() {
        String scope = "scope";
        String stream = "stream";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 1234);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl(endpoint);
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory);
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        ClientFactory clientFactory = new ClientFactoryImpl(scope, controller, streamFactory, streamFactory);
        SynchronizerConfig config = new SynchronizerConfig(null, null);
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
        ReaderGroupStateManager.initializeReadererGroup(stateSynchronizer, segments);

        ReaderGroupStateManager reader1 = new ReaderGroupStateManager("reader1",
                stateSynchronizer,
                controller,
                clock::get);
        reader1.initializeReader();
        Map<Segment, Long> segments1 = reader1.aquireNewSegmentsIfNeeded(0);
        assertEquals(6, segments1.size());

        ReaderGroupStateManager reader2 = new ReaderGroupStateManager("reader2",
                stateSynchronizer,
                controller,
                clock::get);
        reader2.initializeReader();
        assertTrue(reader2.aquireNewSegmentsIfNeeded(0).isEmpty());

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

        Map<Segment, Long> segments2 = reader2.aquireNewSegmentsIfNeeded(0);
        assertEquals(3, segments2.size());

        ReaderGroupStateManager reader3 = new ReaderGroupStateManager("reader3",
                stateSynchronizer,
                controller,
                clock::get);
        reader3.initializeReader();
        assertTrue(reader3.aquireNewSegmentsIfNeeded(0).isEmpty());

        assertNotNull(reader1.findSegmentToReleaseIfRequired());
        reader1.releaseSegment(new Segment(scope, stream, 0), 0, 0);
        assertNull(reader1.findSegmentToReleaseIfRequired());

        assertNotNull(reader2.findSegmentToReleaseIfRequired());
        reader2.releaseSegment(new Segment(scope, stream, 3), 3, 0);
        assertNull(reader2.findSegmentToReleaseIfRequired());

        Map<Segment, Long> segments3 = reader3.aquireNewSegmentsIfNeeded(0);
        assertEquals(2, segments3.size());

        clock.addAndGet(ReaderGroupStateManager.UPDATE_TIME.toNanos());

        assertTrue(reader3.aquireNewSegmentsIfNeeded(0).isEmpty());
        assertNull(reader1.findSegmentToReleaseIfRequired());
        assertNull(reader2.findSegmentToReleaseIfRequired());
        assertNull(reader3.findSegmentToReleaseIfRequired());
    }

}

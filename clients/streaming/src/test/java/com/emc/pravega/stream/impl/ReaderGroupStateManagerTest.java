package com.emc.pravega.stream.impl;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.state.StateSynchronizer;
import com.emc.pravega.state.SynchronizerConfig;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Sequence;
import com.emc.pravega.stream.mock.MockConnectionFactoryImpl;
import com.emc.pravega.stream.mock.MockController;
import com.emc.pravega.stream.mock.MockSegmentStreamFactory;
import com.emc.pravega.stream.mock.MockStreamManager;
import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ReaderGroupStateManagerTest {

    @Test
    public void testAddReader() {
        String scope = "scope";
        String stream = "stream";
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", 1234);
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl(endpoint);
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory);
        MockStreamManager streamManager = new MockStreamManager(scope, controller);
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        ClientFactory clientFactory = new ClientFactoryImpl(scope,
                controller,
                streamManager,
                streamFactory,
                streamFactory);

        SynchronizerConfig config = new SynchronizerConfig(null, null);
        StateSynchronizer<ReaderGroupState> stateSynchronizer = clientFactory.createStateSynchronizer(stream,
                                                                                                      new JavaSerializer<>(),
                                                                                                      new JavaSerializer<>(),
                                                                                                      config);
        ReaderGroupStateManager stateManager = new ReaderGroupStateManager("testReader", stateSynchronizer, controller, null);
        Map<Segment, Long> segments = new HashMap<>();
        segments.put(new Segment(scope, stream, 0), 1L);
        stateManager.initializeReadererGroup(segments);
        stateManager.initializeReader();
        Segment toRelease = stateManager.findSegmentToReleaseIfRequired();
        assertNull(toRelease);
        Map<Segment, Long> newSegments = stateManager.aquireNewSegmentsIfNeeded(Sequence.create(0L, 0L));
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
        MockStreamManager streamManager = new MockStreamManager(scope, controller);
        MockSegmentStreamFactory streamFactory = new MockSegmentStreamFactory();
        ClientFactory clientFactory = new ClientFactoryImpl(scope,
                controller,
                streamManager,
                streamFactory,
                streamFactory);

        SynchronizerConfig config = new SynchronizerConfig(null, null);
        StateSynchronizer<ReaderGroupState> stateSynchronizer = clientFactory.createStateSynchronizer(stream,
                                                                                                      new JavaSerializer<>(),
                                                                                                      new JavaSerializer<>(),
                                                                                                      config);
        Sequence sequence = Sequence.create(0L, 0L);
        AtomicLong clock = new AtomicLong();
        Map<Segment, Long> segments = new HashMap<>();
        segments.put(new Segment(scope, stream, 0), 0L);
        segments.put(new Segment(scope, stream, 1), 1L);
        segments.put(new Segment(scope, stream, 2), 2L);
        segments.put(new Segment(scope, stream, 3), 3L);
        
        ReaderGroupStateManager reader1 = new ReaderGroupStateManager("reader1", stateSynchronizer, controller, clock::get);
        reader1.initializeReadererGroup(segments);
        reader1.initializeReader();
        
        ReaderGroupStateManager reader2 = new ReaderGroupStateManager("reader2", stateSynchronizer, controller, clock::get);
        reader2.initializeReadererGroup(segments);
        reader2.initializeReader();
        
        Map<Segment, Long> segments1 = reader1.aquireNewSegmentsIfNeeded(sequence);
        assertFalse(segments1.isEmpty());
        assertEquals(2, segments1.size());
        assertNull(reader1.aquireNewSegmentsIfNeeded(sequence));
        assertNull(reader1.findSegmentToReleaseIfRequired());
        
        Map<Segment, Long> segments2 = reader2.aquireNewSegmentsIfNeeded(sequence);
        assertFalse(segments2.isEmpty());
        assertEquals(2, segments2.size());
        assertNull(reader2.aquireNewSegmentsIfNeeded(sequence));
        assertNull(reader2.findSegmentToReleaseIfRequired());
        
        assertTrue(Sets.intersection(segments1.keySet(), segments2.keySet()).isEmpty());
        
        clock.addAndGet(ReaderGroupStateManager.ASSUMED_LAG_MILLIS * 1000*1000);
        
        assertFalse(reader1.releaseSegment(new Segment(scope, stream, 0), 0, sequence));
              
        clock.addAndGet(ReaderGroupStateManager.ASSUMED_LAG_MILLIS * 1000*1000);
        
        assertNull(reader1.aquireNewSegmentsIfNeeded(sequence));
        assertNull(reader1.findSegmentToReleaseIfRequired());
        assertNull(reader2.aquireNewSegmentsIfNeeded(sequence));
        assertNull(reader2.findSegmentToReleaseIfRequired());
        
        reader1.readerShutdown(new PositionImpl(segments1));
        
        Map<Segment, Long> segmentsRecovered = reader2.aquireNewSegmentsIfNeeded(sequence);
        assertFalse(segmentsRecovered.isEmpty());
        assertEquals(2, segmentsRecovered.size());
        assertEquals(segments1, segmentsRecovered);
        assertNull(reader2.aquireNewSegmentsIfNeeded(sequence));
        assertNull(reader2.findSegmentToReleaseIfRequired());
        
        segments2.putAll(segmentsRecovered);
        reader2.readerShutdown(new PositionImpl(segments2));
        
        reader1.initializeReader();
        segments1 = reader1.aquireNewSegmentsIfNeeded(sequence);
        assertEquals(4, segments1.size());
        assertEquals(segments2, segments1);
    }

} 

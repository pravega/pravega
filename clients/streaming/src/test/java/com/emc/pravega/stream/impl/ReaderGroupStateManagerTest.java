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

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import static org.junit.Assert.*;

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
        ReaderGroupStateManager stateManager = new ReaderGroupStateManager("testReader", stateSynchronizer, controller);
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
}

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
package io.pravega.test.integration.endtoendtest;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.stream.impl.StreamSegments;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.controller.server.eventProcessor.LocalController;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.IndexAppendProcessor;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.utils.ControllerWrapper;
import java.net.URI;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Cleanup;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EndToEndIndexSegmentTest {

    static final Duration TIMEOUT = Duration.ofMinutes(1);
    private final String serviceHost = "localhost";
    private final int containerCount = 4;
    private int controllerPort;
    private URI controllerURI;
    private TestingServer zkTestServer;
    private PravegaConnectionListener server;
    private ControllerWrapper controllerWrapper;
    private ServiceBuilder serviceBuilder;
    private ScheduledExecutorService executor;
    private StreamSegmentStore store;

    @Before
    public void setUp() throws Exception {
        executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "test");
        zkTestServer = new TestingServerStarter().start();

        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();
        controllerPort = TestUtils.getAvailableListenPort();
        controllerURI = URI.create("tcp://" + serviceHost + ":" + controllerPort);
        int servicePort = TestUtils.getAvailableListenPort();
        server = new PravegaConnectionListener(false, servicePort, store, tableStore, this.serviceBuilder.getLowPriorityExecutor(),
                new IndexAppendProcessor(this.serviceBuilder.getLowPriorityExecutor(), store));
        server.startListening();

        controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(),
                false,
                controllerPort,
                serviceHost,
                servicePort,
                containerCount);
        controllerWrapper.awaitRunning();
    }

    @After
    public void tearDown() throws Exception {
        ExecutorServiceHelpers.shutdown(executor);
        controllerWrapper.close();
        server.close();
        serviceBuilder.close();
        zkTestServer.close();
    }


    @Test(timeout = 20000)
    public void testTruncationOfIndexSegment() throws Exception {
        StreamConfiguration config = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        String streamName = "testTruncationOfIndexSegment";

        @Cleanup
        StreamManager streamManager = StreamManager.create(controllerURI);
        String scope = "test";
        streamManager.createScope(scope);
        streamManager.createStream(scope, streamName, config);

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope,
                ClientConfig.builder().controllerURI(controllerURI).build());
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        writer.writeEvent("0", "Hello world\n").get();
        writer.writeEvent("0", "Hello world\n").get();
        writer.writeEvent("0", "Hello world\n").get();
        writer.writeEvent("0", "Hello world\n").get();

        Stream stream = new StreamImpl("test", streamName);
        LocalController controller = (LocalController) controllerWrapper.getController();

        Map<Long, Long> streamCutPositions = new HashMap<>();
        streamCutPositions.put(0L, 108L);

        StreamSegments streamSegments = controller.getCurrentSegments(scope, streamName).join();
        Collection<Segment> segments = streamSegments.getSegments();
        assertEquals(1, segments.size());
        Segment segment = segments.iterator().next();
        //Validating starting offset of the main and index segment before truncation
        assertEquals(0, store.getStreamSegmentInfo(segment.getScopedName(), TIMEOUT).join().getStartOffset());
        assertEquals(0, store.getStreamSegmentInfo(NameUtils.getIndexSegmentName(segment.getScopedName()), TIMEOUT).join().getStartOffset());

        controller.truncateStream(stream.getScope(), stream.getStreamName(), streamCutPositions).join();
        //Validating starting offset of the main and index segment after truncation
        assertEquals(108L, store.getStreamSegmentInfo(segment.getScopedName(), TIMEOUT).join().getStartOffset());
        long indexOffset = store.getStreamSegmentInfo(NameUtils.getIndexSegmentName(segment.getScopedName()), TIMEOUT).join().getStartOffset();
        assertTrue("Index was: " + indexOffset, indexOffset > 0);
        assertTrue("Index was: " + indexOffset, indexOffset % 24 == 0);
    }
}

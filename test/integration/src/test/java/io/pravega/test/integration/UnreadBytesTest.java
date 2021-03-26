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
package io.pravega.test.integration;

import com.google.common.collect.ImmutableMap;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.common.ThreadPooledTestSuite;
import io.pravega.test.integration.demo.ControllerWrapper;
import java.net.URI;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class UnreadBytesTest extends ThreadPooledTestSuite {

    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final URI controllerUri = URI.create("tcp://localhost:" + String.valueOf(controllerPort));
    private final String serviceHost = "localhost";
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int containerCount = 4;
    private TestingServer zkTestServer;
    private PravegaConnectionListener server;
    private ControllerWrapper controllerWrapper;
    private ServiceBuilder serviceBuilder;

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    @Before
    public void setUp() throws Exception {
        zkTestServer = new TestingServerStarter().start();

        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();

        server = new PravegaConnectionListener(false, servicePort, store, tableStore, serviceBuilder.getLowPriorityExecutor());
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
        controllerWrapper.close();
        server.close();
        serviceBuilder.close();
        zkTestServer.close();
    }

    @Test(timeout = 50000)
    public void testUnreadBytes() throws Exception {
        StreamConfiguration config = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 1))
                .build();

        Controller controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope("unreadbytes").get();
        controller.createStream("unreadbytes", "unreadbytes", config).get();

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope("unreadbytes", ClientConfig.builder().controllerURI(controllerUri).build());
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter("unreadbytes", new JavaSerializer<>(),
                EventWriterConfig.builder().build());

        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope("unreadbytes",  ClientConfig.builder().controllerURI(controllerUri).build());
        groupManager.createReaderGroup("group", ReaderGroupConfig.builder().disableAutomaticCheckpoints().stream("unreadbytes/unreadbytes").build());
        @Cleanup
        ReaderGroup readerGroup = groupManager.getReaderGroup("group");

        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader("readerId", "group", new JavaSerializer<>(),
                ReaderConfig.builder().build());
        long unreadBytes = readerGroup.getMetrics().unreadBytes();
        assertTrue("Unread bvtes: " + unreadBytes, unreadBytes == 0);

        writer.writeEvent("0", "data of size 30").get();
        writer.writeEvent("0", "data of size 30").get();

        EventRead<String> firstEvent = reader.readNextEvent(15000);
        EventRead<String> secondEvent = reader.readNextEvent(15000);
        assertNotNull(firstEvent);
        assertEquals("data of size 30", firstEvent.getEvent());
        assertNotNull(secondEvent);
        assertEquals("data of size 30", secondEvent.getEvent());

        // trigger a checkpoint.
        CompletableFuture<Checkpoint> chkPointResult = readerGroup.initiateCheckpoint("test", executorService());
        EventRead<String> chkpointEvent = reader.readNextEvent(15000);
        assertEquals("test", chkpointEvent.getCheckpointName());
        
        EventRead<String> emptyEvent = reader.readNextEvent(100);
        assertEquals(false, emptyEvent.isCheckpoint());
        assertEquals(null, emptyEvent.getEvent());
        chkPointResult.join();

        unreadBytes = readerGroup.getMetrics().unreadBytes();
        assertTrue("Unread bvtes: " + unreadBytes, unreadBytes == 0);

        writer.writeEvent("0", "data of size 30").get();
        unreadBytes = readerGroup.getMetrics().unreadBytes();
        assertTrue("Unread bytes: " + unreadBytes, unreadBytes == 30);
    }


    @Test(timeout = 50000)
    public void testUnreadBytesWithEndStreamCuts() throws Exception {
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 1))
                                                        .build();

        Controller controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope("unreadbytes").get();
        controller.createStream("unreadbytes", "unreadbytes", config).get();

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope("unreadbytes", ClientConfig.builder().controllerURI(controllerUri).build());
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter("unreadbytes", new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        //Write just 2 events to simplify simulating a checkpoint.
        writer.writeEvent("0", "data of size 30").get();
        writer.writeEvent("0", "data of size 30").get();

        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope("unreadbytes",  ClientConfig.builder().controllerURI(controllerUri).build());
        //create a bounded reader group.
        groupManager.createReaderGroup("group", ReaderGroupConfig
                .builder().disableAutomaticCheckpoints().stream("unreadbytes/unreadbytes", StreamCut.UNBOUNDED,
                        getStreamCut("unreadbytes", 90L, 0)).build());

        ReaderGroup readerGroup = groupManager.getReaderGroup("group");
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader("readerId", "group", new JavaSerializer<>(),
                ReaderConfig.builder().build());

        EventRead<String> firstEvent = reader.readNextEvent(15000);
        EventRead<String> secondEvent = reader.readNextEvent(15000);
        assertNotNull(firstEvent);
        assertEquals("data of size 30", firstEvent.getEvent());
        assertNotNull(secondEvent);
        assertEquals("data of size 30", secondEvent.getEvent());

        // trigger a checkpoint.
        CompletableFuture<Checkpoint> chkPointResult = readerGroup.initiateCheckpoint("test", executorService());
        EventRead<String> chkpointEvent = reader.readNextEvent(15000);
        assertEquals("test", chkpointEvent.getCheckpointName());
        
        EventRead<String> emptyEvent = reader.readNextEvent(100);
        assertEquals(false, emptyEvent.isCheckpoint());
        assertEquals(null, emptyEvent.getEvent());
        
        chkPointResult.join();

        //Writer events, to ensure 120Bytes are written.
        writer.writeEvent("0", "data of size 30").get();
        writer.writeEvent("0", "data of size 30").get();

        long unreadBytes = readerGroup.getMetrics().unreadBytes();
        //Ensure the endoffset of 90 Bytes is taken into consideration when computing unread
        assertTrue("Unread bvtes: " + unreadBytes, unreadBytes == 30);
    }

    @Test
    public void testUnreadBytesWithCheckpointsAndStreamCuts() throws Exception {
        StreamConfiguration config = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 1))
                .build();

        Controller controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope("unreadbytes").get();
        controller.createStream("unreadbytes", "unreadbytes", config).get();

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope("unreadbytes", ClientConfig.builder().controllerURI(controllerUri).build());
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter("unreadbytes", new JavaSerializer<>(),
                EventWriterConfig.builder().build());

        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope("unreadbytes",  ClientConfig.builder().controllerURI(controllerUri).build());
        groupManager.createReaderGroup("group", ReaderGroupConfig.builder().disableAutomaticCheckpoints().stream("unreadbytes/unreadbytes").build());
        @Cleanup
        ReaderGroup readerGroup = groupManager.getReaderGroup("group");

        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader("readerId", "group", new JavaSerializer<>(),
                ReaderConfig.builder().build());
        long unreadBytes = readerGroup.getMetrics().unreadBytes();
        assertTrue("Unread bvtes: " + unreadBytes, unreadBytes == 0);

        writer.writeEvent("0", "data of size 30").get();
        writer.writeEvent("0", "data of size 30").get();

        EventRead<String> firstEvent = reader.readNextEvent(15000);
        EventRead<String> secondEvent = reader.readNextEvent(15000);
        assertNotNull(firstEvent);
        assertEquals("data of size 30", firstEvent.getEvent());
        assertNotNull(secondEvent);
        assertEquals("data of size 30", secondEvent.getEvent());

        // trigger a checkpoint.
        CompletableFuture<Checkpoint> chkPointResult = readerGroup.initiateCheckpoint("test", executorService());
        EventRead<String> chkpointEvent = reader.readNextEvent(15000);
        assertEquals("test", chkpointEvent.getCheckpointName());

        EventRead<String> emptyEvent = reader.readNextEvent(100);
        assertEquals(false, emptyEvent.isCheckpoint());
        assertEquals(null, emptyEvent.getEvent());
        chkPointResult.join();

        unreadBytes = readerGroup.getMetrics().unreadBytes();
        assertTrue("Unread bvtes: " + unreadBytes, unreadBytes == 0);

        // starting from checkpoint "test", data of size 30 is read
        writer.writeEvent("0", "data of size 30").get();
        unreadBytes = readerGroup.getMetrics().unreadBytes();
        assertTrue("Unread bytes: " + unreadBytes, unreadBytes == 30);

        // trigger a stream-cut
        CompletableFuture<Map<Stream, StreamCut>> scResult = readerGroup.generateStreamCuts(executorService());
        EventRead<String> scEvent = reader.readNextEvent(15000);

        reader.readNextEvent(100);

        unreadBytes = readerGroup.getMetrics().unreadBytes();
        assertTrue("Unread bvtes: " + unreadBytes, unreadBytes == 30);

        // starting from checkpoint "test", data of size 60 is written => stream-cut does not change last checkpointed position
        writer.writeEvent("0", "data of size 30").get();
        unreadBytes = readerGroup.getMetrics().unreadBytes();
        assertTrue("Unread bytes: " + unreadBytes, unreadBytes == 60);
    }

    /*
     * Test method to create StreamCuts. In the real world StreamCuts are obtained via the Pravega client apis.
     */
    private StreamCut getStreamCut(String streamName, long offset, int... segmentNumbers) {
        ImmutableMap.Builder<Segment, Long> builder = ImmutableMap.<Segment, Long>builder();
        Arrays.stream(segmentNumbers).forEach(seg -> {
            builder.put(new Segment("unreadbytes", streamName, seg), offset);
        });

        return new StreamCutImpl(Stream.of("unreadbytes", streamName), builder.build());
    }
}

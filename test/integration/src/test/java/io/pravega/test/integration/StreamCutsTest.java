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

import com.google.common.collect.ImmutableSet;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.Controller;
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
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.IndexAppendProcessor;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.utils.ControllerWrapper;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.shared.NameUtils.computeSegmentId;
import static io.pravega.shared.NameUtils.getQualifiedStreamSegmentName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Slf4j
public class StreamCutsTest {

    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final String serviceHost = "localhost";
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int containerCount = 4;
    private TestingServer zkTestServer;
    private PravegaConnectionListener server;
    private ControllerWrapper controllerWrapper;
    private ServiceBuilder serviceBuilder;
    private ScheduledExecutorService executor;

    @Before
    public void setUp() throws Exception {
        executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "test");
        zkTestServer = new TestingServerStarter().start();

        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();

        server = new PravegaConnectionListener(false, servicePort, store, tableStore, serviceBuilder.getLowPriorityExecutor(),
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));
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

    @Test(timeout = 40000)
    public void testReaderGroupCuts() throws Exception {
        StreamConfiguration config = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 1))
                .build();
        Controller controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope("test", 0L).get();
        controller.createStream("test", "test", config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder().build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl("test", controller, connectionFactory);
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter("test", new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        writer.writeEvent("0", "fpj was here").get();
        writer.writeEvent("0", "fpj was here again").get();

        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl("test", controller, clientFactory);
        groupManager.createReaderGroup("cuts", ReaderGroupConfig
                .builder().disableAutomaticCheckpoints().stream("test/test").groupRefreshTimeMillis(0).build());
        @Cleanup
        ReaderGroup readerGroup = groupManager.getReaderGroup("cuts");
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader("readerId", "cuts", new JavaSerializer<>(),
                ReaderConfig.builder().initialAllocationDelay(0).build());

        EventRead<String> firstEvent = reader.readNextEvent(5000);
        assertNotNull(firstEvent.getEvent());
        assertEquals("fpj was here", firstEvent.getEvent());
        readerGroup.initiateCheckpoint("cp1", executor); 
        EventRead<String> cpEvent = reader.readNextEvent(5000);
        assertEquals("cp1", cpEvent.getCheckpointName());
        EventRead<String> secondEvent = reader.readNextEvent(5000);
        assertNotNull(secondEvent.getEvent());
        assertEquals("fpj was here again", secondEvent.getEvent());

        Map<Stream, StreamCut> cuts = readerGroup.getStreamCuts();
        validateCuts(readerGroup, cuts, Collections.singleton(getQualifiedStreamSegmentName("test", "test", 0L)));

        // Scale the stream to verify that we get more segments in the cut.
        Stream stream = Stream.of("test", "test");
        Map<Double, Double> map = new HashMap<>();
        map.put(0.0, 0.5);
        map.put(0.5, 1.0);
        Boolean result = controller.scaleStream(stream, Collections.singletonList(0L), map, executor).getFuture().get();
        assertTrue(result);
        log.info("Finished 1st scaling");
        writer.writeEvent("0", "fpj was here again0").get();
        writer.writeEvent("1", "fpj was here again1").get();
        EventRead<String> eosEvent = reader.readNextEvent(100); 
        assertNull(eosEvent.getEvent()); //Reader does not yet see the data becasue there has been no CP
        CompletableFuture<Checkpoint> checkpoint = readerGroup.initiateCheckpoint("cp2", executor); 
        cpEvent = reader.readNextEvent(100);
        EventRead<String> event0 = reader.readNextEvent(100);
        EventRead<String> event1 = reader.readNextEvent(100);
        cuts = checkpoint.get(5, TimeUnit.SECONDS).asImpl().getPositions();
        //Validate the reader did not release the segments before the checkpoint.
        //This is important because it means that once the checkpoint is initiated no segments change readers.
        Set<String> segmentNames = ImmutableSet.of(getQualifiedStreamSegmentName("test", "test",
                                                                                 computeSegmentId(0, 0)));
        validateCuts(readerGroup, cuts, segmentNames);

        CompletableFuture<Map<Stream, StreamCut>> futureCuts = readerGroup.generateStreamCuts(executor);
        EventRead<String> emptyEvent = reader.readNextEvent(100);
        cuts = futureCuts.get();
        segmentNames = ImmutableSet.of(getQualifiedStreamSegmentName("test", "test", computeSegmentId(1, 1)),
                                       getQualifiedStreamSegmentName("test", "test", computeSegmentId(2, 1)));
        validateCuts(readerGroup, cuts, segmentNames);
        
        // Scale down to verify that the number drops back.
        map = new HashMap<>();
        map.put(0.0, 1.0);
        ArrayList<Long> toSeal = new ArrayList<>();
        toSeal.add(computeSegmentId(1, 1));
        toSeal.add(computeSegmentId(2, 1));
        result = controller.scaleStream(stream, Collections.unmodifiableList(toSeal), map, executor).getFuture().get();
        assertTrue(result);
        log.info("Finished 2nd scaling");
        writer.writeEvent("0", "fpj was here again2").get();

        emptyEvent = reader.readNextEvent(100); //Reader sees the segment is empty
        assertNull(emptyEvent.getEvent());
        checkpoint = readerGroup.initiateCheckpoint("cp3", executor);
        cpEvent = reader.readNextEvent(100);
        assertEquals("cp3", cpEvent.getCheckpointName());
        event0 = reader.readNextEvent(5000); //Reader releases segments here
        assertTrue(event0.getEvent().endsWith("2"));

        cuts = readerGroup.getStreamCuts();
        long three = computeSegmentId(3, 2);
        validateCuts(readerGroup, cuts, Collections.singleton(getQualifiedStreamSegmentName("test", "test", three)));

        // Scale up to 4 segments again.
        map = new HashMap<>();
        map.put(0.0, 0.25);
        map.put(0.25, 0.5);
        map.put(0.5, 0.75);
        map.put(0.75, 1.0);
        result = controller.scaleStream(stream, Collections.singletonList(three), map, executor).getFuture().get();
        assertTrue(result);
        log.info("Finished 3rd scaling");
        writer.writeEvent("0", "fpj was here again3").get();
        
        emptyEvent = reader.readNextEvent(100); //Reader sees the segment is empty
        assertNull(emptyEvent.getEvent());
        readerGroup.initiateCheckpoint("cp4", executor);
        cpEvent = reader.readNextEvent(1000);
        assertEquals("cp4", cpEvent.getCheckpointName());
        event0 = reader.readNextEvent(5000); //Reader releases segments here
        assertNotNull(event0.getEvent());

        cuts = readerGroup.getStreamCuts();
        segmentNames = new HashSet<>();
        long four = computeSegmentId(4, 3);
        long five = computeSegmentId(5, 3);
        long six = computeSegmentId(6, 3);
        long seven = computeSegmentId(7, 3);
        segmentNames.add(getQualifiedStreamSegmentName("test", "test", four));
        segmentNames.add(getQualifiedStreamSegmentName("test", "test", five));
        segmentNames.add(getQualifiedStreamSegmentName("test", "test", six));
        segmentNames.add(getQualifiedStreamSegmentName("test", "test", seven));
        validateCuts(readerGroup, cuts, Collections.unmodifiableSet(segmentNames));
    }

    private void validateCuts(ReaderGroup group, Map<Stream, StreamCut> cuts, Set<String> segmentNames) {
        Set<String> streamNames = group.getStreamNames();
        cuts.forEach((s, c) -> {
                assertTrue(streamNames.contains(s.getScopedName()));
                assertTrue(((StreamCutImpl) c).validate(segmentNames));
        });
    }
}

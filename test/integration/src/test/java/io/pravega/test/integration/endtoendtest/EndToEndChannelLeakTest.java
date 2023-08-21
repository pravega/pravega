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
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
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
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.IndexAppendProcessor;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.InlineExecutor;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.utils.ControllerWrapper;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertEventuallyEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Slf4j
public class EndToEndChannelLeakTest {

    private static final String SCOPE = "test";
    private static final String STREAM_NAME = "test";
    private static final String READER_GROUP = "reader";
    private static final long ASSERT_TIMEOUT = 10000;

    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final String serviceHost = "localhost";
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int containerCount = 4;
    private final JavaSerializer<String> serializer = new JavaSerializer<>();
    private final EventWriterConfig writerConfig = EventWriterConfig.builder().enableConnectionPooling(true).build();
    private TestingServer zkTestServer;
    private PravegaConnectionListener server;
    private ControllerWrapper controllerWrapper;
    private ServiceBuilder serviceBuilder;
    private ScheduledExecutorService executor;

    @Before
    public void setUp() throws Exception {
        executor = new InlineExecutor();
        zkTestServer = new TestingServerStarter().start();

        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();

        server = new PravegaConnectionListener(false, servicePort, store, tableStore, this.serviceBuilder.getLowPriorityExecutor(),
                new IndexAppendProcessor(this.serviceBuilder.getLowPriorityExecutor(), store));
        server.startListening();

        controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(),
                false, controllerPort, serviceHost, servicePort, containerCount);
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

    @Test(timeout = 30000)
    public void testDetectChannelLeakSegmentSealedPooled() throws Exception {
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.fixed(1))
                                                        .build();
        Controller controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope(SCOPE, 0L).get();
        controller.createStream(SCOPE, STREAM_NAME, config).get();
        //Set the max number connections to verify channel creation behaviour
        final ClientConfig clientConfig = ClientConfig.builder().maxConnectionsPerSegmentStore(5).build();

        @Cleanup
        SocketConnectionFactoryImpl connectionFactory = new SocketConnectionFactoryImpl(clientConfig, new InlineExecutor());
        @Cleanup
        ConnectionPoolImpl connectionPool = new ConnectionPoolImpl(clientConfig, connectionFactory);
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE, controller, connectionPool);

        //Create a writer.
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(SCOPE, serializer, writerConfig);

        //Write an event.
        writer.writeEvent("0", "zero").get();
        
        assertChannelCount(1, connectionPool, connectionFactory);

        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl(SCOPE, controller, clientFactory);
        groupManager.createReaderGroup(READER_GROUP, ReaderGroupConfig.builder().disableAutomaticCheckpoints().groupRefreshTimeMillis(0)
                                       .stream(Stream.of(SCOPE, STREAM_NAME)).build());
        
        @Cleanup
        EventStreamReader<String> reader1 = clientFactory.createReader("readerId1", READER_GROUP, serializer,
                                                                       ReaderConfig.builder().disableTimeWindows(true).build());

        //Read an event.
        EventRead<String> event = reader1.readNextEvent(10000);
        assertEquals("zero", event.getEvent());

        // scale
        Stream stream = new StreamImpl(SCOPE, SCOPE);
        Map<Double, Double> map = new HashMap<>();
        map.put(0.0, 0.33);
        map.put(0.33, 0.66);
        map.put(0.66, 1.0);
        Boolean result = controller.scaleStream(stream, Collections.singletonList(0L), map, executor).getFuture().get();
        assertTrue(result);

        event = reader1.readNextEvent(0);
        assertNull(event.getEvent());

        @Cleanup
        ReaderGroup readerGroup = groupManager.getReaderGroup(READER_GROUP);
        readerGroup.initiateCheckpoint("cp", executor);

        event = reader1.readNextEvent(5000);
        assertEquals("cp", event.getCheckpointName());

        //Write more events.
        writer.writeEvent("0", "one").get();
        writer.writeEvent("0", "two").get();
        writer.writeEvent("1", "three").get();

        event = reader1.readNextEvent(10000);
        assertNotNull(event.getEvent());

        assertChannelCount(5, connectionPool, connectionFactory);

        event = reader1.readNextEvent(10000);
        assertNotNull(event.getEvent());
        assertChannelCount(5, connectionPool, connectionFactory);

        event = reader1.readNextEvent(10000);
        assertNotNull(event.getEvent());
        assertChannelCount(5, connectionPool, connectionFactory);
    }

    @Test(timeout = 30000)
    public void testDetectChannelLeakMultiReaderPooled() throws Exception {
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 1))
                                                        .build();
        //Set the max number connections to verify channel creation behaviour
        final ClientConfig clientConfig = ClientConfig.builder().maxConnectionsPerSegmentStore(5).build();

        Controller controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope(SCOPE, 0L).get();
        controller.createStream(SCOPE, STREAM_NAME, config).get();
        @Cleanup
        SocketConnectionFactoryImpl connectionFactory = new SocketConnectionFactoryImpl(clientConfig, executor);
        @Cleanup
        ConnectionPoolImpl connectionPool = new ConnectionPoolImpl(clientConfig, connectionFactory);
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE, controller, connectionPool);
        int expectedChannelCount = 0; // open socket count.
        assertChannelCount(expectedChannelCount, connectionPool, connectionFactory);
        
        //Create a writer and write an event.
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(STREAM_NAME, serializer, writerConfig);
        writer.writeEvent("0", "zero").get();

        expectedChannelCount += 1; // connection to segment 0.
        assertChannelCount(expectedChannelCount, connectionPool, connectionFactory);
        
        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl(SCOPE, controller, clientFactory);
        assertChannelCount(expectedChannelCount, connectionPool, connectionFactory); // no changes expected.
      
        groupManager.createReaderGroup(READER_GROUP, ReaderGroupConfig.builder().disableAutomaticCheckpoints().groupRefreshTimeMillis(0)
                                       .stream(Stream.of(SCOPE, STREAM_NAME)).build());

        //create a reader and read an event.
        @Cleanup
        EventStreamReader<String> reader1 = clientFactory.createReader("readerId1", READER_GROUP, serializer,
                ReaderConfig.builder().disableTimeWindows(true).build());
        //Creating a reader spawns a revisioned stream client which opens 4 sockets ( read, write, metadataClient and conditionalUpdates).
        EventRead<String> event = reader1.readNextEvent(10000);
        //reader creates a new connection to the segment 0;
        assertEquals("zero", event.getEvent());
        //Connection to segment 0 does not cause an increase in number of open connections since we have reached the maxConnection count.
        assertChannelCount(5, connectionPool, connectionFactory);

        // scale
        Stream stream = new StreamImpl(SCOPE, STREAM_NAME);
        Map<Double, Double> map = new HashMap<>();
        map.put(0.0, 0.33);
        map.put(0.33, 0.66);
        map.put(0.66, 1.0);
        Boolean result = controller.scaleStream(stream, Collections.singletonList(0L), map, executor).getFuture().get();
        assertTrue(result);
        //No changes to the channel count.
        assertChannelCount(5, connectionPool, connectionFactory);
        
        //Reaches EOS
        event = reader1.readNextEvent(1000);
        assertNull(event.getEvent());

        //Write more events.
        writer.writeEvent("1", "one").get();
        writer.writeEvent("2", "two").get();
        writer.writeEvent("3", "three").get();
        writer.writeEvent("4", "four").get();
        writer.writeEvent("5", "five").get();
        writer.writeEvent("6", "six").get();

        //2 new flows  are opened.(+3 connections to the segments 1,2,3 after scale by the writer,
        // -1 flow to segment 0 which is sealed.)
        assertChannelCount(5, connectionPool, connectionFactory);

        ReaderGroup readerGroup = groupManager.getReaderGroup(READER_GROUP);
        CompletableFuture<Checkpoint> future = readerGroup.initiateCheckpoint("cp1", executor);
        //4 more from the state synchronizer
        assertChannelCount(5, connectionPool, connectionFactory);
        event = reader1.readNextEvent(5000);
        assertEquals("cp1", event.getCheckpointName());
        event = reader1.readNextEvent(5000);
        assertNotNull(event.getEvent());
        future.join();
        //Checkpoint should close connections back down
        readerGroup.close();
        assertChannelCount(5, connectionPool, connectionFactory);
        
        event = reader1.readNextEvent(10000);
        assertNotNull(event.getEvent());
        assertChannelCount(5, connectionPool, connectionFactory);
    }
    
    @Test(timeout = 30000)
    public void testDetectChannelLeakSegmentSealed() throws Exception {
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.fixed(1))
                                                        .build();
        Controller controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope(SCOPE, 0L).get();
        controller.createStream(SCOPE, STREAM_NAME, config).get();
        //Set the max number connections to verify channel creation behaviour
        final ClientConfig clientConfig = ClientConfig.builder().maxConnectionsPerSegmentStore(500).build();

        @Cleanup
        SocketConnectionFactoryImpl connectionFactory = new SocketConnectionFactoryImpl(clientConfig, executor);
        @Cleanup
        ConnectionPoolImpl connectionPool = new ConnectionPoolImpl(clientConfig, connectionFactory);
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE, controller, connectionPool);

        int channelCount = 0;
        assertChannelCount(channelCount, connectionPool, connectionFactory);
        
        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl(SCOPE, controller, clientFactory);
        groupManager.createReaderGroup(READER_GROUP, ReaderGroupConfig.builder().disableAutomaticCheckpoints().groupRefreshTimeMillis(0)
                                       .stream(Stream.of(SCOPE, STREAM_NAME)).build());
        
        //Should not add any connections
        assertChannelCount(channelCount, connectionPool, connectionFactory);
        
        //Create a writer.
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(SCOPE, serializer, writerConfig);

        //Write an event.
        writer.writeEvent("0", "zero").get();
        channelCount += 1;
        assertChannelCount(channelCount, connectionPool, connectionFactory);

        @Cleanup
        EventStreamReader<String> reader1 = clientFactory.createReader("readerId1", READER_GROUP, serializer,
                ReaderConfig.builder().disableTimeWindows(true).build());
        
        channelCount += 4; //One for segment 3 for state synchronizer
        assertChannelCount(channelCount, connectionPool, connectionFactory);
        
        //Read an event.
        EventRead<String> event = reader1.readNextEvent(10000);
        assertEquals("zero", event.getEvent());

        channelCount += 1;
        assertChannelCount(channelCount, connectionPool, connectionFactory);
        
        // scale
        Stream stream = new StreamImpl(SCOPE, SCOPE);
        Map<Double, Double> map = new HashMap<>();
        map.put(0.0, 0.33);
        map.put(0.33, 0.66);
        map.put(0.66, 1.0);
        Boolean result = controller.scaleStream(stream, Collections.singletonList(0L), map, executor).getFuture().get();
        assertTrue(result);

        event = reader1.readNextEvent(0);
        assertNull(event.getEvent());
        channelCount -= 1; //Reader should see EOS
        assertChannelCount(channelCount, connectionPool, connectionFactory);
        
        writer.writeEvent("1", "one").get(); //should detect end of segment
        channelCount += 2; //Close one segment open 3.
        assertChannelCount(channelCount, connectionPool, connectionFactory);
        
        ReaderGroup readerGroup = groupManager.getReaderGroup(READER_GROUP);
        readerGroup.getMetrics().unreadBytes();
        CompletableFuture<Checkpoint> future = readerGroup.initiateCheckpoint("cp1", executor);
        //3 more from the state synchronizer
        channelCount += 4;
        assertChannelCount(channelCount, connectionPool, connectionFactory);
        event = reader1.readNextEvent(5000);
        assertEquals("cp1", event.getCheckpointName());
        
        event = reader1.readNextEvent(10000);
        assertEquals("one", event.getEvent());
        channelCount += 3; //From new segments on reader
        assertChannelCount(channelCount, connectionPool, connectionFactory);
        
        future.join();
        //Checkpoint should close connections back down
        readerGroup.close();
        channelCount -= 4;
        assertChannelCount(channelCount, connectionPool, connectionFactory);

        //Write more events.
        writer.writeEvent("2", "two").get();
        writer.writeEvent("3", "three").get();
        writer.writeEvent("4", "four").get();
   
        //no changes to socket count.
        assertChannelCount(channelCount, connectionPool, connectionFactory);

        event = reader1.readNextEvent(10000);
        assertNotNull(event.getEvent());
        //no changes to socket count.
        assertChannelCount(channelCount, connectionPool, connectionFactory);
        
        reader1.close();
        //3 from segments 4 from group state.
        channelCount -= 7;
        assertChannelCount(channelCount, connectionPool, connectionFactory);
        groupManager.close();
        writer.close();
        assertChannelCount(0, connectionPool, connectionFactory);

    }

    @Test(timeout = 30000)
    public void testDetectChannelLeakMultiReader() throws Exception {
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 1))
                                                        .build();
        //Set the max number connections to verify channel creation behaviour
        final ClientConfig clientConfig = ClientConfig.builder().maxConnectionsPerSegmentStore(500).build();
        Controller controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope(SCOPE, 0L).get();
        controller.createStream(SCOPE, STREAM_NAME, config).get();
        @Cleanup
        SocketConnectionFactoryImpl connectionFactory = new SocketConnectionFactoryImpl(clientConfig, new InlineExecutor());
        @Cleanup
        ConnectionPoolImpl connectionPool = new ConnectionPoolImpl(clientConfig, connectionFactory);
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE, controller, connectionPool);
        int expectedChannelCount = 0; // open socket count.
        assertChannelCount(expectedChannelCount, connectionPool, connectionFactory);
        
        //Create a writer and write an event.
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(STREAM_NAME, serializer, writerConfig);
        writer.writeEvent("0", "zero").get();

        expectedChannelCount += 1; // connection to segment 0.
        assertChannelCount(expectedChannelCount, connectionPool, connectionFactory);
        
        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl(SCOPE, controller, clientFactory);
        assertChannelCount(expectedChannelCount, connectionPool, connectionFactory); // no changes expected.
      
        groupManager.createReaderGroup(READER_GROUP, ReaderGroupConfig.builder().disableAutomaticCheckpoints().groupRefreshTimeMillis(0)
                                       .stream(Stream.of(SCOPE, STREAM_NAME)).build());

        //create a reader and read an event.
        @Cleanup
        EventStreamReader<String> reader1 = clientFactory.createReader("readerId1", READER_GROUP, serializer,
                ReaderConfig.builder().disableTimeWindows(true).build());
        //Creating a reader spawns a revisioned stream client which opens 4 sockets ( read, write, metadataClient and conditionalUpdates).
        expectedChannelCount += 4;
        EventRead<String> event = reader1.readNextEvent(10000);
        
        //reader creates a new connection to the segment 0;
        expectedChannelCount += 1;
        assertEquals("zero", event.getEvent());
        assertChannelCount(expectedChannelCount, connectionPool, connectionFactory);

        // scale
        Stream stream = new StreamImpl(SCOPE, STREAM_NAME);
        Map<Double, Double> map = new HashMap<>();
        map.put(0.0, 0.33);
        map.put(0.33, 0.66);
        map.put(0.66, 1.0);
        Boolean result = controller.scaleStream(stream, Collections.singletonList(0L), map, executor).getFuture().get();
        assertTrue(result);
        //No changes to the channel count.
        assertChannelCount(expectedChannelCount, connectionPool, connectionFactory);

        event = reader1.readNextEvent(0);
        assertNull(event.getEvent());
        event = reader1.readNextEvent(0);
        assertNull(event.getEvent());
        //should decrease channel count from close connection
        expectedChannelCount -= 1;
        assertChannelCount(expectedChannelCount, connectionPool, connectionFactory);
        
        //Write more events.
        writer.writeEvent("1", "one").get();
        writer.writeEvent("2", "two").get();
        writer.writeEvent("3", "three").get();
        writer.writeEvent("4", "four").get();
        writer.writeEvent("5", "five").get();
        writer.writeEvent("6", "six").get();
        
        //Open 3 new segments close one old one. 
        expectedChannelCount += 2;
        assertChannelCount(expectedChannelCount, connectionPool, connectionFactory);
        
        ReaderGroup readerGroup = groupManager.getReaderGroup(READER_GROUP);
        CompletableFuture<Checkpoint> future = readerGroup.initiateCheckpoint("cp1", executor);
        //4 more from the state synchronizer
        expectedChannelCount += 4;
        assertChannelCount(expectedChannelCount, connectionPool, connectionFactory);
        event = reader1.readNextEvent(5000);
        assertEquals("cp1", event.getCheckpointName());
        
        //Add a new reader
        @Cleanup
        EventStreamReader<String> reader2 = clientFactory.createReader("readerId2", READER_GROUP, serializer,
                ReaderConfig.builder().disableTimeWindows(true).build());
        //Creating a reader spawns a revisioned stream client which opens 4 sockets ( read, write, metadataClient and conditionalUpdates).
        expectedChannelCount += 4;
        assertChannelCount(expectedChannelCount, connectionPool, connectionFactory);

        event = reader1.readNextEvent(5000);
        assertNotNull(event.getEvent());
        event = reader2.readNextEvent(5000);
        assertNotNull(event.getEvent());
        //3 more from the new segments
        expectedChannelCount += 3;
        assertChannelCount(expectedChannelCount, connectionPool, connectionFactory);
        
        future.join();
        //Checkpoint should close connections back down
        readerGroup.close();
        expectedChannelCount -= 4;
        assertChannelCount(expectedChannelCount, connectionPool, connectionFactory);
        
        reader1.close();
        reader2.close();
        expectedChannelCount -= 8 + 3;
        assertChannelCount(expectedChannelCount, connectionPool, connectionFactory);
    }
    
    private void assertChannelCount(int expectedChannelCount, ConnectionPoolImpl connectionPool,
            SocketConnectionFactoryImpl factory) throws Exception {
        assertEventuallyEquals(expectedChannelCount, () -> {
            connectionPool.pruneUnusedConnections();
            return factory.getOpenSocketCount();
        }, ASSERT_TIMEOUT);
    }

}

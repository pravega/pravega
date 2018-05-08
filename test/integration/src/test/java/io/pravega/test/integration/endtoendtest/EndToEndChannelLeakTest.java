/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.endtoendtest;

import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Slf4j
public class EndToEndChannelLeakTest {

    private static final String SCOPE = "test";
    private static final String STREAM_NAME = "test";
    private static final String READER_GROUP = "reader";

    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final String serviceHost = "localhost";
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int containerCount = 4;
    private final JavaSerializer<String> serializer = new JavaSerializer<>();
    private TestingServer zkTestServer;
    private PravegaConnectionListener server;
    private ControllerWrapper controllerWrapper;
    private ServiceBuilder serviceBuilder;
    private ScheduledExecutorService executor;

    @Before
    public void setUp() throws Exception {
        executor = Executors.newSingleThreadScheduledExecutor();
        zkTestServer = new TestingServerStarter().start();

        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();

        server = new PravegaConnectionListener(false, servicePort, store);
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
    public void testDetectChannelLeakSegmentSealed() throws Exception {
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scope(SCOPE)
                                                        .streamName(STREAM_NAME)
                                                        .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 1))
                                                        .build();
        Controller controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope(SCOPE).get();
        controller.createStream(config).get();
        @Cleanup
        ConnectionFactoryImpl connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());
        @Cleanup
        ClientFactory clientFactory = new ClientFactoryImpl(SCOPE, controller, connectionFactory);

        //Create a writer.
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(SCOPE, new JavaSerializer<>(),
                EventWriterConfig.builder().build());

        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl(SCOPE, controller, clientFactory,
                connectionFactory);
        groupManager.createReaderGroup(READER_GROUP, ReaderGroupConfig.builder().disableAutomaticCheckpoints().
                stream(Stream.of(SCOPE, STREAM_NAME)).build());

        @Cleanup
        EventStreamReader<String> reader1 = clientFactory.createReader("readerId1", READER_GROUP, new JavaSerializer<>(),
                ReaderConfig.builder().build());
        //Write an event.
        writer.writeEvent("0", "zero").get();

        //Read an event.
        EventRead<String> event = reader1.readNextEvent(10000);
        assertNotNull(event);
        assertEquals("zero", event.getEvent());

        // scale
        Stream stream = new StreamImpl(SCOPE, SCOPE);
        Map<Double, Double> map = new HashMap<>();
        map.put(0.0, 0.33);
        map.put(0.33, 0.66);
        map.put(0.66, 1.0);
        Boolean result = controller.scaleStream(stream, Collections.singletonList(0), map, executor).getFuture().get();
        assertTrue(result);

        //Write more events.
        writer.writeEvent("0", "one").get();
        writer.writeEvent("0", "two").get();
        writer.writeEvent("1", "three").get();

        int channelCount = connectionFactory.getActiveChannelCount(); //store the open channel count before reading.

        event = reader1.readNextEvent(10000);
        assertNotNull(event.getEvent());
        //Number of sockets will increase by 2 ( +3 for the new segments -1 since the older segment is sealed).
        assertEquals(channelCount + 2, connectionFactory.getActiveChannelCount());

        event = reader1.readNextEvent(10000);
        assertNotNull(event.getEvent());
        //no changes to socket count.
        assertEquals(channelCount + 2, connectionFactory.getActiveChannelCount());

        event = reader1.readNextEvent(10000);
        assertNotNull(event.getEvent());
        //no changes to socket count.
        assertEquals(channelCount + 2, connectionFactory.getActiveChannelCount());
    }

    @Test(timeout = 30000)
    public void testDetectChannelLeakMultiReader() throws Exception {
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scope(SCOPE)
                                                        .streamName(STREAM_NAME)
                                                        .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 1))
                                                        .build();
        Controller controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope(SCOPE).get();
        controller.createStream(config).get();
        @Cleanup
        ConnectionFactoryImpl connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE, controller, connectionFactory);
        int expectedChannelCount = 0; // open socket count.
        assertEquals(expectedChannelCount, connectionFactory.getActiveChannelCount());
        
        //Create a writer and write an event.
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(STREAM_NAME, new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        writer.writeEvent("0", "zero").get();

        expectedChannelCount += 1; // connection to segment 0.
        assertEquals(expectedChannelCount, connectionFactory.getActiveChannelCount());
        
        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl(SCOPE, controller, clientFactory,
                connectionFactory);
        assertEquals(expectedChannelCount, connectionFactory.getActiveChannelCount()); // no changes expected.
      
        groupManager.createReaderGroup(READER_GROUP, ReaderGroupConfig.builder().disableAutomaticCheckpoints()
                .stream(Stream.of(SCOPE, STREAM_NAME)).build());

        //create a reader and read an event.
        @Cleanup
        EventStreamReader<String> reader1 = clientFactory.createReader("readerId1", READER_GROUP, serializer,
                ReaderConfig.builder().build());
        //Creating a reader spawns a revisioned stream client which opens 4 sockets ( read, write, metadataClient and conditionalUpdates).
        expectedChannelCount += 4;
        EventRead<String> event = reader1.readNextEvent(10000);
        //reader creates a new connection to the segment 0;
        expectedChannelCount += 1;

        assertNotNull(event);
        assertEquals("zero", event.getEvent());
        //+1 socket to segment 0 of the stream.
        assertEquals(expectedChannelCount, connectionFactory.getActiveChannelCount());

        // scale
        Stream stream = new StreamImpl(SCOPE, STREAM_NAME);
        Map<Double, Double> map = new HashMap<>();
        map.put(0.0, 0.33);
        map.put(0.33, 0.66);
        map.put(0.66, 1.0);
        Boolean result = controller.scaleStream(stream, Collections.singletonList(0), map, executor).getFuture().get();
        assertTrue(result);
        //No changes to the channel count.
        assertEquals(expectedChannelCount, connectionFactory.getActiveChannelCount());

        //Write more events.
        writer.writeEvent("0", "one").get();
        writer.writeEvent("0", "two").get();
        writer.writeEvent("1", "three").get();

        //2 new connections are opened.(+3 connections to the segments 1,2,3 after scale by the writer,
        // -1 connection to segment 0 which is sealed.)
        expectedChannelCount += 2;
        assertEquals(expectedChannelCount, connectionFactory.getActiveChannelCount());

        //Add a new reader
        @Cleanup
        EventStreamReader<String> reader2 = clientFactory.createReader("readerId2", READER_GROUP, serializer,
                ReaderConfig.builder().build());
        //Creating a reader spawns a revisioned stream client which opens 4 sockets ( read, write, metadataClient and conditionalUpdates).
        expectedChannelCount += 4;

        event = reader1.readNextEvent(10000);
        assertNotNull(event);

        //+1 connection (-1 since segment 0 of stream is sealed + 2 connections to two segments of stream (there are
        // 2 readers and 3 segments and the reader1 will be assigned 2 segments))
        expectedChannelCount += 1;
        assertEquals(expectedChannelCount, connectionFactory.getActiveChannelCount());

        event = reader2.readNextEvent(10000);
        assertNotNull(event);

        //+1 connection (a new connection to the remaining stream segment)
        expectedChannelCount += 1;
        assertEquals(expectedChannelCount, connectionFactory.getActiveChannelCount());
    }
}

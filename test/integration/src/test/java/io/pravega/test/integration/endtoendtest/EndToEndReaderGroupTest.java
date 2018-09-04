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
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
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
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.controller.server.eventProcessor.LocalController;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Slf4j
public class EndToEndReaderGroupTest {

    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final String serviceHost = "localhost";
    private final URI controllerURI = URI.create("tcp://" + serviceHost + ":" + controllerPort);
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int containerCount = 4;
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

    @Test(timeout = 30000)
    public void testReaderOffline() throws Exception {
        StreamConfiguration config = getStreamConfig("test", "test");
        LocalController controller = (LocalController) controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope("test").get();
        controller.createStream(config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder()
                                                                                    .controllerURI(URI.create("tcp://" + serviceHost))
                                                                                    .build());
        @Cleanup
        ClientFactory clientFactory = new ClientFactoryImpl("test", controller, connectionFactory);

        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl("test", controller, clientFactory,
                connectionFactory);
        groupManager.createReaderGroup("group", ReaderGroupConfig.builder().disableAutomaticCheckpoints()
                                                                  .stream("test/test").build());

        final ReaderGroup readerGroup = groupManager.getReaderGroup("group");

        // create a reader
        @Cleanup
        EventStreamReader<String> reader1 = clientFactory.createReader("reader1", "group", new JavaSerializer<>(),
                ReaderConfig.builder().build());

        EventRead<String> eventRead = reader1.readNextEvent(100);
        assertNull("Event read should be null since no events are written", eventRead.getEvent());

        @Cleanup
        EventStreamReader<String> reader2 = clientFactory.createReader("reader2", "group", new JavaSerializer<>(),
                ReaderConfig.builder().build());

        //make reader1 offline
        readerGroup.readerOffline("reader1", null);

        // write events into the stream.
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter("test", new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        writer.writeEvent("0", "data1").get();
        writer.writeEvent("0", "data2").get();

        eventRead = reader2.readNextEvent(10000);
        assertEquals("data1", eventRead.getEvent());
    }

    @Test(timeout = 30000)
    public void testMultiScopeReaderGroup() throws Exception {
        LocalController controller = (LocalController) controllerWrapper.getController();

        // Config of two streams with same name and different scopes.
        String defaultScope = "test";
        String scopeA = "scopeA";
        String scopeB = "scopeB";
        String streamName = "test";

        // Create Scopes
        controllerWrapper.getControllerService().createScope(defaultScope).get();
        controllerWrapper.getControllerService().createScope(scopeA).get();
        controllerWrapper.getControllerService().createScope(scopeB).get();

        // Create Streams.
        controller.createStream(getStreamConfig(scopeA, streamName)).get();
        controller.createStream(getStreamConfig(scopeB, streamName)).get();

        // Create ReaderGroup and reader.
        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder()
                                                                                    .controllerURI(URI.create("tcp://" + serviceHost))
                                                                                    .build());
        @Cleanup
        ClientFactory clientFactory = new ClientFactoryImpl(streamName, controller, connectionFactory);

        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl(streamName, controller, clientFactory,
                                                                     connectionFactory);
        groupManager.createReaderGroup("group", ReaderGroupConfig.builder()
                                                                 .disableAutomaticCheckpoints()
                                                                 .stream(Stream.of(scopeA, streamName))
                                                                 .stream(Stream.of(scopeB, streamName))
                                                                 .build());

        ReaderGroup readerGroup = groupManager.getReaderGroup("group");
        @Cleanup
        EventStreamReader<String> reader1 = clientFactory.createReader("reader1", "group", new JavaSerializer<>(),
                                                                       ReaderConfig.builder().build());

        // Read empty stream.
        EventRead<String> eventRead = reader1.readNextEvent(100);
        assertNull("Event read should be null since no events are written", eventRead.getEvent());

        // Write to scopeA stream.
        writeTestEvent(scopeA, streamName, 0);
        eventRead = reader1.readNextEvent(10000);
        assertEquals("0", eventRead.getEvent());

        // Write to scopeB stream.
        writeTestEvent(scopeB, streamName, 1);
        eventRead = reader1.readNextEvent(10000);
        assertEquals("1", eventRead.getEvent());

        // Verify ReaderGroup.getStreamNames().
        Set<String> managedStreams = readerGroup.getStreamNames();
        assertTrue(managedStreams.contains(Stream.of(scopeA, streamName).getScopedName()));
        assertTrue(managedStreams.contains(Stream.of(scopeB, streamName).getScopedName()));
    }

    private StreamConfiguration getStreamConfig(String scope, String streamName) {
        return StreamConfiguration.builder()
                                  .scope(scope)
                                  .streamName(streamName)
                                  .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 2))
                                  .build();
    }

    private void writeTestEvent(String scope, String streamName, int eventId) {
        @Cleanup
        ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new JavaSerializer<>(), EventWriterConfig.builder().build());

        writer.writeEvent( "0", Integer.toString(eventId)).join();
    }
}

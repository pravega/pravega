/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import io.pravega.client.stream.Checkpoint;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.test.common.InlineExecutor;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UnreadBytesTest {

    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final URI controllerUri = URI.create("tcp://localhost:" + String.valueOf(controllerPort));
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
        executor.shutdown();
        controllerWrapper.close();
        server.close();
        serviceBuilder.close();
        zkTestServer.close();
    }

    @Test(timeout = 40000)
    public void testUnreadBytes() throws Exception {
        StreamConfiguration config = StreamConfiguration.builder()
                .scope("unreadbytes")
                .streamName("unreadbytes")
                .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 1))
                .build();
        Controller controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope("unreadbytes").get();
        controller.createStream(config).get();
        @Cleanup
        ClientFactory clientFactory = ClientFactory.withScope("unreadbytes", controllerUri);
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter("unreadbytes", new JavaSerializer<>(),
                EventWriterConfig.builder().build());

        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope("unreadbytes", controllerUri);
        ReaderGroup readerGroup = groupManager.createReaderGroup("group", ReaderGroupConfig
                .builder().disableAutomaticCheckpoints().build(), Collections
                .singleton("unreadbytes"));
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader("readerId", "group", new JavaSerializer<>(),
                ReaderConfig.builder().build());
        long unreadBytes = readerGroup.getMetrics().unreadBytes();
        assertTrue("Unread bvtes: " + unreadBytes, unreadBytes == 0);

        writer.writeEvent("0", "fpj was here").get();
        writer.writeEvent("0", "fpj was here").get();

        EventRead<String> firstEvent = reader.readNextEvent(15000);
        EventRead<String> secondEvent = reader.readNextEvent(15000);
        assertNotNull(firstEvent);
        assertEquals("fpj was here", firstEvent.getEvent());
        assertNotNull(secondEvent);
        assertEquals("fpj was here", secondEvent.getEvent());

        // TODO: This is not actually working. We have read everything, but the
        // assertion is failing essentially because the number of read bytes is
        // internally computed is zero.
        unreadBytes = readerGroup.getMetrics().unreadBytes();
        assertTrue("Unread bvtes: " + unreadBytes, unreadBytes == 0);

        // TODO: This is also not working because of the way we are computing
        // bytes read. It sounds like the count does not change unless there
        // changes to the set of assigned and unassigned segments in the state
        // of the synchronizer.
        writer.writeEvent("0", "fpj was here").get();
        unreadBytes = readerGroup.getMetrics().unreadBytes();
        assertTrue("Unread bytes: " + unreadBytes, unreadBytes == 27);
    }
}
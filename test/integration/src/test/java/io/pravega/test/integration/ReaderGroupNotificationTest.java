/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.stream.notifications.Listener;
import io.pravega.client.stream.notifications.events.ScaleEvent;
import io.pravega.common.util.ReusableLatch;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.InlineExecutor;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ReaderGroupNotificationTest {

    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final String serviceHost = "localhost";
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int containerCount = 4;
    private TestingServer zkTestServer;
    private PravegaConnectionListener server;
    private ControllerWrapper controllerWrapper;
    private ServiceBuilder serviceBuilder;
    private ScheduledExecutorService executor;
    private AtomicBoolean listenerInvoked = new AtomicBoolean();
    private AtomicInteger numberOfReaders = new AtomicInteger(0);
    private AtomicInteger numberOfSegments = new AtomicInteger(0);
    private ReusableLatch listenerLatch = new ReusableLatch();

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

    @Test
    public void testScaleNotifications() throws Exception {
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scope("test")
                                                        .streamName("test")
                                                        .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 2))
                                                        .build();
        Controller controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope("test").get();
        controller.createStream(config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(false);
        @Cleanup
        ClientFactory clientFactory = new ClientFactoryImpl("test", controller, connectionFactory);
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter("test", new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        writer.writeEvent("0", "data1").get();

        // scale
        Stream stream = new StreamImpl("test", "test");
        Map<Double, Double> map = new HashMap<>();
        map.put(0.0, 0.25);
        map.put(0.25, 0.5);
        Boolean result = controller.scaleStream(stream, Collections.singletonList(0), map, executor).getFuture().get();
        assertTrue(result);

        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl("test", controller, clientFactory,
                connectionFactory);
        ReaderGroup readerGroup = groupManager.createReaderGroup("reader", ReaderGroupConfig
                .builder().build(), Collections
                .singleton("test"));
        @Cleanup
        EventStreamReader<String> reader1 = clientFactory.createReader("readerId", "reader", new JavaSerializer<>(),
                ReaderConfig.builder().build());

        //Add scale event listener
        Listener<ScaleEvent> l1 = event -> {
            listenerInvoked.set(true);
            numberOfReaders.set(event.getNumOfReaders());
            numberOfSegments.set(event.getNumOfSegments());
            listenerLatch.release();
        };
        ScheduledExecutorService executor = new InlineExecutor();
        readerGroup.getScaleEventNotifier().addListener(l1, executor);

        EventRead<String> event1 = reader1.readNextEvent(10000);

        listenerLatch.await();
        assertNotNull(event1);
        assertEquals("data1", event1.getEvent());
        assertTrue("Listener invoked", listenerInvoked.get());
        assertEquals(2, numberOfSegments.get());
        assertEquals(1, numberOfReaders.get());

        //reset values
        listenerInvoked.set(false);
        numberOfSegments.set(0);
        numberOfReaders.set(0);
        listenerLatch.reset();
        readerGroup.getScaleEventNotifier().removeListeners();

        @Cleanup
        EventStreamReader<String> reader2 = clientFactory.createReader("readerId2", "reader", new JavaSerializer<>(),
                ReaderConfig.builder().build());
        reader1.readNextEvent(1000);


        readerGroup.getScaleEventNotifier().addListener(l1, executor);
        listenerLatch.await();
        assertTrue("Listener invoked", listenerInvoked.get());
        assertEquals(3, numberOfSegments.get());
        assertEquals(2, numberOfReaders.get());
    }
}

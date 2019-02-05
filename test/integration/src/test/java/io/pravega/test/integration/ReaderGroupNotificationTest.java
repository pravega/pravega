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

import io.pravega.client.ClientConfig;
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
import io.pravega.client.stream.notifications.EndOfDataNotification;
import io.pravega.client.stream.notifications.Listener;
import io.pravega.client.stream.notifications.SegmentNotification;
import io.pravega.common.util.ReusableLatch;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@Slf4j
public class ReaderGroupNotificationTest {

    private static final String SCOPE = "test";
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
    private ReusableLatch listenerLatch = new ReusableLatch();

    @BeforeClass
    public static void beforeClass() {
        System.setProperty("pravega.client.segmentNotification.poll.interval.seconds", String.valueOf(5));
        System.setProperty("pravega.client.endOfDataNotification.poll.interval.seconds", String.valueOf(5));
    }

    @Before
    public void setUp() throws Exception {
        executor = Executors.newSingleThreadScheduledExecutor();
        zkTestServer = new TestingServerStarter().start();

        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();

        server = new PravegaConnectionListener(false, servicePort, store, mock(TableStore.class));
        server.startListening();

        controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(),
                false,
                controllerPort,
                serviceHost,
                servicePort,
                containerCount);
        controllerWrapper.awaitRunning();
        listenerLatch.reset();
        listenerInvoked.set(false);
    }

    @After
    public void tearDown() throws Exception {
        executor.shutdownNow();
        controllerWrapper.close();
        server.close();
        serviceBuilder.close();
        zkTestServer.close();
    }

    @Test(timeout = 40000)
    public void testSegmentNotifications() throws Exception {
        final String streamName = "stream1";
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 1))
                                                        .build();
        Controller controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope(SCOPE).get();
        controller.createStream(SCOPE, streamName, config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder()
                                                                                    .controllerURI(URI.create("tcp://localhost"))
                                                                                    .build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE, controller, connectionFactory);
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        writer.writeEvent("0", "data1").get();

        // scale
        Stream stream = new StreamImpl(SCOPE, streamName);
        Map<Double, Double> map = new HashMap<>();
        map.put(0.0, 0.5);
        map.put(0.5, 1.0);
        Boolean result = controller.scaleStream(stream, Collections.singletonList(0L), map, executor).getFuture().get();
        assertTrue(result);
        writer.writeEvent("0", "data2").get();

        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl(SCOPE, controller, clientFactory,
                connectionFactory);
        groupManager.createReaderGroup("reader", ReaderGroupConfig
                .builder().disableAutomaticCheckpoints().stream(Stream.of(SCOPE, streamName)).build());
        @Cleanup
        ReaderGroup readerGroup = groupManager.getReaderGroup("reader");
        @Cleanup
        EventStreamReader<String> reader1 = clientFactory.createReader("readerId", "reader", new JavaSerializer<>(),
                ReaderConfig.builder().build());

        final CountDownLatch latch = new CountDownLatch(2);
        final ArrayDeque<SegmentNotification> notificationResults = new ArrayDeque<>();

        //Add segment event listener
        Listener<SegmentNotification> l1 = notification -> {
            log.info("Number of Segments{}, Number of Readers: {}", notification.getNumOfSegments(), notification.getNumOfReaders());
            notificationResults.offer(notification);
            latch.countDown();
        };
        readerGroup.getSegmentNotifier(executor).registerListener(l1);

        EventRead<String> event1 = reader1.readNextEvent(15000);
        EventRead<String> event2 = reader1.readNextEvent(15000);
        assertNotNull(event1);
        assertEquals("data1", event1.getEvent());
        assertNotNull(event2);
        assertEquals("data2", event2.getEvent());

        latch.await(); // await two invocations.

        SegmentNotification initialSegmentNotification = notificationResults.poll();
        assertNotNull(initialSegmentNotification);
        assertEquals(1, initialSegmentNotification.getNumOfReaders());
        assertEquals(1, initialSegmentNotification.getNumOfSegments());

        SegmentNotification segmentNotificationPostScale = notificationResults.poll();
        assertEquals(1, segmentNotificationPostScale.getNumOfReaders());
        assertEquals(2, segmentNotificationPostScale.getNumOfSegments());
    }

    @Test(timeout = 40000)
    public void testEndOfStreamNotifications() throws Exception {
        final String streamName = "stream2";
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 1))
                                                        .build();
        Controller controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope(SCOPE).get();
        controller.createStream(SCOPE, streamName, config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder()
                                                                                    .controllerURI(URI.create("tcp://localhost"))
                                                                                    .build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE, controller, connectionFactory);
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        writer.writeEvent("0", "data1").get();

        // scale
        Stream stream = new StreamImpl(SCOPE, streamName);
        Map<Double, Double> map = new HashMap<>();
        map.put(0.0, 0.5);
        map.put(0.5, 1.0);
        Boolean result = controller.scaleStream(stream, Collections.singletonList(0L), map, executor).getFuture().get();
        assertTrue(result);
        writer.writeEvent("0", "data2").get();
        assertTrue(controller.sealStream(SCOPE, streamName).get()); // seal stream

        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl(SCOPE, controller, clientFactory,
                connectionFactory);
        groupManager.createReaderGroup("reader", ReaderGroupConfig
                .builder().disableAutomaticCheckpoints().stream(Stream.of(SCOPE, streamName)).build());
        @Cleanup
        ReaderGroup readerGroup = groupManager.getReaderGroup("reader");
        @Cleanup
        EventStreamReader<String> reader1 = clientFactory.createReader("readerId", "reader", new JavaSerializer<>(),
                ReaderConfig.builder().build());

        //Add segment event listener
        Listener<EndOfDataNotification> l1 = notification -> {
            listenerInvoked.set(true);
            listenerLatch.release();
        };
        readerGroup.getEndOfDataNotifier(executor).registerListener(l1);

        EventRead<String> event1 = reader1.readNextEvent(10000);
        EventRead<String> event2 = reader1.readNextEvent(10000);
        EventRead<String> event3 = reader1.readNextEvent(10000);
        assertNotNull(event1);
        assertEquals("data1", event1.getEvent());
        assertNotNull(event2);
        assertEquals("data2", event2.getEvent());
        assertNull(event3.getEvent());

        listenerLatch.await();
        assertTrue("Listener invoked", listenerInvoked.get());
    }

}

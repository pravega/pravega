/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
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
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.segment.impl.Segment;
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
import io.pravega.client.stream.impl.StreamSegments;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.stream.notifications.EndOfDataNotification;
import io.pravega.client.stream.notifications.Listener;
import io.pravega.client.stream.notifications.SegmentNotification;
import io.pravega.client.stream.notifications.notifier.EndOfDataNotifier;
import io.pravega.client.stream.notifications.notifier.SegmentNotifier;
import io.pravega.common.util.ReusableLatch;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.LeakDetectorTestSuite;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import lombok.Cleanup;
import lombok.val;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Slf4j
public class ReaderGroupNotificationTest extends LeakDetectorTestSuite {

    private static final String SCOPE = "test";
    private static final int NUM_SEGMENTS = 10;
    private static final int NUM_EVENTS = 10;
    private static final int NUM_THREADS = 3;
    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final String serviceHost = "localhost";
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int containerCount = 4;
    private TestingServer zkTestServer;
    private PravegaConnectionListener server;
    private ControllerWrapper controllerWrapper;
    private ServiceBuilder serviceBuilder;
    private AtomicBoolean listenerInvoked = new AtomicBoolean();
    private ReusableLatch listenerLatch = new ReusableLatch();

    @Before
    public void setUp() throws Exception {
        zkTestServer = new TestingServerStarter().start();

        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();

        server = new PravegaConnectionListener(false, servicePort, store, tableStore, this.serviceBuilder.getLowPriorityExecutor());
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
        controllerWrapper.close();
        server.close();
        serviceBuilder.close();
        zkTestServer.close();
    }

    @Override
    public int getThreadPoolSize() {
           return NUM_THREADS;
    }
    
    @Test(timeout = 40000)
    public void testSegmentNotifications() throws Exception {
        final String streamName = "stream1";
        StreamConfiguration config = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        Controller controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope(SCOPE).get();
        controller.createStream(SCOPE, streamName, config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder()
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
        Boolean result = controller.scaleStream(stream, Collections.singletonList(0L), map, executorService()).getFuture().get();
        assertTrue(result);
        writer.writeEvent("0", "data2").get();

        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl(SCOPE, controller, clientFactory);
        groupManager.createReaderGroup("reader", ReaderGroupConfig
                .builder().disableAutomaticCheckpoints().stream(Stream.of(SCOPE, streamName)).groupRefreshTimeMillis(0).build());
        @Cleanup
        ReaderGroup readerGroup = groupManager.getReaderGroup("reader");
        @Cleanup
        EventStreamReader<String> reader1 = clientFactory.createReader("readerId", "reader", new JavaSerializer<>(),
                ReaderConfig.builder().initialAllocationDelay(0).build());

        val notificationResults = new ArrayBlockingQueue<SegmentNotification>(2);

        //Add segment event listener
        Listener<SegmentNotification> l1 = notification -> {
            log.info("Number of Segments: {}, Number of Readers: {}", notification.getNumOfSegments(), notification.getNumOfReaders());
            notificationResults.add(notification);
        };
        SegmentNotifier segmentNotifier = (SegmentNotifier) readerGroup.getSegmentNotifier(executorService());
        segmentNotifier.registerListener(l1);

        // Read first event and validate notification.
        EventRead<String> event1 = reader1.readNextEvent(5000);
        assertEquals("data1", event1.getEvent());

        segmentNotifier.pollNow();
        SegmentNotification initialSegmentNotification = notificationResults.take();
        assertNotNull(initialSegmentNotification);
        assertEquals(1, initialSegmentNotification.getNumOfReaders());
        assertEquals(1, initialSegmentNotification.getNumOfSegments());

        EventRead<String> emptyEvent = reader1.readNextEvent(0);
        assertNull(emptyEvent.getEvent());
        assertFalse(emptyEvent.isCheckpoint());
        readerGroup.initiateCheckpoint("cp", executorService());
        EventRead<String> cpEvent = reader1.readNextEvent(1000);
        assertTrue(cpEvent.isCheckpoint());

        // Read second event and validate notification.
        EventRead<String> event2 = reader1.readNextEvent(10000);
        assertEquals("data2", event2.getEvent());
        segmentNotifier.pollNow();
        SegmentNotification segmentNotificationPostScale = notificationResults.take();
        assertEquals(1, segmentNotificationPostScale.getNumOfReaders());
        assertEquals(2, segmentNotificationPostScale.getNumOfSegments());
    }

    @Test(timeout = 40000)
    public void testTargetRateNotifications() throws Exception {
        final String streamName = "stream1";
        Random random = new Random(0);
        StreamConfiguration config = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.byEventRate(1, 2, 3))
                .build();
        Controller controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope(SCOPE).get();
        controller.createStream(SCOPE, streamName, config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder()
                .controllerURI(URI.create("tcp://localhost"))
                .build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE, controller, connectionFactory);
        EventStreamWriter<String> writer1 = clientFactory.createEventWriter(streamName, new UTF8StringSerializer(),
                EventWriterConfig.builder().build());
        List<String> routingKeys = new ArrayList<>();
        routingKeys.add("0");
        routingKeys.add("1");
        routingKeys.add("2");
        routingKeys.add("3");
        writer1.writeEvent(routingKeys.get(0), "data").get();
        writer1.writeEvent("0", "data").get();
        long start = System.currentTimeMillis();
        log.info("Segments before scale found as: {}", controller.getCurrentSegments(SCOPE, streamName).get());

        // scale
        List<List<Segment>> segmentsList = scale(controller, SCOPE, streamName, 1, 1, executorService());
        assertNotNull(segmentsList);
        for (int i = 0; i < NUM_EVENTS - 2; i++) {
            writer1.writeEvent(routingKeys.get(random.nextInt(3)), "data").get();
        }
        log.info("Segments after scale found as: {}", controller.getCurrentSegments(SCOPE, streamName).get());
        String readerId = "readerId";
        String readerGroupName = "readerGroup";
        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl(SCOPE, controller, clientFactory);
        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig
                .builder().disableAutomaticCheckpoints().stream(Stream.of(SCOPE, streamName)).groupRefreshTimeMillis(0).build();
        groupManager.createReaderGroup(readerGroupName, readerGroupConfig);
        @Cleanup
        ReaderGroup readerGroup = groupManager.getReaderGroup(readerGroupName);

        StreamSegments streamSegments =  controller.getCurrentSegments(SCOPE, streamName).get();
        EventStreamReader<String> reader1 = clientFactory.createReader(readerId, readerGroupName, new UTF8StringSerializer(),
              ReaderConfig.builder().initialAllocationDelay(0).build());
        readerGroup.initiateCheckpoint("cp", executorService());
        for (int i = 0; i < NUM_EVENTS; i++) {
             EventRead<String> result =  reader1.readNextEvent(1000);
             if (!result.isCheckpoint()) {
             assertNotNull(result);
             assertNotNull(result.getEvent());
             assertTrue(result.getEvent().equals("data"));
             }
        }
        val notificationResults = new ArrayBlockingQueue<SegmentNotification>(10);
        //Add segment event listener
        Listener<SegmentNotification> l1 = notification -> {
            log.info("Number of Segments: {}, Number of Readers: {}", notification.getNumOfSegments(), notification.getNumOfReaders());
            notificationResults.add(notification);
        };
        SegmentNotifier segmentNotifier = (SegmentNotifier) readerGroup.getSegmentNotifier(executorService());
        segmentNotifier.registerListener(l1);
        segmentNotifier.pollNow();
        SegmentNotification initialSegmentNotification = notificationResults.take();
        assertNotNull(initialSegmentNotification);
        assertEquals(initialSegmentNotification.getNumOfReaders(), 1);
        assertEquals(initialSegmentNotification.getNumOfSegments(), NUM_SEGMENTS + 3);
        log.info("notification={}", initialSegmentNotification);
        readerGroup.resetReaderGroup(readerGroupConfig);
        readerGroup.readerOffline("readerId", null);
        long segmentSize = streamSegments.getSegments().size();
        log.info(String.format("Number of active segments expected: %d, actual:%d", NUM_SEGMENTS, segmentSize));
        assertEquals(segmentSize, NUM_SEGMENTS);
    }

    @Test(timeout = 40000)
    public void testEndOfStreamNotifications() throws Exception {
        final String streamName = "stream2";
        StreamConfiguration config = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        Controller controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope(SCOPE).get();
        controller.createStream(SCOPE, streamName, config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder()
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
        Boolean result = controller.scaleStream(stream, Collections.singletonList(0L), map, executorService()).getFuture().get();
        assertTrue(result);
        writer.writeEvent("0", "data2").get();
        assertTrue(controller.sealStream(SCOPE, streamName).get()); // seal stream

        String readerId = "readerId";
        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl(SCOPE, controller, clientFactory);
        groupManager.createReaderGroup("reader", ReaderGroupConfig
                .builder().disableAutomaticCheckpoints().stream(Stream.of(SCOPE, streamName)).groupRefreshTimeMillis(0).build());
        @Cleanup
        ReaderGroup readerGroup = groupManager.getReaderGroup("reader");
        @Cleanup
        EventStreamReader<String> reader1 = clientFactory.createReader("readerId", "reader", new JavaSerializer<>(),
                ReaderConfig.builder().initialAllocationDelay(0).build());

        //Add segment event listener
        Listener<EndOfDataNotification> l1 = notification -> {
            listenerInvoked.set(true);
            listenerLatch.release();
        };
        EndOfDataNotifier endOfDataNotifier = (EndOfDataNotifier) readerGroup.getEndOfDataNotifier(executorService());
        endOfDataNotifier.registerListener(l1);

        EventRead<String> event1 = reader1.readNextEvent(10000);
        assertEquals("data1", event1.getEvent());
        EventRead<String> emptyEvent = reader1.readNextEvent(0);
        assertNull(emptyEvent.getEvent());
        assertFalse(emptyEvent.isCheckpoint());
        readerGroup.initiateCheckpoint("cp", executorService());
        EventRead<String> cpEvent = reader1.readNextEvent(10000);
        assertTrue(cpEvent.isCheckpoint());
        EventRead<String> event2 = reader1.readNextEvent(10000);
        assertEquals("data2", event2.getEvent());
        emptyEvent = reader1.readNextEvent(0);
        assertNull(emptyEvent.getEvent());
        assertFalse(emptyEvent.isCheckpoint());
        emptyEvent = reader1.readNextEvent(0);
        assertNull(emptyEvent.getEvent());
        assertFalse(emptyEvent.isCheckpoint());
        readerGroup.initiateCheckpoint("cp2", executorService());
        cpEvent = reader1.readNextEvent(10000);
        assertTrue(cpEvent.isCheckpoint());
        emptyEvent = reader1.readNextEvent(0);
        assertNull(emptyEvent.getEvent());
        assertFalse(emptyEvent.isCheckpoint());

        endOfDataNotifier.pollNow();
        listenerLatch.await();
        assertTrue("Listener invoked", listenerInvoked.get());
    }
    
    private List<List<Segment>> scale(Controller controller, String scopeName, String streamName, int numSegments, int scalesToPerform, ScheduledExecutorService executor) {
        Stream stream = new StreamImpl(scopeName, streamName);
        AtomicInteger counter = new AtomicInteger(0);
        List<List<Segment>> listOfEpochs = new LinkedList<>();

        while (counter.incrementAndGet() <= scalesToPerform) {
                   controller.getCurrentSegments(scopeName, streamName)
                        .thenCompose(segments -> {
                            ArrayList<Segment> currentSegments = new ArrayList<>(segments.getSegments());
                            listOfEpochs.add(currentSegments);
                            Pair<List<Long>, Map<Double, Double>> scaleInput = getScaleInput(currentSegments);
                            List<Long> segmentsToSeal = scaleInput.getKey();
                            Map<Double, Double> newRanges = scaleInput.getValue();

                            return controller.scaleStream(stream, segmentsToSeal, newRanges, executor)
                                    .getFuture()
                                    .thenAccept(scaleStatus -> {
                                        assertTrue(scaleStatus);
                                        log.info("scale stream for epoch {} completed with status {}", counter.get(), scaleStatus);
                                    });
                        }).join();
        }

        return listOfEpochs;
    }

    /*
     * get the parameter for manually scaling the stream to
     * a number of uniformly partitioned segments.
     */
    Pair<List<Long>, Map<Double, Double>> getScaleInput(ArrayList<Segment> currentSegments) {
        return new ImmutablePair<>(getSegmentsToSeal(currentSegments), getNewRanges());
    }

    private List<Long> getSegmentsToSeal(ArrayList<Segment> currentSegments) {
        return currentSegments.stream()
                .map(Segment::getSegmentId).collect(Collectors.toList());
    }

    private Map<Double, Double> getNewRanges() {
        Map<Double, Double> newRanges = new HashMap<>();
        double delta = 1.0 / NUM_SEGMENTS;
        for (int i = 0; i < NUM_SEGMENTS; i++) {
            double low = delta * i;
            double high = i == NUM_SEGMENTS - 1 ? 1.0 : delta * (i + 1);

            newRanges.put(low, high);
        }
        return newRanges;
    }
}

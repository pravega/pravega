/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.endtoendtest;

import io.pravega.client.ClientConfig;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.server.eventProcessor.LocalController;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.InlineExecutor;
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
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

@Slf4j
public class RetentionTest {

    private static final long CLOCK_ADVANCE_INTERVAL = 60 * 1000000000L;

    private final String serviceHost = "localhost";
    private final int containerCount = 4;
    private final JavaSerializer<String> serializer = new JavaSerializer<>();
    private int controllerPort;
    private URI controllerURI; 
    private TestingServer zkTestServer;
    private PravegaConnectionListener server;
    private ControllerWrapper controllerWrapper;
    private ServiceBuilder serviceBuilder;
    private ScheduledExecutorService executor;

    @Before
    public void setUp() throws Exception {
        executor = ExecutorServiceHelpers.newScheduledThreadPool(10, "test");
        zkTestServer = new TestingServerStarter().start();

        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();
        controllerPort = TestUtils.getAvailableListenPort();
        controllerURI = URI.create("tcp://" + serviceHost + ":" + controllerPort);
        int servicePort = TestUtils.getAvailableListenPort();
        server = new PravegaConnectionListener(false, servicePort, store, tableStore, this.serviceBuilder.getLowPriorityExecutor());
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
    public void testRetentionTime() throws Exception {
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.fixed(2))
                                                        .retentionPolicy(RetentionPolicy.byTime(Duration.ofSeconds(1)))
                                                        .build();
        LocalController controller = (LocalController) controllerWrapper.getController();
        String name = "testtime";
        Stream stream = new StreamImpl(name, name);
        controllerWrapper.getControllerService().createScope(name).get();
        controller.createStream(name, name, config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder()
                                                                                          .controllerURI(controllerURI)
                                                                                          .build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(name, controller, connectionFactory);
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(name, new JavaSerializer<>(),
                EventWriterConfig.builder().build());

        Map<Segment, Long> x = controller.getSegmentsAtTime(stream, 0L).join();
        assertTrue(x.values().stream().allMatch(a -> a == 0));
        AtomicBoolean continueLoop = new AtomicBoolean(true);
        Futures.loop(continueLoop::get, () -> writer.writeEvent("a"), executor);
        
        AssertExtensions.assertEventuallyEquals(true, () -> controller
                .getSegmentsAtTime(stream, 0L).join().values().stream().anyMatch(a -> a > 0), 30 * 1000L);
        continueLoop.set(false);
    }
    
    @Test(timeout = 30000)
    public void testRetentionSize() throws Exception {
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.fixed(2))
                                                        .retentionPolicy(RetentionPolicy.bySizeBytes(10))
                                                        .build();
        LocalController controller = (LocalController) controllerWrapper.getController();
        String name = "testsize";
        Stream stream = new StreamImpl(name, name);
        controllerWrapper.getControllerService().createScope(name).get();
        controller.createStream(name, name, config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder()
                                                                                          .controllerURI(controllerURI)
                                                                                          .build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(name, controller, connectionFactory);
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(name, new JavaSerializer<>(),
                EventWriterConfig.builder().build());

        Map<Segment, Long> x = controller.getSegmentsAtTime(stream, 0L).join();
        assertTrue(x.values().stream().allMatch(a -> a == 0));
        AtomicBoolean continueLoop = new AtomicBoolean(true);
        Futures.loop(continueLoop::get, () -> writer.writeEvent("a"), executor);
        
        AssertExtensions.assertEventuallyEquals(true, () -> controller
                .getSegmentsAtTime(stream, 0L).join().values().stream().anyMatch(a -> a > 0), 30 * 1000L);
        continueLoop.set(false);
    }

    @Test
    public void testReaderGroupAutoRetention() throws Exception {
        String scope = "test";
        String streamName = "test";
        String groupName = "group";
        StreamConfiguration config = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .retentionPolicy(RetentionPolicy.bySizeBytes(100))
                .build();
        LocalController controller = (LocalController) controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope(scope).get();
        controller.createStream(scope, streamName, config).get();
        Stream stream = Stream.of(scope, streamName);
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder()
                .controllerURI(URI.create("tcp://" + serviceHost))
                .build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory);

        // write events
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, serializer,
                EventWriterConfig.builder().build());
        writer.writeEvent("1", "e1").join();
        writer.writeEvent("2", "e2").join();

        // Create a ReaderGroup
        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl(scope, controller, clientFactory);
        groupManager.createReaderGroup(groupName, ReaderGroupConfig.builder().disableAutomaticCheckpoints()
                .stream(NameUtils.getScopedStreamName(scope, streamName)).retentionType(ReaderGroupConfig.StreamDataRetention.AUTOMATIC_RELEASE_AT_LAST_CHECKPOINT).build());

        // Create a Reader
        AtomicLong clock = new AtomicLong();
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader("reader1", groupName, serializer,
                ReaderConfig.builder().build(), clock::get,
                clock::get);
        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        EventRead<String> read = reader.readNextEvent(60000);
        assertEquals("e1", read.getEvent());

        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        @Cleanup("shutdown")
        final InlineExecutor backgroundExecutor = new InlineExecutor();
        ReaderGroup readerGroup = groupManager.getReaderGroup(groupName);
        CompletableFuture<Checkpoint> checkpoint = readerGroup.initiateCheckpoint("Checkpoint", backgroundExecutor);
        assertFalse(checkpoint.isDone());
        read = reader.readNextEvent(60000);
        assertTrue(read.isCheckpoint());
        assertEquals("Checkpoint", read.getCheckpointName());
        assertNull(read.getEvent());

        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        read = reader.readNextEvent(60000);
        assertEquals("e2", read.getEvent());
        Checkpoint cpResult = checkpoint.get(5, TimeUnit.SECONDS);
        assertTrue(checkpoint.isDone());
        assertEquals("Checkpoint", cpResult.getName());
        read = reader.readNextEvent(100);
        assertNull(read.getEvent());
        assertFalse(read.isCheckpoint());

        AssertExtensions.assertEventuallyEquals(true, () -> {
            Map<Segment, Long> segments = controller.getSegmentsAtTime(new StreamImpl(scope, streamName), 0L).join();
            System.out.println("Here: " + segments);
            return segments.values().stream().anyMatch(off -> off > 0);
        }, 30 * 1000L);

        groupManager.createReaderGroup("group2", ReaderGroupConfig.builder().disableAutomaticCheckpoints().stream(NameUtils.getScopedStreamName(scope, streamName)).build());
        EventStreamReader<String> reader2 = clientFactory.createReader("reader2", "group2", serializer, ReaderConfig.builder().build());
        EventRead<String> eventRead2 = reader2.readNextEvent(10000);
        assertEquals("e2", eventRead2.getEvent());
    }

    @Test
    public void testReaderGroupManualRetention() throws Exception {
        String scope = "test";
        String streamName = "test";
        String groupName = "group";
        StreamConfiguration config = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .retentionPolicy(RetentionPolicy.bySizeBytes(100))
                .build();
        LocalController controller = (LocalController) controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope(scope).get();
        controller.createStream(scope, streamName, config).get();
        Stream stream = Stream.of(scope, streamName);
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder()
                .controllerURI(URI.create("tcp://" + serviceHost))
                .build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory);

        // write events
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, serializer,
                EventWriterConfig.builder().build());
        writer.writeEvent("1", "e1").join();
        writer.writeEvent("2", "e2").join();

        // Create a ReaderGroup
        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl(scope, controller, clientFactory);
        groupManager.createReaderGroup(groupName, ReaderGroupConfig.builder().disableAutomaticCheckpoints()
                .stream(NameUtils.getScopedStreamName(scope, streamName)).retentionType(ReaderGroupConfig.StreamDataRetention.MANUAL_RELEASE_AT_USER_STREAMCUT).build());

        // Create a Reader
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader("reader1", groupName, serializer,
                ReaderConfig.builder().build());
        EventRead<String> read = reader.readNextEvent(60000);
        assertEquals("e1", read.getEvent());

        @Cleanup("shutdown")
        final InlineExecutor backgroundExecutor = new InlineExecutor();
        ReaderGroup readerGroup = groupManager.getReaderGroup(groupName);
        CompletableFuture<Map<Stream, StreamCut>> sc = readerGroup.generateStreamCuts(backgroundExecutor);
        assertTrue(Futures.await(sc));
        Map<Stream, StreamCut> scMap = sc.join();
        System.out.println("StreamCut: " + scMap);
        readerGroup.updateRetentionStreamCut(scMap);

        AssertExtensions.assertEventuallyEquals(true, () -> {
            Map<Segment, Long> segments = controller.getSegmentsAtTime(new StreamImpl(scope, streamName), 0L).join();
            System.out.println("Here: " + segments);
            return segments.values().stream().anyMatch(off -> off > 0);
        }, 30 * 1000L);


        groupManager.createReaderGroup("group2", ReaderGroupConfig.builder().disableAutomaticCheckpoints().stream(NameUtils.getScopedStreamName(scope, streamName)).build());
        EventStreamReader<String> reader2 = clientFactory.createReader("reader2", "group2", serializer, ReaderConfig.builder().build());
        EventRead<String> eventRead2 = reader2.readNextEvent(10000);
        assertEquals("e2", eventRead2.getEvent());
    }
}

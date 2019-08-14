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
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.client.watermark.WatermarkSerializer;
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.NameUtils;
import io.pravega.shared.watermarks.Watermark;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import lombok.Cleanup;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

/**
 * Collection of tests to validate controller bootstrap sequence.
 */
public class WatermarkingTest {

    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final int servicePort = TestUtils.getAvailableListenPort();

    private TestingServer zkTestServer;
    private ControllerWrapper controllerWrapper;
    private PravegaConnectionListener server;
    private ServiceBuilder serviceBuilder;
    private StreamSegmentStore store;
    private TableStore tableStore;
    private ScheduledExecutorService executorService;

    @Before
    public void setup() throws Exception {
        final String serviceHost = "localhost";
        final int containerCount = 4;

        // 1. Start ZK
        zkTestServer = new TestingServerStarter().start();

        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        store = serviceBuilder.createStreamSegmentService();
        tableStore = serviceBuilder.createTableStoreService();

        server = new PravegaConnectionListener(false, servicePort, store, tableStore);
        server.startListening();

        // 2. Start controller
        controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), false,
                controllerPort, serviceHost, servicePort, containerCount);

        controllerWrapper.awaitRunning();
        executorService = Executors.newScheduledThreadPool(5);
    }

    @After
    public void cleanup() throws Exception {
        if (controllerWrapper != null) {
            controllerWrapper.close();
        }
        if (server != null) {
            server.close();
        }
        if (serviceBuilder != null) {
            serviceBuilder.close();
        }
        if (zkTestServer != null) {
            zkTestServer.close();
        }
        executorService.shutdown();
    }

    @Test(timeout = 60000)
    public void watermarkTest() throws Exception {
        Controller controller = controllerWrapper.getController();
        String scope = "scope";
        String stream = "stream";
        StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(5)).build();

        URI controllerUri = URI.create("tcp://localhost:" + controllerPort);
        StreamManager streamManager = StreamManager.create(controllerUri);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, config);
        StreamConfiguration markConfig = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build();
        controller.createStream(scope, NameUtils.getMarkStreamForStream(stream), markConfig).join();

        Stream streamObj = Stream.of(scope, stream);

        // create 2 writers
        ClientConfig clientConfig = ClientConfig.builder().controllerURI(controllerUri).build();
        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
        JavaSerializer<String> javaSerializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<String> writer1 = clientFactory.createEventWriter(stream, javaSerializer,
                EventWriterConfig.builder().build());
        @Cleanup
        EventStreamWriter<String> writer2 = clientFactory.createEventWriter(stream, javaSerializer,
                EventWriterConfig.builder().build());

        AtomicBoolean stopFlag = new AtomicBoolean(false);
        // write events
        writeEvents(writer1, stopFlag);
        writeEvents(writer2, stopFlag);

        // scale the stream several times so that we get complex positions
        scale(controller, streamObj, config);

        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(clientConfig);
        @Cleanup
        SynchronizerClientFactory syncClientFactory = SynchronizerClientFactory.withScope(scope, clientConfig);
        @Cleanup
        ReaderGroupManager watermarkingRG = new ReaderGroupManagerImpl(scope, controller, syncClientFactory, connectionFactory);
        String markRG = "watermarkingRG";
        String markStream = NameUtils.getMarkStreamForStream(stream);
        watermarkingRG.createReaderGroup(markRG, ReaderGroupConfig.builder().stream(Stream.of(scope, markStream)).build());

        // create reader
        @Cleanup
        final EventStreamReader<Watermark> markReader = clientFactory.createReader("markreader",
                markRG,
                new WatermarkSerializer(),
                ReaderConfig.builder().build());
        EventRead<Watermark> eventRead = markReader.readNextEvent(30000L);
        while (eventRead.getEvent() == null) {
            eventRead = markReader.readNextEvent(30000L);
        }
        Watermark watermark = eventRead.getEvent();
        stopFlag.set(true);

        // read events from the stream
        @Cleanup
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, controller, syncClientFactory, connectionFactory);
        String readerGroup = "rg";

        long timeLow = watermark.getLowerTimeBound();
        Map<Segment, Long> positionMap0 = watermark.getStreamCut()
                                                   .entrySet().stream().collect(Collectors.toMap(x -> new Segment(scope, stream, x.getKey().getSegmentId()),
                        Map.Entry::getValue));

        StreamCut streamCutStart = new StreamCutImpl(streamObj, positionMap0);
        Map<Stream, StreamCut> start = Collections.singletonMap(streamObj, streamCutStart);
        
        readerGroupManager.createReaderGroup(readerGroup, ReaderGroupConfig.builder().stream(streamObj)
                                                                           .startingStreamCuts(start).build());

        // create reader on the stream
        @Cleanup
        final EventStreamReader<String> reader = clientFactory.createReader("myreader",
                readerGroup,
                javaSerializer,
                ReaderConfig.builder().build());
        
        // read events from the reader. 
        // verify that events read belong to the bound
        EventRead<String> event = reader.readNextEvent(10000L);
        while (event.getEvent() != null) {
            Long time = Long.parseLong(event.getEvent());
            assertTrue(time >= timeLow);
            event = reader.readNextEvent(10000L);
        }
    }

    private void scale(Controller controller, Stream streamObj, StreamConfiguration configuration) {
        // perform several scales
        int numOfSegments = configuration.getScalingPolicy().getMinNumSegments();
        double delta = 1.0 / numOfSegments;
        for (long segmentNumber = 0; segmentNumber < numOfSegments - 1; segmentNumber++) {
            double rangeLow = segmentNumber * delta;
            double rangeHigh = (segmentNumber + 1) * delta;
            double rangeMid = (rangeHigh + rangeLow) / 2;

            Map<Double, Double> map = new HashMap<>();
            map.put(rangeLow, rangeMid);
            map.put(rangeMid, rangeHigh);
            controller.scaleStream(streamObj, Collections.singletonList(segmentNumber), map, executorService).getFuture().join();
        }
    }

    private CompletableFuture<Void> writeEvents(EventStreamWriter<String> writer, AtomicBoolean stopFlag) {
        AtomicInteger count = new AtomicInteger(0);
        AtomicLong currentTime = new AtomicLong();
        return Futures.loop(() -> !stopFlag.get(), () -> Futures.delayedFuture(() -> {
            currentTime.set(System.currentTimeMillis());
            return writer.writeEvent(count.toString(), Long.toString(currentTime.get()))
                         .thenAccept(v -> {
                             if (count.incrementAndGet() % 10 == 0) {
                                 writer.noteTime(currentTime.get());
                             }
                         });
        }, 1000L, executorService), executorService);
    }
}

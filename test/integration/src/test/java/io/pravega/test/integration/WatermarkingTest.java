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
import io.pravega.client.state.Revision;
import io.pravega.client.state.RevisionedStreamClient;
import io.pravega.client.state.SynchronizerConfig;
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
import io.pravega.client.stream.TimeWindow;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.client.control.impl.Controller;
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
import io.pravega.test.common.AssertExtensions;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Collection of tests to validate controller bootstrap sequence.
 */
@Slf4j
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
    private AtomicLong timer = new AtomicLong();

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

        server = new PravegaConnectionListener(false, servicePort, store, tableStore, serviceBuilder.getLowPriorityExecutor());
        server.startListening();

        // 2. Start controller
        controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), false,
                controllerPort, serviceHost, servicePort, containerCount);

        controllerWrapper.awaitRunning();
        executorService = Executors.newScheduledThreadPool(5);
        timer.set(0);
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

    @Test(timeout = 120000)
    public void watermarkTest() throws Exception {
        Controller controller = controllerWrapper.getController();
        String scope = "scope";
        String stream = "stream";
        StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(5)).build();

        URI controllerUri = URI.create("tcp://localhost:" + controllerPort);
        StreamManager streamManager = StreamManager.create(controllerUri);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, config);

        Stream streamObj = Stream.of(scope, stream);

        // create 2 writers
        ClientConfig clientConfig = ClientConfig.builder().controllerURI(controllerUri).build();
        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
        JavaSerializer<Long> javaSerializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<Long> writer1 = clientFactory.createEventWriter(stream, javaSerializer,
                EventWriterConfig.builder().build());
        @Cleanup
        EventStreamWriter<Long> writer2 = clientFactory.createEventWriter(stream, javaSerializer,
                EventWriterConfig.builder().build());

        AtomicBoolean stopFlag = new AtomicBoolean(false);
        // write events
        CompletableFuture<Void> writer1Future = writeEvents(writer1, stopFlag);
        CompletableFuture<Void> writer2Future = writeEvents(writer2, stopFlag);

        // scale the stream several times so that we get complex positions
        scale(controller, streamObj, config);

        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(clientConfig);
        @Cleanup
        SynchronizerClientFactory syncClientFactory = SynchronizerClientFactory.withScope(scope, clientConfig);

        String markStream = NameUtils.getMarkStreamForStream(stream);
        RevisionedStreamClient<Watermark> watermarkReader = syncClientFactory.createRevisionedStreamClient(markStream,
                new WatermarkSerializer(),
                SynchronizerConfig.builder().build());

        LinkedBlockingQueue<Watermark> watermarks = new LinkedBlockingQueue<>();
        fetchWatermarks(watermarkReader, watermarks, stopFlag);

        AssertExtensions.assertEventuallyEquals(true, () -> watermarks.size() >= 2, 100000);

        stopFlag.set(true);

        writer1Future.join();
        writer2Future.join();
        
        // read events from the stream
        @Cleanup
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, controller, syncClientFactory, connectionFactory);
        
        Watermark watermark0 = watermarks.take();
        Watermark watermark1 = watermarks.take();
        assertTrue(watermark0.getLowerTimeBound() <= watermark0.getUpperTimeBound());
        assertTrue(watermark1.getLowerTimeBound() <= watermark1.getUpperTimeBound());
        assertTrue(watermark0.getLowerTimeBound() < watermark1.getLowerTimeBound());

        Map<Segment, Long> positionMap0 = watermark0.getStreamCut().entrySet().stream().collect(
                Collectors.toMap(x -> new Segment(scope, stream, x.getKey().getSegmentId()), Map.Entry::getValue));
        Map<Segment, Long> positionMap1 = watermark1.getStreamCut().entrySet().stream().collect(
                Collectors.toMap(x -> new Segment(scope, stream, x.getKey().getSegmentId()), Map.Entry::getValue));

        StreamCut streamCutFirst = new StreamCutImpl(streamObj, positionMap0);
        StreamCut streamCutSecond = new StreamCutImpl(streamObj, positionMap1);
        Map<Stream, StreamCut> firstMarkStreamCut = Collections.singletonMap(streamObj, streamCutFirst);
        Map<Stream, StreamCut> secondMarkStreamCut = Collections.singletonMap(streamObj, streamCutSecond);
        
        // read from stream cut of first watermark
        String readerGroup = "rg";
        readerGroupManager.createReaderGroup(readerGroup, ReaderGroupConfig.builder().stream(streamObj)
                                                                                         .startingStreamCuts(firstMarkStreamCut)
                                                                                         .endingStreamCuts(secondMarkStreamCut)
                                                                                         .build());

        @Cleanup
        final EventStreamReader<Long> reader = clientFactory.createReader("myreader",
                readerGroup,
                javaSerializer,
                ReaderConfig.builder().build());

        EventRead<Long> event = reader.readNextEvent(10000L);
        TimeWindow currentTimeWindow = reader.getCurrentTimeWindow(streamObj);
        while (event.getEvent() != null && currentTimeWindow.getLowerTimeBound() == null && currentTimeWindow.getUpperTimeBound() == null) {
            event = reader.readNextEvent(10000L);
            currentTimeWindow = reader.getCurrentTimeWindow(streamObj);
        }

        assertNotNull(currentTimeWindow.getUpperTimeBound());

        // read all events and verify that all events are below the bounds
        while (event.getEvent() != null) {
            Long time = event.getEvent();
            log.info("timewindow = {} event = {}", currentTimeWindow, time);
            assertTrue(currentTimeWindow.getLowerTimeBound() == null || time >= currentTimeWindow.getLowerTimeBound());
            assertTrue(currentTimeWindow.getUpperTimeBound() == null || time <= currentTimeWindow.getUpperTimeBound());

            TimeWindow nextTimeWindow = reader.getCurrentTimeWindow(streamObj);
            assertTrue(currentTimeWindow.getLowerTimeBound() == null || nextTimeWindow.getLowerTimeBound() >= currentTimeWindow.getLowerTimeBound());
            assertTrue(currentTimeWindow.getUpperTimeBound() == null || nextTimeWindow.getUpperTimeBound() >= currentTimeWindow.getUpperTimeBound());
            currentTimeWindow = nextTimeWindow;
            
            event = reader.readNextEvent(10000L);
            if (event.isCheckpoint()) {
                event = reader.readNextEvent(10000L);
            }
        }
        
        assertNotNull(currentTimeWindow.getLowerTimeBound());
    }


    @Test(timeout = 120000)
    public void recreateStreamWatermarkTest() throws Exception {
        // in each iteration create stream, note times and let watermark be generated. 
        // then delete stream and move to next iteration and verify that watermarks are generated. 
        for (int i = 0; i < 2; i++) {
            String scope = "scope";
            String stream = "recreate";
            StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(5)).build();

            URI controllerUri = URI.create("tcp://localhost:" + controllerPort);
            StreamManager streamManager = StreamManager.create(controllerUri);
            streamManager.createScope(scope);
            streamManager.createStream(scope, stream, config);
            
            // create writer
            ClientConfig clientConfig = ClientConfig.builder().controllerURI(controllerUri).build();
            @Cleanup
            EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
            JavaSerializer<Long> javaSerializer = new JavaSerializer<>();
            @Cleanup
            EventStreamWriter<Long> writer = clientFactory.createEventWriter(stream, javaSerializer,
                    EventWriterConfig.builder().build());

            AtomicBoolean stopFlag = new AtomicBoolean(false);
            // write events
            CompletableFuture<Void> writerFuture = writeEvents(writer, stopFlag);

            @Cleanup
            SynchronizerClientFactory syncClientFactory = SynchronizerClientFactory.withScope(scope, clientConfig);

            String markStream = NameUtils.getMarkStreamForStream(stream);
            RevisionedStreamClient<Watermark> watermarkReader = syncClientFactory.createRevisionedStreamClient(markStream,
                    new WatermarkSerializer(),
                    SynchronizerConfig.builder().build());

            LinkedBlockingQueue<Watermark> watermarks = new LinkedBlockingQueue<>();
            fetchWatermarks(watermarkReader, watermarks, stopFlag);

            AssertExtensions.assertEventuallyEquals(true, () -> watermarks.size() >= 2, 100000);

            // stop run and seal and delete stream
            stopFlag.set(true);
            writerFuture.join();
            streamManager.sealStream(scope, stream);
            streamManager.deleteStream(scope, stream);
        }
    }

    @Test(timeout = 120000)
    public void watermarkTxnTest() throws Exception {
        Controller controller = controllerWrapper.getController();
        String scope = "scopeTx";
        String stream = "streamTx";
        StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(5)).build();

        URI controllerUri = URI.create("tcp://localhost:" + controllerPort);
        StreamManager streamManager = StreamManager.create(controllerUri);
        streamManager.createScope(scope);
        streamManager.createStream(scope, stream, config);

        Stream streamObj = Stream.of(scope, stream);

        // create 2 writers
        ClientConfig clientConfig = ClientConfig.builder().controllerURI(controllerUri).build();
        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
        JavaSerializer<Long> javaSerializer = new JavaSerializer<>();
        @Cleanup
        TransactionalEventStreamWriter<Long> writer1 = clientFactory
                .createTransactionalEventWriter("writer1", stream, new JavaSerializer<>(),
                EventWriterConfig.builder().transactionTimeoutTime(10000).build());
        @Cleanup
        TransactionalEventStreamWriter<Long> writer2 = clientFactory
                .createTransactionalEventWriter("writer2", stream, new JavaSerializer<>(),
                        EventWriterConfig.builder().transactionTimeoutTime(10000).build());

        AtomicBoolean stopFlag = new AtomicBoolean(false);
        // write events
        CompletableFuture<Void> writer1Future = writeTxEvents(writer1, stopFlag);
        CompletableFuture<Void> writer2Future = writeTxEvents(writer2, stopFlag);

        // scale the stream several times so that we get complex positions
        scale(controller, streamObj, config);

        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(clientConfig);
        @Cleanup
        SynchronizerClientFactory syncClientFactory = SynchronizerClientFactory.withScope(scope, clientConfig);

        String markStream = NameUtils.getMarkStreamForStream(stream);
        RevisionedStreamClient<Watermark> watermarkReader = syncClientFactory.createRevisionedStreamClient(markStream,
                new WatermarkSerializer(),
                SynchronizerConfig.builder().build());

        LinkedBlockingQueue<Watermark> watermarks = new LinkedBlockingQueue<>();
        fetchWatermarks(watermarkReader, watermarks, stopFlag);

        AssertExtensions.assertEventuallyEquals(true, () -> watermarks.size() >= 2, 100000);

        stopFlag.set(true);

        writer1Future.join();
        writer2Future.join();

        // read events from the stream
        @Cleanup
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, controller, syncClientFactory, connectionFactory);

        Watermark watermark0 = watermarks.take();
        Watermark watermark1 = watermarks.take();
        assertTrue(watermark0.getLowerTimeBound() <= watermark0.getUpperTimeBound());
        assertTrue(watermark1.getLowerTimeBound() <= watermark1.getUpperTimeBound());
        assertTrue(watermark0.getLowerTimeBound() < watermark1.getLowerTimeBound());

        Map<Segment, Long> positionMap0 = watermark0.getStreamCut().entrySet().stream().collect(
                Collectors.toMap(x -> new Segment(scope, stream, x.getKey().getSegmentId()), Map.Entry::getValue));
        Map<Segment, Long> positionMap1 = watermark1.getStreamCut().entrySet().stream().collect(
                Collectors.toMap(x -> new Segment(scope, stream, x.getKey().getSegmentId()), Map.Entry::getValue));

        StreamCut streamCutFirst = new StreamCutImpl(streamObj, positionMap0);
        StreamCut streamCutSecond = new StreamCutImpl(streamObj, positionMap1);
        Map<Stream, StreamCut> firstMarkStreamCut = Collections.singletonMap(streamObj, streamCutFirst);
        Map<Stream, StreamCut> secondMarkStreamCut = Collections.singletonMap(streamObj, streamCutSecond);

        // read from stream cut of first watermark
        String readerGroup = "rgTx";
        readerGroupManager.createReaderGroup(readerGroup, ReaderGroupConfig.builder().stream(streamObj)
                                                                           .startingStreamCuts(firstMarkStreamCut)
                                                                           .endingStreamCuts(secondMarkStreamCut)
                                                                           .build());

        @Cleanup
        final EventStreamReader<Long> reader = clientFactory.createReader("myreaderTx",
                readerGroup,
                javaSerializer,
                ReaderConfig.builder().build());

        EventRead<Long> event = reader.readNextEvent(10000L);
        TimeWindow currentTimeWindow = reader.getCurrentTimeWindow(streamObj);
        while (event.getEvent() != null && currentTimeWindow.getLowerTimeBound() == null && currentTimeWindow.getUpperTimeBound() == null) {
            event = reader.readNextEvent(10000L);
            currentTimeWindow = reader.getCurrentTimeWindow(streamObj);
        }

        assertNotNull(currentTimeWindow.getUpperTimeBound());

        // read all events and verify that all events are below the bounds
        while (event.getEvent() != null) {
            Long time = event.getEvent();
            log.info("timewindow = {} event = {}", currentTimeWindow, time);
            assertTrue(currentTimeWindow.getLowerTimeBound() == null || time >= currentTimeWindow.getLowerTimeBound());
            assertTrue(currentTimeWindow.getUpperTimeBound() == null || time <= currentTimeWindow.getUpperTimeBound());

            TimeWindow nextTimeWindow = reader.getCurrentTimeWindow(streamObj);
            assertTrue(currentTimeWindow.getLowerTimeBound() == null || nextTimeWindow.getLowerTimeBound() >= currentTimeWindow.getLowerTimeBound());
            assertTrue(currentTimeWindow.getUpperTimeBound() == null || nextTimeWindow.getUpperTimeBound() >= currentTimeWindow.getUpperTimeBound());
            currentTimeWindow = nextTimeWindow;

            event = reader.readNextEvent(10000L);
            if (event.isCheckpoint()) {
                event = reader.readNextEvent(10000L);
            }
        }

        assertNotNull(currentTimeWindow.getLowerTimeBound());
    }

    @Test(timeout = 60000)
    public void progressingWatermarkWithWriterTimeouts() throws Exception {
        String scope = "Timeout";
        String streamName = "Timeout";
        int numSegments = 1;

        URI controllerUri = URI.create("tcp://localhost:" + controllerPort);

        ClientConfig clientConfig = ClientConfig.builder().controllerURI(controllerUri).build();

        @Cleanup
        StreamManager streamManager = StreamManager.create(controllerUri);
        assertNotNull(streamManager);

        streamManager.createScope(scope);

        streamManager.createStream(scope, streamName, StreamConfiguration.builder()
                                                                         .scalingPolicy(ScalingPolicy.fixed(numSegments))
                                                                         .build());
        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, clientConfig);
        @Cleanup
        SynchronizerClientFactory syncClientFactory = SynchronizerClientFactory.withScope(scope, clientConfig);

        String markStream = NameUtils.getMarkStreamForStream(streamName);
        RevisionedStreamClient<Watermark> watermarkReader = syncClientFactory.createRevisionedStreamClient(markStream,
                new WatermarkSerializer(),
                SynchronizerConfig.builder().build());

        LinkedBlockingQueue<Watermark> watermarks = new LinkedBlockingQueue<>();
        AtomicBoolean stopFlag = new AtomicBoolean(false);
        fetchWatermarks(watermarkReader, watermarks, stopFlag);

        // create two writers and write two sevent and call note time for each writer. 
        @Cleanup
        EventStreamWriter<String> writer1 = clientFactory.createEventWriter(streamName,
                new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        writer1.writeEvent("1").get();
        writer1.noteTime(100L);

        @Cleanup
        EventStreamWriter<String> writer2 = clientFactory.createEventWriter(streamName,
                new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        writer2.writeEvent("2").get();
        writer2.noteTime(102L);

        // writer0 should timeout. writer1 and writer2 should result in two more watermarks with following times:
        // 1: 100L-101L 2: 101-101
        // then first writer should timeout and be discarded. But second writer should continue to be active as its time 
        // is higher than first watermark. This should result in a second watermark to be emitted.  
        AssertExtensions.assertEventuallyEquals(true, () -> watermarks.size() == 2, 100000);
        Watermark watermark1 = watermarks.poll();
        Watermark watermark2 = watermarks.poll();
        assertEquals(100L, watermark1.getLowerTimeBound());
        assertEquals(102L, watermark1.getUpperTimeBound());

        assertEquals(102L, watermark2.getLowerTimeBound());
        assertEquals(102L, watermark2.getUpperTimeBound());

        // stream cut should be same
        assertTrue(watermark2.getStreamCut().entrySet().stream().allMatch(x -> watermark1.getStreamCut().get(x.getKey()).equals(x.getValue())));

        // bring back writer1 and post an event with note time smaller than current watermark
        writer1.writeEvent("3").get();
        writer1.noteTime(101L);

        // no watermark should be emitted. 
        Watermark nullMark = watermarks.poll(10, TimeUnit.SECONDS);
        assertNull(nullMark);
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

    private CompletableFuture<Void> writeEvents(EventStreamWriter<Long> writer, AtomicBoolean stopFlag) {
        AtomicInteger count = new AtomicInteger(0);
        AtomicLong currentTime = new AtomicLong();
        return Futures.loop(() -> !stopFlag.get(), () -> Futures.delayedFuture(() -> {
            currentTime.set(timer.incrementAndGet());
            return writer.writeEvent(count.toString(), currentTime.get())
                         .thenAccept(v -> {
                             if (count.incrementAndGet() % 10 == 0) {
                                 writer.noteTime(currentTime.get());
                             }
                         });
        }, 1000L, executorService), executorService);
    }

    private CompletableFuture<Void> writeTxEvents(TransactionalEventStreamWriter<Long> writer, AtomicBoolean stopFlag) {
        AtomicInteger count = new AtomicInteger(0);
        return Futures.loop(() -> !stopFlag.get(), () -> Futures.delayedFuture(() -> {
            AtomicLong currentTime = new AtomicLong();
            Transaction<Long> txn = writer.beginTxn();
            return CompletableFuture.runAsync(() -> {
                try {
                    for (int i = 0; i < 10; i++) {
                        currentTime.set(timer.incrementAndGet());
                        txn.writeEvent(count.toString(), currentTime.get());
                    }
                    txn.commit(currentTime.get());
                } catch (TxnFailedException e) {
                    throw new CompletionException(e);
                }
            });
        }, 1000L, executorService), executorService);
    }

    private void fetchWatermarks(RevisionedStreamClient<Watermark> watermarkReader, LinkedBlockingQueue<Watermark> watermarks, AtomicBoolean stop) throws Exception {
        AtomicReference<Revision> revision = new AtomicReference<>(watermarkReader.fetchOldestRevision());

        Futures.loop(() -> !stop.get(), () -> Futures.delayedTask(() -> {
            Iterator<Map.Entry<Revision, Watermark>> marks = watermarkReader.readFrom(revision.get());
            if (marks.hasNext()) {
                Map.Entry<Revision, Watermark> next = marks.next();
                log.info("watermark = {}", next.getValue());

                watermarks.add(next.getValue());
                revision.set(next.getKey());
            }
            return null;
        }, Duration.ofSeconds(10), executorService), executorService);
    }

}

/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system;

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
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.client.watermark.WatermarkSerializer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.hash.RandomFactory;
import io.pravega.shared.NameUtils;
import io.pravega.shared.watermarks.Watermark;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.MarathonException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SystemTestRunner.class)
public class WatermarkingTest extends AbstractSystemTest {

    private static final String STREAM = "testWatermarkingStream";
    private static final String SCOPE = "testWatermarkingScope" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final String READER_GROUP = "testWatermarkingReaderGroup" + RandomFactory.create().nextInt(Integer.MAX_VALUE);

    @Rule
    public Timeout globalTimeout = Timeout.seconds(10 * 60);

    private final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(5);
    private final StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(scalingPolicy).build();
    private URI controllerURI;
    private StreamManager streamManager;
    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(5);
    Service controllerInstance;

    /**
     * This is used to setup the various services required by the system test framework.
     *
     * @throws MarathonException    when error in setup
     */
    @Environment
    public static void initialize() throws MarathonException, ExecutionException {
        URI zkUri = startZookeeperInstance();
        startBookkeeperInstances(zkUri);
        URI controllerUri = startPravegaControllerInstances(zkUri, 2);
        ensureSegmentStoreRunning(zkUri, controllerUri);
    }

    @Before
    public void setup() {
        controllerInstance = Utils.createPravegaControllerService(null);
        List<URI> ctlURIs = controllerInstance.getServiceDetails();
        controllerURI = ctlURIs.get(0);
        streamManager = StreamManager.create(Utils.buildClientConfig(controllerURI));
        assertTrue("Creating Scope", streamManager.createScope(SCOPE));
        assertTrue("Creating stream", streamManager.createStream(SCOPE, STREAM, config));
        assertTrue("Creating mark stream", streamManager.createStream(SCOPE, NameUtils.getMarkStreamForStream(STREAM), config));
    }

    @After
    public void tearDown() {
        streamManager.close();
    }

    @Test
    public void watermarkingTests() throws Exception {
        final ClientConfig clientConfig = Utils.buildClientConfig(controllerURI);
        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(clientConfig);
        ControllerImpl controller = new ControllerImpl(ControllerImplConfig.builder().clientConfig(clientConfig).build(),
                                                       connectionFactory.getInternalExecutor());
        // create 2 writers
        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(SCOPE, clientConfig);
        JavaSerializer<String> javaSerializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<String> writer1 = clientFactory.createEventWriter(STREAM, javaSerializer,
                EventWriterConfig.builder().build());
        @Cleanup
        EventStreamWriter<String> writer2 = clientFactory.createEventWriter(STREAM, javaSerializer,
                EventWriterConfig.builder().build());

        AtomicBoolean stopFlag = new AtomicBoolean(false);
        // write events
        writeEvents(writer1, stopFlag);
        writeEvents(writer2, stopFlag);

        // scale the stream several times so that we get complex positions
        Stream streamObj = Stream.of(SCOPE, STREAM);
        scale(controller, streamObj);

        @Cleanup
        SynchronizerClientFactory syncClientFactory = SynchronizerClientFactory.withScope(SCOPE, clientConfig);
        @Cleanup
        ReaderGroupManager watermarkingRG = new ReaderGroupManagerImpl(SCOPE, controller, syncClientFactory, connectionFactory);
        String markRG = "watermarkingRG";
        String markStream = NameUtils.getMarkStreamForStream(STREAM);
        watermarkingRG.createReaderGroup(markRG, ReaderGroupConfig.builder().stream(Stream.of(SCOPE, markStream)).build());

        // create reader
        @Cleanup
        final EventStreamReader<Watermark> markReader = clientFactory.createReader("markreader",
                markRG,
                new WatermarkSerializer(),
                ReaderConfig.builder().build());
        List<Watermark> watermarks = new LinkedList<>();

        // wait until we have generated 3 watermarks.
        while (watermarks.size() < 2) {
            EventRead<Watermark> eventRead = markReader.readNextEvent(30000L);
            if (eventRead.getEvent() != null) {
                watermarks.add(eventRead.getEvent());
            }
        }

        // scale down one controller instance. 
        Futures.getAndHandleExceptions(controllerInstance.scaleService(1), ExecutionException::new);

        // wait until at least 2 more watermarks are emitted
        while (watermarks.size() < 4) {
            EventRead<Watermark> eventRead = markReader.readNextEvent(30000L);
            if (eventRead.getEvent() != null) {
                watermarks.add(eventRead.getEvent());
            }
        }

        stopFlag.set(true);

        assertTrue(watermarks.get(0).getLowerTimeBound() < watermarks.get(0).getUpperTimeBound());
        assertTrue(watermarks.get(1).getLowerTimeBound() < watermarks.get(1).getUpperTimeBound());
        assertTrue(watermarks.get(2).getLowerTimeBound() < watermarks.get(2).getUpperTimeBound());
        assertTrue(watermarks.get(3).getLowerTimeBound() < watermarks.get(3).getUpperTimeBound());

        // verify that watermarks are increasing in time.
        assertTrue(watermarks.get(0).getLowerTimeBound() < watermarks.get(1).getLowerTimeBound());
        assertTrue(watermarks.get(1).getLowerTimeBound() < watermarks.get(2).getLowerTimeBound());
        assertTrue(watermarks.get(2).getLowerTimeBound() < watermarks.get(3).getLowerTimeBound());
        
        // use watermark as lower and upper bounds.
        long timeLow = watermarks.get(0).getLowerTimeBound();
        Map<Segment, Long> positionMap0 = watermarks.get(0).getStreamCut()
                                                    .entrySet().stream()
                                                    .collect(Collectors.toMap(x ->
                                                                    new Segment(SCOPE, STREAM, x.getKey().getSegmentId()),
                                                            Map.Entry::getValue));

        StreamCut streamCutStart = new StreamCutImpl(streamObj, positionMap0);
        Map<Stream, StreamCut> start = Collections.singletonMap(streamObj, streamCutStart);

        Map<Segment, Long> positionMap2 = watermarks.get(2).getStreamCut()
                                                    .entrySet().stream()
                                                    .collect(Collectors.toMap(x ->
                                                                    new Segment(SCOPE, STREAM, x.getKey().getSegmentId()),
                                                            Map.Entry::getValue));

        StreamCut streamCutEnd = new StreamCutImpl(streamObj, positionMap2);
        Map<Stream, StreamCut> end = Collections.singletonMap(streamObj, streamCutEnd);

        @Cleanup
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(SCOPE, controller, syncClientFactory, connectionFactory);
        String readerGroup = "rg";

        readerGroupManager.createReaderGroup(readerGroup, ReaderGroupConfig.builder().stream(streamObj)
                                                                           .startingStreamCuts(start)
                                                                           .endingStreamCuts(end).build());

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

    private void scale(Controller controller, Stream streamObj) throws InterruptedException {
        // perform several scales
        int numOfSegments = config.getScalingPolicy().getMinNumSegments();
        double delta = 1.0 / numOfSegments;
        for (long segmentNumber = 0; segmentNumber < numOfSegments - 1; segmentNumber++) {
            Thread.sleep(3000L);
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

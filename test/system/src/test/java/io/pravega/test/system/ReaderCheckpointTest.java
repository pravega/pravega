/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.pravega.common.concurrent.Futures;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.services.BookkeeperService;
import io.pravega.test.system.framework.services.PravegaControllerService;
import io.pravega.test.system.framework.services.PravegaSegmentStoreService;
import io.pravega.test.system.framework.services.Service;
import io.pravega.test.system.framework.services.ZookeeperService;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RunWith(SystemTestRunner.class)
public class ReaderCheckpointTest {

    private static final long READ_TIMEOUT = SECONDS.toMillis(30);
    private static final int RANDOM_SUFFIX = new Random().nextInt(Integer.MAX_VALUE);
    private static final String SCOPE = "scope" + RANDOM_SUFFIX;
    private static final String STREAM = "checkPointTestStream";
    private static final String READER_GROUP_NAME = "checkpointTest" + RANDOM_SUFFIX;
    private static final int NUMBER_OF_READERS = 3; //this matches the number of segments in the stream

    @Rule
    public Timeout globalTimeout = Timeout.seconds(7 * 60);

    private final ReaderConfig readerConfig = ReaderConfig.builder().build();
    private final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder().build();
    private final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "checkPointExecutor");
    private final StreamConfiguration streamConfig = StreamConfiguration.builder()
                                                                        .scalingPolicy(ScalingPolicy.fixed(NUMBER_OF_READERS)).build();
    private final ScheduledExecutorService readerExecutor = ExecutorServiceHelpers.newScheduledThreadPool(NUMBER_OF_READERS,
            "readerCheckpointTest-reader");

    private URI controllerURI;

    @Environment
    public static void initialize() throws Exception {

        //1. check if zk is running, if not start it
        Service zkService = new ZookeeperService("zookeeper");
        if (!zkService.isRunning()) {
            zkService.start(true);
        }

        List<URI> zkUris = zkService.getServiceDetails();
        log.debug("Zookeeper service details: {}", zkUris);

        //get the zk ip details and pass it to bk, host, controller
        //2, check if bk is running, otherwise start, get the zk ip
        Service bkService = new BookkeeperService("bookkeeper", zkUris.get(0));
        if (!bkService.isRunning()) {
            bkService.start(true);
        }
        log.debug("Bookkeeper service details: {}", bkService.getServiceDetails());

        //3. start controller
        Service conService = new PravegaControllerService("controller", zkUris.get(0));
        if (!conService.isRunning()) {
            conService.start(true);
        }
        List<URI> conUris = conService.getServiceDetails();
        log.debug("Pravega Controller service details: {}", conUris);

        //4.start host
        Service segService = new PravegaSegmentStoreService("segmentstore", zkUris.get(0), conUris.get(0));
        if (!segService.isRunning()) {
            segService.start(true);
        }
        log.debug("Pravega segment store details: {}", segService.getServiceDetails());
    }

    @Before
    public void setup() throws URISyntaxException {
        controllerURI = fetchControllerURI();
        StreamManager streamManager = StreamManager.create(controllerURI);
        assertTrue("Creating Scope", streamManager.createScope(SCOPE));
        assertTrue("Creating stream", streamManager.createStream(SCOPE, STREAM, streamConfig));
    }

    @Test
    public void readerCheckpointTest() throws Exception {

        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(SCOPE, controllerURI);
        ReaderGroup readerGroup = readerGroupManager.createReaderGroup(READER_GROUP_NAME, readerGroupConfig,
                Collections.singleton(STREAM));

        int startInclusive = 1;
        int endExclusive = 100;
        log.info("Write events with range [{},{})", startInclusive, endExclusive);
        writeEvents(IntStream.range(startInclusive, endExclusive).boxed().collect(Collectors.toList()));
        readEventsAndVerify(startInclusive, endExclusive);

        //initiate checkpoint100
        Checkpoint checkPoint100 = createCheckPointAndVerify(readerGroup, "batch100");

        //write and read events 100 to 200
        startInclusive = 100;
        endExclusive = 200;
        log.info("Write events with range [{},{})", startInclusive, endExclusive);
        writeEvents(IntStream.range(startInclusive, endExclusive).boxed().collect(Collectors.toList()));
        readEventsAndVerify(startInclusive, endExclusive);

        //reset to check point 100
        readerGroup.resetReadersToCheckpoint(checkPoint100);
        readEventsAndVerify(100, endExclusive);

        //initiate checkpoint200
        Checkpoint checkPoint200 = createCheckPointAndVerify(readerGroup, "batch200");

        //write and read events 200 to 300
        startInclusive = 200;
        endExclusive = 300;
        log.info("Write events with range [{},{})", startInclusive, endExclusive);
        writeEvents(IntStream.range(startInclusive, endExclusive).boxed().collect(Collectors.toList()));
        readEventsAndVerify(startInclusive, endExclusive);

        //reset back to checkpoint 200
        readerGroup.resetReadersToCheckpoint(checkPoint200);
        readEventsAndVerify(200, endExclusive);

        //reset back to checkpoint 100
        readerGroup.resetReadersToCheckpoint(checkPoint100);
        readEventsAndVerify(100, endExclusive);

        readerGroupManager.deleteReaderGroup(READER_GROUP_NAME); //clean up
    }

    private Checkpoint createCheckPointAndVerify(final ReaderGroup readerGroup, final String checkPointName) {
        log.info("Create and verify check point {}", checkPointName);
        String readerId = "checkPointReader";
        CompletableFuture<Checkpoint> checkpoint = null;

        try (ClientFactory clientFactory = ClientFactory.withScope(SCOPE, controllerURI);
             EventStreamReader<Integer> reader = clientFactory.createReader(readerId, READER_GROUP_NAME,
                     new JavaSerializer<Integer>(), readerConfig)) {

            checkpoint = readerGroup.initiateCheckpoint(checkPointName, executor); //create checkpoint

            //verify checkpoint event.
            EventRead<Integer> event = reader.readNextEvent(READ_TIMEOUT);
            assertTrue("Read for Checkpoint event", (event != null) && (event.isCheckpoint()));
            assertEquals("CheckPoint Name", checkPointName, event.getCheckpointName());
        } catch (ReinitializationRequiredException e) {
            log.error("Exception while reading event using readerId: {}", readerId, e);
            fail("Reinitialization Exception is not expected");
        }
        return checkpoint.join();
    }

    private void readEventsAndVerify(int startInclusive, int endExclusive) {
        log.info("Read and Verify events between [{},{})", startInclusive, endExclusive);
        final List<CompletableFuture<List<EventRead<Integer>>>> readResults = new ArrayList<>();

        //start reading using configured number of readers
        for (int i = 0; i < NUMBER_OF_READERS; i++) {
            readResults.add(asyncReadEvents("reader-" + i));
        }

        //results from all readers
        List<List<EventRead<Integer>>> results = Futures.allOfWithResults(readResults).join();
        List<EventRead<Integer>> eventsRead = results.stream().flatMap(List::stream).collect(Collectors.toList());

        verifyEvents(eventsRead, startInclusive, endExclusive);
    }

    private CompletableFuture<List<EventRead<Integer>>> asyncReadEvents(final String readerId) {
        CompletableFuture<List<EventRead<Integer>>> result = CompletableFuture.supplyAsync(
                () -> readEvents(readerId), readerExecutor);
        Futures.exceptionListener(result,
                t -> log.error("Error observed while reading events for reader id :{}", readerId, t));
        return result;
    }

    private void verifyEvents(final List<EventRead<Integer>> events, int startInclusive, int endExclusive) {

        Supplier<Stream<Integer>> streamSupplier = () -> events.stream().map(i -> i.getEvent()).sorted();
        IntSummaryStatistics stats = streamSupplier.get().collect(Collectors.summarizingInt(value -> value));

        assertTrue("Check for first event", stats.getMin() == startInclusive);
        assertTrue("Check for last event", stats.getMax() == endExclusive - 1);
        //Check for missing events
        assertEquals("Check for number of events", endExclusive - startInclusive, stats.getCount());
        assertEquals("Check for duplicate events", endExclusive - startInclusive,
                streamSupplier.get().distinct().count());
    }

    private <T extends Serializable> List<EventRead<T>> readEvents(final String readerId) {
        List<EventRead<T>> events = new ArrayList<>();

        try (ClientFactory clientFactory = ClientFactory.withScope(SCOPE, controllerURI);
             EventStreamReader<T> reader = clientFactory.createReader(readerId,
                     READER_GROUP_NAME,
                     new JavaSerializer<T>(),
                     readerConfig)) {
            log.info("Reading events from {}/{} with readerId: {}", SCOPE, STREAM, readerId);
            EventRead<T> event = null;
            do {
                try {
                    event = reader.readNextEvent(READ_TIMEOUT);
                    if (event.getEvent() != null) {
                        log.info("Read event {}", event.getEvent());
                        events.add(event);
                    }
                } catch (ReinitializationRequiredException e) {
                    log.error("Exception while reading event using readerId: {}", readerId, e);
                    fail("Reinitialization Exception is not expected");
                }
            } while (event.getEvent() != null);
            log.info("No more events from {}/{} for readerId: {}", SCOPE, STREAM, readerId);
        } //reader.close() will automatically invoke ReaderGroup#readerOffline(String, Position)
        return events;
    }

    private <T extends Serializable> void writeEvents(final List<T> events) {
        try (ClientFactory clientFactory = ClientFactory.withScope(SCOPE, controllerURI);
             EventStreamWriter<T> writer = clientFactory.createEventWriter(STREAM,
                     new JavaSerializer<T>(),
                     EventWriterConfig.builder().build())) {
            for (T event : events) {
                String routingKey = String.valueOf(event);
                log.info("Writing message: {} with routing-key: {} to stream {}", event, routingKey, STREAM);
                writer.writeEvent(routingKey, event);
            }
        }
    }

    private URI fetchControllerURI() {
        Service conService = new PravegaControllerService("controller", null);
        List<URI> ctlURIs = conService.getServiceDetails();
        return ctlURIs.get(0);
    }
}

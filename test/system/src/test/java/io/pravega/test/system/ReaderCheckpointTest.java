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
import io.pravega.common.concurrent.Futures;
import io.pravega.common.hash.RandomFactory;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Slf4j
@RunWith(SystemTestRunner.class)
public class ReaderCheckpointTest extends AbstractSystemTest {

    private static final long READ_TIMEOUT = SECONDS.toMillis(30);
    private static final int RANDOM_SUFFIX = RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final String SCOPE = "scope" + RANDOM_SUFFIX;
    private static final String STREAM = "checkPointTestStream";
    private static final String READER_GROUP_NAME = "checkpointTest" + RANDOM_SUFFIX;
    private static final int NUMBER_OF_READERS = 3; //this matches the number of segments in the stream

    @Rule
    public Timeout globalTimeout = Timeout.seconds(7 * 60);

    private final ReaderConfig readerConfig = ReaderConfig.builder().build();
    private final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "checkPointExecutor");
    private final StreamConfiguration streamConfig = StreamConfiguration.builder()
                                                                        .scalingPolicy(ScalingPolicy.fixed(NUMBER_OF_READERS)).build();
    private final ScheduledExecutorService readerExecutor = ExecutorServiceHelpers.newScheduledThreadPool(NUMBER_OF_READERS,
            "readerCheckpointTest-reader");

    private URI controllerURI;

    @Environment
    public static void initialize() throws ExecutionException {
        URI zkUri = startZookeeperInstance();
        startBookkeeperInstances(zkUri);
        URI controllerUri = startPravegaControllerInstances(zkUri, 1);
        startPravegaSegmentStoreInstances(zkUri, controllerUri, 1);
    }

    @Before
    public void setup() {
        controllerURI = fetchControllerURI();
        StreamManager streamManager = StreamManager.create(controllerURI);
        assertTrue("Creating Scope", streamManager.createScope(SCOPE));
        assertTrue("Creating stream", streamManager.createStream(SCOPE, STREAM, streamConfig));
    }

    @After
    public void tearDown() {
        ExecutorServiceHelpers.shutdown(readerExecutor);
    }

    @Test
    public void readerCheckpointTest() {

        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(SCOPE, controllerURI);
        readerGroupManager.createReaderGroup(READER_GROUP_NAME,
                ReaderGroupConfig.builder().stream(io.pravega.client.stream.Stream.of(SCOPE, STREAM)).build());
        @Cleanup
        ReaderGroup readerGroup = readerGroupManager.getReaderGroup(READER_GROUP_NAME);

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
        readerGroup.resetReaderGroup(ReaderGroupConfig.builder().startFromCheckpoint(checkPoint100).build());
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
        readerGroup.resetReaderGroup(ReaderGroupConfig.builder().startFromCheckpoint(checkPoint200).build());
        readEventsAndVerify(200, endExclusive);

        //reset back to checkpoint 100
        readerGroup.resetReaderGroup(ReaderGroupConfig.builder().startFromCheckpoint(checkPoint100).build());
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

        assertTrue(String.format("Check for first event: %d, %d", stats.getMin(), startInclusive),
                stats.getMin() == startInclusive);
        assertTrue(String.format("Check for last event: %d, %d", stats.getMax(), endExclusive),
                stats.getMax() == endExclusive - 1);
        //Check for missing events
        assertEquals(String.format("Check for number of events: %d, %d, %d", endExclusive, startInclusive, stats.getCount()),
                endExclusive - startInclusive, stats.getCount());
        assertEquals(String.format("Check for duplicate events: %d, %d, %d", endExclusive, startInclusive, streamSupplier.get().distinct().count()),
                endExclusive - startInclusive, streamSupplier.get().distinct().count());
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
                    if (event.isCheckpoint()) {
                        log.info("Read a check point event, checkpointName: {}", event.getCheckpointName());
                    }
                } catch (ReinitializationRequiredException e) {
                    log.error("Exception while reading event using readerId: {}", readerId, e);
                    fail("Reinitialization Exception is not expected");
                }
            } while (event.isCheckpoint() || event.getEvent() != null);
            //stop reading if event read(non-checkpoint) is null.
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
        Service conService = Utils.createPravegaControllerService(null);
        List<URI> ctlURIs = conService.getServiceDetails();
        return ctlURIs.get(0);
    }
}

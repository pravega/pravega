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
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.hash.RandomFactory;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.MarathonException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SystemTestRunner.class)
public class StreamCutsTest {

    private static final String STREAM_ONE = "streamCutsStreamOne";
    private static final String STREAM_TWO = "streamCutsStreamTwo";
    private static final String SCOPE = "streamCutsScope" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final String READER_GROUP = "streamCutsRG" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    @Rule
    public Timeout globalTimeout = Timeout.seconds(8 * 60);

    private final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(4, "executor");
    private URI controllerURI;
    private StreamManager streamManager;

    /**
     * This is used to setup the services required by the system test framework.
     *
     * @throws MarathonException When error in setup.
     */
    @Environment
    public static void initialize() throws MarathonException {

        // 1. Check if zk is running, if not start it.
        Service zkService = Utils.createZookeeperService();
        if (!zkService.isRunning()) {
            zkService.start(true);
        }

        List<URI> zkUris = zkService.getServiceDetails();
        log.debug("Zookeeper service details: {}", zkUris);
        // Get the zk ip details and pass it to bk, host, controller.
        URI zkUri = zkUris.get(0);

        // 2. Check if bk is running, otherwise start, get the zk ip.
        Service bkService = Utils.createBookkeeperService(zkUri);
        if (!bkService.isRunning()) {
            bkService.start(true);
        }

        List<URI> bkUris = bkService.getServiceDetails();
        log.debug("Bookkeeper service details: {}", bkUris);

        // 3. Start controller.
        Service conService = Utils.createPravegaControllerService(zkUri);
        if (!conService.isRunning()) {
            conService.start(true);
        }

        List<URI> conUris = conService.getServiceDetails();
        log.debug("Pravega controller service details: {}", conUris);

        // 4.Start segmentstore.
        Service segService = Utils.createPravegaSegmentStoreService(zkUri, conUris.get(0));
        if (!segService.isRunning()) {
            segService.start(true);
        }

        List<URI> segUris = segService.getServiceDetails();
        log.debug("Pravega segmentstore service details: {}", segUris);
    }

    @Before
    public void setup() {
        Service conService = Utils.createPravegaControllerService(null);
        List<URI> ctlURIs = conService.getServiceDetails();
        controllerURI = ctlURIs.get(0);
        streamManager = StreamManager.create(controllerURI);
        assertTrue("Creating scope", streamManager.createScope(SCOPE));
        assertTrue("Creating stream one", streamManager.createStream(SCOPE, STREAM_ONE,
                StreamConfiguration.builder().scope(SCOPE).streamName(STREAM_ONE).scalingPolicy(ScalingPolicy.fixed(1)).build()));
        assertTrue("Creating stream two", streamManager.createStream(SCOPE, STREAM_TWO,
                StreamConfiguration.builder().scope(SCOPE).streamName(STREAM_TWO).scalingPolicy(ScalingPolicy.fixed(2)).build()));
    }

    /**
     * This test verifies the correct operation of readers using StreamCuts. Concretely, the test creates two streams
     * with different number of segments and it writes some events (totalEvents) in them. Afterwards, the test creates a
     * list of StreamCuts that encompasses both streams every sliceSize events. The test asserts that new groups of
     * readers can be initialized at these StreamCut intervals and that only sliceSize events are read.
     */
    @Test
    public void streamCutsTest() {
        final int totalEvents = 1000;
        final int sliceSize = 100;
        @Cleanup
        ClientFactory clientFactory = ClientFactory.withScope(SCOPE, controllerURI);
        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(SCOPE, controllerURI);
        readerGroupManager.createReaderGroup(READER_GROUP, ReaderGroupConfig.builder().stream(Stream.of(SCOPE, STREAM_ONE))
                                                                            .stream(Stream.of(SCOPE, STREAM_TWO))
                                                                            .build());
        // First, write half of events in each Stream.
        writeDummyEvents(clientFactory, STREAM_ONE, totalEvents / 2);
        writeDummyEvents(clientFactory, STREAM_TWO, totalEvents / 2);
        log.debug("Finished writing events to streams.");

        // Second, get StreamCuts for each slice from both Streams at the same time (may be different in each execution).
        @Cleanup
        ReaderGroup readerGroup = readerGroupManager.getReaderGroup(READER_GROUP);
        List<Map<Stream, StreamCut>> streamSlices = getStreamCutSlices(clientFactory, readerGroup, sliceSize);
        log.debug("Finished creating StreamCuts.");

        // Third, ensure that reader groups can correctly read slices from different Streams.
        int groupId = 0;
        Map<Stream, StreamCut> startingPoint = null;
        for (Map<Stream, StreamCut> endingPoint : streamSlices) {
            ReaderGroupConfig config = (groupId == 0) ? ReaderGroupConfig.builder().endingStreamCuts(endingPoint).build() :
                    ReaderGroupConfig.builder().startingStreamCuts(startingPoint).endingStreamCuts(endingPoint).build();

            // Create a new reader group per stream cut slice and read only events within the cut.
            String readerGroupId = READER_GROUP + String.valueOf(groupId);
            readerGroupManager.createReaderGroup(readerGroupId, config);
            int readEvents = readDummyEvents(clientFactory, readerGroupId, 3).stream().map(CompletableFuture::join)
                                                                             .reduce((a, b) -> a + b).get();
            log.debug("Read events by group {}: {}", readerGroupId, readEvents);
            assertEquals("Expected events read: ", sliceSize, readEvents);
            startingPoint = endingPoint;
            groupId++;
        }

        log.debug("All events correctly read from StreamCut slices on multiple Streams. StreamCuts test passed.");
    }

    @After
    public void tearDown() {
        streamManager.close();
        ExecutorServiceHelpers.shutdown(executor);
    }

    // Start utils region

    private <T extends Serializable> List<Map<Stream, StreamCut>> getStreamCutSlices(ClientFactory client, ReaderGroup readerGroup, int slice) {
        @Cleanup
        EventStreamReader<T> reader = client.createReader("slicer", readerGroup.getGroupName(), new JavaSerializer<>(),
                ReaderConfig.builder().build());
        List<Map<Stream, StreamCut>> streamCuts = new ArrayList<>();
        EventRead<T> event;
        int validEvents = 1;
        try {
            do {
                event = reader.readNextEvent(1000);
                if (event.getEvent() != null) {
                    validEvents++;
                }

                // Get a StreamCut each for each slice.
                if (validEvents % slice == 0) {
                    Map<Stream, StreamCut> newCut = readerGroup.getStreamCuts();
                    log.debug("Creating {} StreamCuts in a snapshot at event {}.", streamCuts.size(), validEvents);
                    for (Stream stream: newCut.keySet())
                        log.debug("Stream {}, StreamCut info: {}", stream.getScopedName(), newCut.get(stream).asImpl().toString());
                    streamCuts.add(newCut);
                }
            } while (event.getEvent() != null || event.isCheckpoint());
        } catch (ReinitializationRequiredException | RuntimeException e) {
            log.error("Exception while reading event: ", e);
        }

        return streamCuts;
    }

    private void writeDummyEvents(ClientFactory clientFactory, String streamName, int totalEvents) {
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        for (int i = 0; i < totalEvents; i++) {
            writer.writeEvent(streamName + String.valueOf(i)).join();
            log.debug("Writing event: {} to stream {}", streamName + String.valueOf(i), streamName);
        }
    }

    private List<CompletableFuture<Integer>> readDummyEvents(ClientFactory client, String rGroup, int numReaders) {
        List<EventStreamReader<String>> readers = new ArrayList<>();
        for (int i = 0; i < numReaders; i++) {
            readers.add(client.createReader(String.valueOf(i), rGroup, new JavaSerializer<>(), ReaderConfig.builder().build()));
        }

        return readers.stream().map(r -> CompletableFuture.supplyAsync(() -> readEvents(r))).collect(toList());
    }

    private <T> int readEvents(EventStreamReader<T> reader) {
        EventRead<T> event;
        int validEvents = 0;
        try {
            do {
                event = reader.readNextEvent(1000);
                if (event.getEvent() != null) {
                    validEvents++;
                }
            } while (event.getEvent() != null || event.isCheckpoint());

            reader.close();
        } catch (ReinitializationRequiredException | RuntimeException e) {
            log.error("Exception while reading event: ", e);
        }

        return validEvents;
    }

    // End utils region
}

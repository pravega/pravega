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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
@RunWith(SystemTestRunner.class)
public class StreamCutsTest extends AbstractReadWriteTest {

    private static final String STREAM_ONE = "testStreamcutsStreamOne";
    private static final int RG_PARALLELISM_ONE = 1;
    private static final String STREAM_TWO = "testStreamcutsStreamTwo";
    private static final int RG_PARALLELISM_TWO = 2;
    private static final String SCOPE = "testStreamcutsStreamScope" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final String READER_GROUP = "testStreamcutsStreamRG" + RandomFactory.create().nextInt(Integer.MAX_VALUE);

    private static final int TOTAL_EVENTS = 5000;
    private static final int CUT_SIZE = 1000;

    @Rule
    public Timeout globalTimeout = Timeout.seconds(8 * 60);
    private final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(4, "executor");
    private URI controllerURI = null;
    private StreamManager streamManager  = null;

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
                StreamConfiguration.builder().scope(SCOPE).streamName(STREAM_ONE)
                                   .scalingPolicy(ScalingPolicy.fixed(RG_PARALLELISM_ONE)).build()));
        assertTrue("Creating stream two", streamManager.createStream(SCOPE, STREAM_TWO,
                StreamConfiguration.builder().scope(SCOPE).streamName(STREAM_TWO)
                                   .scalingPolicy(ScalingPolicy.fixed(RG_PARALLELISM_TWO)).build()));
    }

    /**
     * This test verifies the correct operation of readers using StreamCuts. Concretely, the test creates two streams
     * with different number of segments and it writes some events (TOTAL_EVENTS) in them. Afterwards, the test creates
     * a list of StreamCuts that encompasses both streams every CUT_SIZE events. The test asserts that new groups of
     * readers can be initialized at these StreamCut intervals and that only CUT_SIZE events are read. Finally, this
     * test also verifies that an existing reader group can be reset to the starting position in the stream.
     */
    @Test
    public void streamCutsTest() {
        @Cleanup
        ClientFactory clientFactory = ClientFactory.withScope(SCOPE, controllerURI);
        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(SCOPE, controllerURI);
        readerGroupManager.createReaderGroup(READER_GROUP, ReaderGroupConfig.builder()
                                                                            .stream(Stream.of(SCOPE, STREAM_ONE))
                                                                            .stream(Stream.of(SCOPE, STREAM_TWO))
                                                                            .build());
        @Cleanup
        ReaderGroup readerGroup = readerGroupManager.getReaderGroup(READER_GROUP);

        // First, write half of events in each Stream.
        writeDummyEvents(clientFactory, STREAM_ONE, TOTAL_EVENTS / 2);
        writeDummyEvents(clientFactory, STREAM_TWO, TOTAL_EVENTS / 2);
        log.debug("Finished writing events to streams.");

        // Second, get StreamCuts for each slice from both Streams at the same time (may be different in each execution).
        List<Map<Stream, StreamCut>> streamSlices = getStreamCutSlices(clientFactory, readerGroup);
        log.debug("Finished creating StreamCuts.");

        // Third, ensure that reader groups can correctly read slice by slice from different Streams.
        readSliceBySliceAndVerify(readerGroupManager, clientFactory, streamSlices);

        // Fourth, perform different combinations of StreamCuts and verify that read event boundaries are still correct.
        combineSlicesAndVerify(readerGroupManager, clientFactory, streamSlices);

        // Finally, Test that a reader group can be reset correctly.
        ReaderGroupConfig firsSliceConfig = ReaderGroupConfig.builder()
                                                             .stream(Stream.of(SCOPE, STREAM_ONE))
                                                             .stream(Stream.of(SCOPE, STREAM_TWO))
                                                             .startingStreamCuts(streamSlices.get(0))
                                                             .build();
        readerGroup.resetReaderGroup(firsSliceConfig);
        log.info("Resetting existing reader group {} to stream cut {}.", READER_GROUP, streamSlices.get(0));
        final int parallelSegments = RG_PARALLELISM_ONE + RG_PARALLELISM_TWO;
        final int readEvents = readEventFutures(clientFactory, readerGroup.getGroupName(),
                parallelSegments).stream().map(CompletableFuture::join).reduce((a, b) -> a + b).get();
        assertEquals("Expected read events: ", TOTAL_EVENTS - CUT_SIZE, readEvents);
        log.info("All events correctly read from StreamCut slices on multiple Streams. StreamCuts test passed.");
    }

    @After
    public void tearDown() {
        streamManager.close();
        ExecutorServiceHelpers.shutdown(executor);
    }

    // Start utils region

    /**
     * This method performs slices in streams of non-consecutive StreamCuts. For instance, say that we generate 5 cuts
     * in this order: C1, C2, C3, C4, C5. We then read slices of a stream formed like this:
     *
     * [C1, C3), [C1, C4), [C1, C5)
     * [C2, C4), [C2, C5)
     * [C3, C5)
     *
     * Note that all the consecutive slices have been previously tested, so we avoid them to shorten test execution.
     * Moreover, take into account that a increase in the number of slices greatly lengthen execution time.
     *
     * @param manager Group manager for this scope.
     * @param clientFactory Client factory to instantiate new readers.
     * @param streamSlices StreamCuts lists to be combined and tested via bounded processing.
     */
    private void combineSlicesAndVerify(ReaderGroupManager manager, ClientFactory clientFactory,
                                        List<Map<Stream, StreamCut>> streamSlices) {
        ReaderGroupConfig.ReaderGroupConfigBuilder configBuilder = ReaderGroupConfig.builder()
                                                                                    .stream(Stream.of(SCOPE, STREAM_ONE))
                                                                                    .stream(Stream.of(SCOPE, STREAM_TWO));
        for (int i = 0; i < streamSlices.size()-1; i++) {
            List<Map<Stream, StreamCut>> combinationSlices = new ArrayList<>(streamSlices).subList(i, streamSlices.size());
            configBuilder = configBuilder.startingStreamCuts(combinationSlices.remove(0));

            // Remove the contiguous StreamCut to the starting one, as the slice [CN, CN+1) has been already tested.
            combinationSlices.remove(0);

            // The minimum slice we are going to test is twice the size of CUT_SIZE.
            int readEvents, combinationCutSize = 2;
            final int parallelSegments = RG_PARALLELISM_ONE + RG_PARALLELISM_TWO;
            for (Map<Stream, StreamCut> endingPoint : combinationSlices) {
                configBuilder = configBuilder.endingStreamCuts(endingPoint);

                // Create a new reader group per stream cut slice and read in parallel only events within the cut.
                final String readerGroupId = READER_GROUP + "Comb" + String.valueOf(i) + "Cut" + String.valueOf(combinationCutSize);
                manager.createReaderGroup(readerGroupId, configBuilder.build());
                readEvents = readEventFutures(clientFactory, readerGroupId, parallelSegments).stream()
                                                                                             .map(CompletableFuture::join)
                                                                                             .reduce((a, b) -> a + b).get();
                log.debug("Read events by group {}: {}.", readerGroupId, readEvents);
                assertEquals("Expected events read: ", combinationCutSize * CUT_SIZE, readEvents);
                combinationCutSize++;
            }
        }
    }

    /**
     * Test that all the stream slices represented by consecutive StreamCut pairs can be read correctly.
     *
     * @param manager Group manager for this scope.
     * @param clientFactory Client factory to instantiate new readers.
     * @param streamSlices StreamCuts lists to be combined and tested via bounded processing.
     */
    private void readSliceBySliceAndVerify(ReaderGroupManager manager, ClientFactory clientFactory,
                                            List<Map<Stream, StreamCut>> streamSlices) {
        int readEvents, groupId = 0;
        Map<Stream, StreamCut> startingPoint = null;
        final int parallelSegments = RG_PARALLELISM_ONE + RG_PARALLELISM_TWO;
        ReaderGroupConfig.ReaderGroupConfigBuilder configBuilder = ReaderGroupConfig.builder()
                                                                                    .stream(Stream.of(SCOPE, STREAM_ONE))
                                                                                    .stream(Stream.of(SCOPE, STREAM_TWO));
        for (Map<Stream, StreamCut> endingPoint : streamSlices) {
            configBuilder = configBuilder.endingStreamCuts(endingPoint);
            if (startingPoint != null) {
                configBuilder = configBuilder.startingStreamCuts(startingPoint);
            }

            // Create a new reader group per stream cut slice and read in parallel only events within the cut.
            final String readerGroupId = READER_GROUP + String.valueOf(groupId);
            manager.createReaderGroup(readerGroupId, configBuilder.build());
            readEvents = readEventFutures(clientFactory, readerGroupId, parallelSegments).stream()
                                                                                         .map(CompletableFuture::join)
                                                                                         .reduce((a, b) -> a + b).get();
            log.debug("Read events by group {}: {}.", readerGroupId, readEvents);
            assertEquals("Expected events read: ", CUT_SIZE, readEvents);
            startingPoint = endingPoint;
            groupId++;
        }
    }

    private <T extends Serializable> List<Map<Stream, StreamCut>> getStreamCutSlices(ClientFactory client, ReaderGroup readerGroup) {
        @Cleanup
        EventStreamReader<T> reader = client.createReader("slicer", readerGroup.getGroupName(), new JavaSerializer<>(),
                ReaderConfig.builder().build());
        List<Map<Stream, StreamCut>> streamCuts = new ArrayList<>();
        EventRead<T> event;
        int validEvents = 0;
        try {
            do {
                event = reader.readNextEvent(5000);
                log.debug("Read event result in getStreamCutSlices: {}.", event.getEvent());
                if (event.getEvent() != null) {
                    validEvents++;
                } else {
                    log.warn("Read unexpected null event at {}.", validEvents);
                    continue;
                }

                // Get a StreamCut each defined number of events.
                if (validEvents % CUT_SIZE == 0 && validEvents > 0) {
                    reader.close();
                    log.debug("Adding a StreamCut positioned at event {}.", validEvents);
                    streamCuts.add(readerGroup.getStreamCuts());
                    reader = client.createReader("slicer", readerGroup.getGroupName(), new JavaSerializer<>(),
                            ReaderConfig.builder().build());
                }
            } while (validEvents < TOTAL_EVENTS);
        } catch (ReinitializationRequiredException e) {
            throw new RuntimeException(e);
        } finally {
            reader.close();
        }

        return streamCuts;
    }

    // End utils region
}

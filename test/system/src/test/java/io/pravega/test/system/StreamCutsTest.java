/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.test.system;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
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
import io.pravega.common.Exceptions;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.MarathonException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Slf4j
@RunWith(SystemTestRunner.class)
public class StreamCutsTest extends AbstractReadWriteTest {

    private static final String STREAM_ONE = "testStreamcutsStreamOne";
    private static final int RG_PARALLELISM_ONE = 1;
    private static final String STREAM_TWO = "testStreamcutsStreamTwo";
    private static final int RG_PARALLELISM_TWO = 2;
    private static final String SCOPE = "testStreamcutsStreamScope" + RandomFactory.create().nextInt(Integer.MAX_VALUE);
    private static final String READER_GROUP = "testStreamcutsStreamRG" + RandomFactory.create().nextInt(Integer.MAX_VALUE);

    private static final int TOTAL_EVENTS = 2400;
    private static final int CUT_SIZE = 400;
    private static final int SCALE_WAIT_SECONDS = 30 * 1000;
    private static final int READ_TIMEOUT = 5 * 1000;

    @Rule
    public final Timeout globalTimeout = Timeout.seconds(18 * 60);
    private final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(4, "executor");
    private final ScheduledExecutorService streamCutExecutor = ExecutorServiceHelpers.newScheduledThreadPool(1, "streamCutExecutor");
    private final ScheduledExecutorService chkPointExecutor = ExecutorServiceHelpers.newScheduledThreadPool(1, "chkPointExecutor");
    private final ScheduledExecutorService readerExecutor = ExecutorServiceHelpers.newScheduledThreadPool(4, "readerPool");
    private URI controllerURI = null;
    private StreamManager streamManager = null;
    private Controller controller = null;

    /**
     * This is used to setup the services required by the system test framework.
     *
     * @throws MarathonException When error in setup.
     */
    @Environment
    public static void initialize() throws MarathonException {
        URI zkUri = startZookeeperInstance();
        startBookkeeperInstances(zkUri);
        URI controllerUri = ensureControllerRunning(zkUri);
        ensureSegmentStoreRunning(zkUri, controllerUri);
    }

    @Before
    public void setup() {
        Service conService = Utils.createPravegaControllerService(null);
        List<URI> ctlURIs = conService.getServiceDetails();
        controllerURI = ctlURIs.get(0);

        final ClientConfig clientConfig = Utils.buildClientConfig(controllerURI);

        controller = new ControllerImpl(ControllerImplConfig.builder()
                                                            .clientConfig(clientConfig)
                                                            .maxBackoffMillis(5000).build(), executor);
        streamManager = StreamManager.create(clientConfig);

        assertTrue("Creating scope", streamManager.createScope(SCOPE));
        assertTrue("Creating stream one", streamManager.createStream(SCOPE, STREAM_ONE,
                StreamConfiguration.builder()
                                   .scalingPolicy(ScalingPolicy.fixed(RG_PARALLELISM_ONE)).build()));
        assertTrue("Creating stream two", streamManager.createStream(SCOPE, STREAM_TWO,
                StreamConfiguration.builder()
                                   .scalingPolicy(ScalingPolicy.fixed(RG_PARALLELISM_TWO)).build()));
    }

    @After
    public void tearDown() {
        streamManager.close();
        ExecutorServiceHelpers.shutdown(executor);
    }

    /**
     * This test verifies the correct operation of readers using StreamCuts. Concretely, the test creates two streams
     * with different number of segments and it writes some events (TOTAL_EVENTS / 2) in them. Then, the test creates a
     * list of StreamCuts that encompasses both streams every CUT_SIZE events. The test asserts that new groups of
     * readers can be initialized at these sequential StreamCut intervals and that only CUT_SIZE events are read. Also,
     * the test checks the correctness of different combinations of StreamCuts that have not been sequentially created.
     * After creating StreamCuts and tests the correctness of reads, the test also checks resetting a reader group to a
     * specific initial read point. The previous process is repeated twice: before and after scaling streams, to test if
     * StreamCuts work correctly under scaling events (thus writing TOTAL_EVENTS). Finally, this test checks reading
     * different StreamCut combinations in both streams for all events (encompassing events before and after scaling).
     */
    @Test
    public void streamCutsTest() {
        final ClientConfig clientConfig = Utils.buildClientConfig(controllerURI);

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(SCOPE, clientConfig);
        @Cleanup
        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(SCOPE, clientConfig);
        readerGroupManager.createReaderGroup(READER_GROUP, ReaderGroupConfig.builder()
                                                                            .stream(Stream.of(SCOPE, STREAM_ONE))
                                                                            .stream(Stream.of(SCOPE, STREAM_TWO)).build());
        @Cleanup
        ReaderGroup readerGroup = readerGroupManager.getReaderGroup(READER_GROUP);

        // Perform write of events, slice by slice StreamCuts test and combinations StreamCuts test.
        log.info("Write, slice by slice and combinations test before scaling.");
        final int parallelismBeforeScale = RG_PARALLELISM_ONE + RG_PARALLELISM_TWO;
        List<Map<Stream, StreamCut>> slicesBeforeScale = writeEventsAndCheckSlices(clientFactory, readerGroup, readerGroupManager,
                parallelismBeforeScale);

        // Now, we perform a manual scale on both streams and wait until it occurs.
        CompletableFuture<Boolean> scaleStreamOne = scaleStream(SCOPE, STREAM_ONE, RG_PARALLELISM_ONE * 2, executor);
        checkScaleStatus(scaleStreamOne);

        // Perform again the same test on the stream segments after scaling.
        final int parallelSegmentsAfterScale = RG_PARALLELISM_ONE * 2 + RG_PARALLELISM_TWO;
        final String newReaderGroupName = READER_GROUP + "new";
        final Map<Stream, StreamCut> streamCutBeforeScale = slicesBeforeScale.get(slicesBeforeScale.size() - 1);
        readerGroupManager.createReaderGroup(newReaderGroupName, ReaderGroupConfig.builder()
                                                                                  .stream(Stream.of(SCOPE, STREAM_ONE))
                                                                                  .stream(Stream.of(SCOPE, STREAM_TWO))
                                                                                  .startingStreamCuts(streamCutBeforeScale).build());
        @Cleanup
        ReaderGroup newReaderGroup = readerGroupManager.getReaderGroup(newReaderGroupName);
        log.info("Checking slices again starting from {}.", streamCutBeforeScale);
        List<Map<Stream, StreamCut>> slicesAfterScale = writeEventsAndCheckSlices(clientFactory, newReaderGroup, readerGroupManager,
                parallelSegmentsAfterScale);

        // Perform combinations including StreamCuts before and after the scale event.
        slicesAfterScale.remove(0);
        slicesBeforeScale.addAll(slicesAfterScale);
        log.info("Performing combinations in the whole stream.");
        combineSlicesAndVerify(readerGroupManager, clientFactory, parallelSegmentsAfterScale, slicesBeforeScale);
        log.info("All events correctly read from StreamCut slices on multiple Streams. StreamCuts test passed.");
    }

    // Start utils region

    private List<Map<Stream, StreamCut>> writeEventsAndCheckSlices(EventStreamClientFactory clientFactory, ReaderGroup readerGroup,
                                                                   ReaderGroupManager readerGroupManager, int parallelSegments) {
        // First, write half of total events before scaling (1/4 in each Stream).
        writeEvents(clientFactory, STREAM_ONE, TOTAL_EVENTS / 4);
        writeEvents(clientFactory, STREAM_TWO, TOTAL_EVENTS / 4);
        log.info("Finished writing events to streams.");

        Map<Stream, StreamCut> initialPosition = new HashMap<>(readerGroup.getStreamCuts());
        log.info("Creating StreamCuts from: {}.", initialPosition);

        // Get StreamCuts for each slice from both Streams at the same time (may be different in each execution).
        List<Map<Stream, StreamCut>> streamSlices = getStreamCutSlices(clientFactory, readerGroup, TOTAL_EVENTS / 2);
        streamSlices.add(0, initialPosition);
        log.info("Finished creating StreamCuts {}.", streamSlices);

        // Ensure that reader groups can correctly read slice by slice from different Streams.
        readSliceBySliceAndVerify(readerGroupManager, clientFactory, parallelSegments, streamSlices);
            log.info("Finished checking sequentially slice by slice.");

        // Perform different combinations of StreamCuts and verify that read event boundaries are still correct.
        combineSlicesAndVerify(readerGroupManager, clientFactory, parallelSegments, streamSlices);
        log.info("Finished checking StreamCut combinations.");

        // Test that a reader group can be reset correctly.
        ReaderGroupConfig firstSliceConfig = ReaderGroupConfig.builder()
                                                              .stream(Stream.of(SCOPE, STREAM_ONE))
                                                              .stream(Stream.of(SCOPE, STREAM_TWO))
                                                              .startingStreamCuts(initialPosition)
                                                              .endingStreamCuts(streamSlices.get(streamSlices.size() - 1)).build();
        readerGroup.resetReaderGroup(firstSliceConfig);
        log.info("Resetting existing reader group {} to stream cut {}.", READER_GROUP, initialPosition);
        final int readEvents = readAllEvents(readerGroupManager, clientFactory, readerGroup.getGroupName(), parallelSegments
        ).stream().map(CompletableFuture::join).reduce(Integer::sum).get();
        assertEquals("Expected read events: ", TOTAL_EVENTS / 2, readEvents);
        return streamSlices;
    }

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
     * @param parallelSegments Number of parallel segments that indicates the number of parallel readers to instantiate.
     * @param streamSlices StreamCuts lists to be combined and tested via bounded processing.
     */
    private void combineSlicesAndVerify(ReaderGroupManager manager, EventStreamClientFactory clientFactory, int parallelSegments,
                                        List<Map<Stream, StreamCut>> streamSlices) {

        for (int i = 0; i < streamSlices.size() - 1; i++) {
            List<Map<Stream, StreamCut>> combinationSlices = new ArrayList<>(streamSlices).subList(i, streamSlices.size());
            ReaderGroupConfig.ReaderGroupConfigBuilder configBuilder = ReaderGroupConfig.builder()
                                                                                        .stream(Stream.of(SCOPE, STREAM_ONE))
                                                                                        .stream(Stream.of(SCOPE, STREAM_TWO))
                                                                                        .startingStreamCuts(combinationSlices.remove(0));

            // Remove the contiguous StreamCut to the starting one, as the slice [CN, CN+1) has been already tested.
            combinationSlices.remove(0);

            // The minimum slice we are going to test is twice the size of CUT_SIZE.
            int readEvents, combinationCutSize = 2;
            for (Map<Stream, StreamCut> endingPoint : combinationSlices) {
                configBuilder = configBuilder.endingStreamCuts(endingPoint);

                // Create a new reader group per stream cut slice and read in parallel only events within the cut.
                final String readerGroupId = READER_GROUP + "CombSize" + String.valueOf(combinationCutSize) + "-" + System.nanoTime();
                manager.createReaderGroup(readerGroupId, configBuilder.build());
                log.debug("Reading events between starting StreamCut {} and ending StreamCut {}",
                          configBuilder.build().getStartingStreamCuts(), endingPoint);
                readEvents = readAllEvents(manager, clientFactory, readerGroupId, parallelSegments).stream()
                                                                                                   .map(CompletableFuture::join)
                                                                                                   .reduce(Integer::sum).get();
                log.debug("Read events by group {}: {}.", readerGroupId, readEvents);
                assertEquals("Expected events read: ", combinationCutSize * CUT_SIZE, readEvents);
                combinationCutSize++;
            }
        }
    }

    /**
     * Check that all the stream slices represented by consecutive StreamCut pairs can be read correctly.
     *
     * @param manager Group manager for this scope.
     * @param clientFactory Client factory to instantiate new readers.
     * @param streamSlices StreamCuts lists to be combined and tested via bounded processing.
     */
    private void readSliceBySliceAndVerify(ReaderGroupManager manager, EventStreamClientFactory clientFactory, int parallelSegments,
                                           List<Map<Stream, StreamCut>> streamSlices) {
        int readEvents;
        for (int i = 1; i < streamSlices.size(); i++) {
            log.debug("Reading events between starting StreamCut {} and ending StreamCut {}", streamSlices.get(i-1), streamSlices.get(i));
            ReaderGroupConfig configBuilder = ReaderGroupConfig.builder().stream(Stream.of(SCOPE, STREAM_ONE))
                                                                         .stream(Stream.of(SCOPE, STREAM_TWO))
                                                                         .startingStreamCuts(streamSlices.get(i - 1))
                                                                         .endingStreamCuts(streamSlices.get(i)).build();

            // Create a new reader group per stream cut slice and read in parallel only events within the cut.
            final String readerGroupId = READER_GROUP + String.valueOf(i) + "-" + System.nanoTime();
            manager.createReaderGroup(readerGroupId, configBuilder);
            readEvents = readAllEvents(manager, clientFactory, readerGroupId, parallelSegments).stream()
                                                                                               .map(CompletableFuture::join)
                                                                                               .reduce(Integer::sum).get();
            log.debug("Read events by group {}: {}.", readerGroupId, readEvents);
            assertEquals("Expected events read: ", CUT_SIZE, readEvents);
        }
    }

    private <T extends Serializable> List<Map<Stream, StreamCut>> getStreamCutSlices(EventStreamClientFactory client, ReaderGroup readerGroup,
                                                                                     int totalEvents) {
        final AtomicReference<EventStreamReader<T>> reader = new AtomicReference<>();
        final AtomicReference<CompletableFuture<Map<Stream, StreamCut>>> streamCutFuture = new AtomicReference<>(CompletableFuture.completedFuture(null));
        reader.set(client.createReader("slicer", readerGroup.getGroupName(), new JavaSerializer<>(),
                                       ReaderConfig.builder().build()));
        final List<CompletableFuture<Map<Stream, StreamCut>>> streamCutFutureList = new ArrayList<>();
        final AtomicInteger validEvents = new AtomicInteger();

        Futures.loop(
                () -> validEvents.get() < totalEvents,
                () -> CompletableFuture.runAsync(() -> {
                    try {
                        EventRead<T> event = reader.get().readNextEvent(READ_TIMEOUT);
                        if (event.getEvent() != null) {
                            log.info("Await and verify if the last StreamCut generation completed successfully {}",
                                     Futures.await(streamCutFuture.get(), 10_000));
                            assertTrue("StreamCut generation did not complete", Futures.await(streamCutFuture.get(), 10_000));
                            validEvents.incrementAndGet();
                            log.debug("Read event result in getStreamCutSlices: {}. Valid events: {}.", event.getEvent(), validEvents);

                            // Get a StreamCut each defined number of events.
                            if (validEvents.get() % CUT_SIZE == 0 && validEvents.get() > 0) {
                                CompletableFuture<Map<Stream, StreamCut>> streamCutsFuture =
                                        readerGroup.generateStreamCuts(streamCutExecutor);
                                streamCutFuture.set(streamCutsFuture);
                                // wait for 5 seconds to force reader group state update, so that we can ensure StreamCut for every
                                // CUT_SIZE number of events.
                                Exceptions.handleInterrupted(() -> TimeUnit.SECONDS.sleep(5));
                                log.info("Adding a StreamCut positioned at event {}", validEvents);
                                streamCutFutureList.add(streamCutsFuture);
                            }
                        } else {
                            log.warn("Read unexpected null event at {}.", validEvents);
                        }
                    } catch (ReinitializationRequiredException e) {
                        log.warn("Reinitialization of readers required.", e);
                    }
                }, executor),
                executor).join();
        reader.get().close();

        // Now return all the streamCuts generated.
        return Futures.getAndHandleExceptions(Futures.allOfWithResults(streamCutFutureList), t -> {
            log.error("StreamCut generation did not complete", t);
            throw new AssertionError("StreamCut generation did not complete", t);
        });
    }

    private CompletableFuture<Boolean> scaleStream(String scope, String stream, double splitSize, ScheduledExecutorService executorService) {
        Map<Double, Double> keyRanges = new HashMap<>();
        double perSegmentKeySpace = 1.0 / splitSize;
        for (double keySpaceEnd = perSegmentKeySpace; keySpaceEnd <= 1.0; keySpaceEnd += perSegmentKeySpace) {
            log.debug("Keyspace {} - {} for stream {}.", keySpaceEnd - perSegmentKeySpace, keySpaceEnd, stream);
            keyRanges.put(keySpaceEnd - perSegmentKeySpace, keySpaceEnd);
        }

        return controller.scaleStream(Stream.of(scope, stream), Collections.singletonList(0L), keyRanges, executorService).getFuture();
    }

    private void checkScaleStatus(CompletableFuture<Boolean> scaleStatus) {
        Futures.exceptionListener(scaleStatus, t -> log.error("Scale Operation completed with an error", t));
        if (Futures.await(scaleStatus, SCALE_WAIT_SECONDS)) {
            log.info("Scale operation has completed: {}", scaleStatus.join());
            if (!scaleStatus.join()) {
                log.error("Scale operation did not complete {}", scaleStatus.join());
                Assert.fail("Scale operation did not complete successfully");
            }
        } else {
            Assert.fail("Scale operation threw an exception");
        }
    }

    private List<CompletableFuture<Integer>> readAllEvents(ReaderGroupManager rgMgr, EventStreamClientFactory clientFactory, String rGroupId,
                                                           int readerCount) {
        return IntStream.range(0, readerCount)
                        .mapToObj(i -> CompletableFuture.supplyAsync(() -> createReaderAndReadEvents(rgMgr, clientFactory, rGroupId, i),
                                                                        readerExecutor))
                        .collect(Collectors.toList());

    }

    private <T extends Serializable> int createReaderAndReadEvents(ReaderGroupManager rgMgr, EventStreamClientFactory clientFactory,
                                                                   String rGroupId, int readerIndex) {
        // create a reader.
        EventStreamReader<T> reader = clientFactory.createReader(rGroupId + "-" + readerIndex, rGroupId, new JavaSerializer<>(),
                                                                 ReaderConfig.builder().build());
        EventRead<T> event = null;
        int validEvents = 0;
        AtomicBoolean sealedSegmentUpdated = new AtomicBoolean(false);
        try {
            do {
                try {
                    event = reader.readNextEvent(READ_TIMEOUT);
                    log.debug("Read event result in readEvents: {}.", event.getEvent());
                    if (event.getEvent() == null && !event.isCheckpoint() && !sealedSegmentUpdated.get()) {
                        // initiate a checkpoint to ensure all sealed segments are acquired by the reader.
                        ReaderGroup readerGroup = rgMgr.getReaderGroup(rGroupId);
                        readerGroup.initiateCheckpoint("chkPoint", chkPointExecutor)
                                   .whenComplete((checkpoint, t) -> {
                                       if (t != null) {
                                           log.error("Checkpoint operation failed", t);
                                       } else {
                                           log.info("Checkpoint {} completed", checkpoint);
                                           sealedSegmentUpdated.set(true);
                                       }
                                   });
                    }
                    if (event.getEvent() != null) {
                        validEvents++;
                    }
                } catch (ReinitializationRequiredException e) {
                    log.error("Reinitialization Exception while reading event using readerId: {}", reader, e);
                    fail("Reinitialization Exception is not expected");
                }
            } while (event.getEvent() != null || event.isCheckpoint() || !sealedSegmentUpdated.get());
        } finally {
            closeReader(reader);
        }
        return validEvents;
    }

    // End utils region
}

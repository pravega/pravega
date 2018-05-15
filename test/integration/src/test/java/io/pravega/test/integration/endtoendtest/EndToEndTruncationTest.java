/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.endtoendtest;

import com.google.common.collect.Lists;
import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.segment.impl.Segment;
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
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.TruncatedDataException;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.controller.server.eventProcessor.LocalController;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static java.util.stream.Collectors.toList;
import static io.pravega.test.common.AssertExtensions.assertThrows;

@Slf4j
public class EndToEndTruncationTest {

    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final String serviceHost = "localhost";
    private final URI controllerURI = URI.create("tcp://" + serviceHost + ":" + controllerPort);
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int containerCount = 4;
    private TestingServer zkTestServer;
    private PravegaConnectionListener server;
    private ControllerWrapper controllerWrapper;
    private ServiceBuilder serviceBuilder;
    private ScheduledExecutorService executor;

    @Before
    public void setUp() throws Exception {
        executor = Executors.newSingleThreadScheduledExecutor();
        zkTestServer = new TestingServerStarter().start();

        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();

        server = new PravegaConnectionListener(false, servicePort, store);
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
    public void testTruncation() throws Exception {
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scope("test")
                                                        .streamName("test")
                                                        .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 2))
                                                        .build();
        LocalController controller = (LocalController) controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope("test").get();
        controller.createStream(config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder()
                                                                                    .controllerURI(URI.create("tcp://" + serviceHost))
                                                                                    .build());
        @Cleanup
        ClientFactory clientFactory = new ClientFactoryImpl("test", controller, connectionFactory);
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter("test", new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        writer.writeEvent("0", "truncationTest1").get();

        // scale
        Stream stream = new StreamImpl("test", "test");
        Map<Double, Double> map = new HashMap<>();
        map.put(0.0, 0.33);
        map.put(0.33, 0.66);
        map.put(0.66, 1.0);
        Boolean result = controller.scaleStream(stream, Lists.newArrayList(0L, 1L), map, executor).getFuture().get();

        assertTrue(result);
        writer.writeEvent("0", "truncationTest2").get();

        Map<Long, Long> streamCutPositions = new HashMap<>();
        streamCutPositions.put(2L, 0L);
        streamCutPositions.put(3L, 0L);
        streamCutPositions.put(4L, 0L);

        controller.truncateStream(stream.getStreamName(), stream.getStreamName(), streamCutPositions).join();

        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl("test", controller, clientFactory,
                connectionFactory);
        groupManager.createReaderGroup("reader", ReaderGroupConfig.builder().disableAutomaticCheckpoints()
                                                                  .stream("test/test").build());

        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader("readerId", "reader", new JavaSerializer<>(),
                ReaderConfig.builder().build());

        EventRead<String> event = reader.readNextEvent(10000);
        assertNotNull(event);
        assertEquals("truncationTest2", event.getEvent());
        event = reader.readNextEvent(1000);
        assertNull(event.getEvent());
    }

    /**
     * This test checks the basic operation of truncation with offsets. The test first writes two events on a Stream
     * (1 segment) and then truncates the Stream after the first event. We verify that a new reader first gets a
     * TruncatedDataException and then it reads only the second event written, as the first has been truncated.
     *
     * @throws ReinitializationRequiredException If a checkpoint or reset is performed on the reader group.
     */
    @Test(timeout = 30000)
    public void testSimpleOffsetTruncation() throws ReinitializationRequiredException {
        final String scope = "truncationTests";
        final String streamName = "testSimpleOffsetTruncation";
        final String readerGroupName = "RGTestSimpleOffsetTruncation";

        StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                                                                     .scalingPolicy(ScalingPolicy.fixed(1)).build();
        StreamManager streamManager = StreamManager.create(controllerURI);
        streamManager.createScope(scope);
        streamManager.createStream(scope, streamName, streamConfiguration);
        @Cleanup
        ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope(scope, controllerURI);
        groupManager.createReaderGroup(readerGroupName, ReaderGroupConfig.builder().disableAutomaticCheckpoints()
                                                                         .stream(scope + "/" + streamName)
                                                                         .build());
        ReaderGroup readerGroup = groupManager.getReaderGroup(readerGroupName);

        // Write two events to the Stream.
        writeDummyEvents(clientFactory, streamName, 2);

        // Read only the first one.
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader(readerGroupName + "1", readerGroupName,
                new JavaSerializer<>(), ReaderConfig.builder().build());
        assertEquals(reader.readNextEvent(1000).getEvent(), "0");
        reader.close();

        // Create a Checkpoint, get StreamCut and truncate the Stream at that point.
        Checkpoint cp = readerGroup.initiateCheckpoint("myCheckpoint", executor).join();
        StreamCut streamCut = cp.asImpl().getPositions().values().iterator().next();
        assertTrue(streamManager.truncateStream(scope, streamName, streamCut));

        // Verify that a new reader reads from event 1 onwards.
        final String newReaderGroupName = readerGroupName + "new";
        groupManager.createReaderGroup(newReaderGroupName, ReaderGroupConfig.builder().stream(Stream.of(scope, streamName)).build());
        @Cleanup
        final EventStreamReader<String> newReader = clientFactory.createReader(newReaderGroupName + "2",
                newReaderGroupName, new JavaSerializer<>(), ReaderConfig.builder().build());

        // Check that we get a TruncatedDataException after truncating the active segment and then read the expected event.
        assertThrows(TruncatedDataException.class, () -> newReader.readNextEvent(1000).getEvent());
        assertEquals("Expected read event: ", "1", newReader.readNextEvent(1000).getEvent());
        assertNull(newReader.readNextEvent(1000).getEvent());
    }

    /**
     * This test verifies that truncation works specifying an offset that applies to multiple segments. To this end,
     * the test first writes a set of events on a Stream (with multiple segments) and truncates it at a specified offset
     * (truncatedEvents). The tests asserts that readers gets a TruncatedDataException after truncation and then it
     * (only) reads the remaining events that have not been truncated.
     */
    @Test(timeout = 30000)
    public void testParallelSegmentOffsetTruncation() {
        final String scope = "truncationTests";
        final String streamName = "testParallelSegmentOffsetTruncation";
        final String readerGroupName = "RGTestParallelSegmentOffsetTruncation";
        final int parallelism = 2;
        final int totalEvents = 200;
        final int truncatedEvents = 50;

        StreamConfiguration streamConf = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(parallelism)).build();
        StreamManager streamManager = StreamManager.create(controllerURI);
        streamManager.createScope(scope);
        streamManager.createStream(scope, streamName, streamConf);
        @Cleanup
        ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);
        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope(scope, controllerURI);
        groupManager.createReaderGroup(readerGroupName, ReaderGroupConfig.builder().disableAutomaticCheckpoints()
                                                                .stream(scope + "/" + streamName).build());
        ReaderGroup readerGroup = groupManager.getReaderGroup(readerGroupName);

        // Write events to the Stream.
        writeDummyEvents(clientFactory, streamName, totalEvents);

        // Instantiate readers to consume from Stream up to truncatedEvents.
        List<CompletableFuture<Integer>> futures = readDummyEvents(clientFactory, readerGroupName, parallelism, truncatedEvents);
        Futures.allOf(futures).join();

        // Perform truncation on stream segment
        Checkpoint cp = readerGroup.initiateCheckpoint("myCheckpoint", executor).join();
        StreamCut streamCut = cp.asImpl().getPositions().values().iterator().next();
        assertTrue(streamManager.truncateStream(scope, streamName, streamCut));

        // Just after the truncation, trying to read the whole stream should raise a TruncatedDataException.
        final String newGroupName = readerGroupName + "new";
        groupManager.createReaderGroup(newGroupName, ReaderGroupConfig.builder().stream(Stream.of(scope, streamName)).build());
        assertThrows(TruncatedDataException.class, () -> Futures.allOf(readDummyEvents(clientFactory, newGroupName, parallelism)).join());

        // Read again, now expecting to read from the offset defined in truncate call onwards.
        groupManager.createReaderGroup(newGroupName, ReaderGroupConfig.builder().stream(Stream.of(scope, streamName)).build());
        futures = readDummyEvents(clientFactory, newGroupName, parallelism);
        Futures.allOf(futures).join();
        assertEquals("Expected read events: ", totalEvents - (truncatedEvents * parallelism),
                (int) futures.stream().map(CompletableFuture::join).reduce((a, b) -> a + b).get());
    }

    /**
     * This test checks the behavior of a reader (or group of readers) based on whether segment truncation takes place
     * while reading (first part of the test) or before starting reading (second part).
     *
     * @throws InterruptedException If the current thread is interrupted while waiting for the Controller service.
     */
    @Test(timeout = 30000)
    public void testSegmentTruncationWhileReading() throws InterruptedException {
        final int totalEvents = 100;
        final int parallelism = 1;
        final String scope = "truncationTests";
        final String streamName = "testSegmentTruncationWhileReading";
        final String readerGroupName = "RGTestSegmentTruncationWhileReading";

        StreamConfiguration config = StreamConfiguration.builder().scope(scope).streamName(streamName)
                                                        .scalingPolicy(ScalingPolicy.byEventRate(10, 2, parallelism))
                                                        .build();
        LocalController controller = (LocalController) controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope(scope).join();
        controller.createStream(config).join();
        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().controllerURI(controllerURI)
                                                                                    .build());
        @Cleanup
        ClientFactory clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory);

        // Write half of totalEvents to the Stream.
        writeDummyEvents(clientFactory, streamName, totalEvents / 2);

        // Seal current segment (0) and split it into two segments (1,2).
        Stream stream = new StreamImpl(scope, streamName);
        Map<Double, Double> map = new HashMap<>();
        map.put(0.0, 0.5);
        map.put(0.5, 1.0);
        assertTrue(controller.scaleStream(stream, Lists.newArrayList(0L), map, executor).getFuture().join());

        long one = StreamSegmentNameUtils.computeSegmentId(1, 1);
        long two = StreamSegmentNameUtils.computeSegmentId(2, 1);
        // Write rest of events to the new Stream segments.
        writeDummyEvents(clientFactory, streamName, totalEvents, totalEvents / 2);

        // Instantiate readers to consume from Stream.
        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl(scope, controller, clientFactory, connectionFactory);
        groupManager.createReaderGroup(readerGroupName, ReaderGroupConfig.builder().stream(Stream.of(scope, streamName)).build());
        List<CompletableFuture<Integer>> futures = readDummyEvents(clientFactory, readerGroupName, parallelism);

        // Let readers to consume some events and truncate segment while readers are consuming events
        Exceptions.handleInterrupted(() -> Thread.sleep(500));
        Map<Long, Long> streamCutPositions = new HashMap<>();
        streamCutPositions.put(one, 0L);
        streamCutPositions.put(two, 0L);
        assertTrue(controller.truncateStream(scope, streamName, streamCutPositions).join());

        // Wait for readers to complete and assert that they have read all the events (totalEvents).
        Futures.allOf(futures).join();
        assertEquals((int) futures.stream().map(CompletableFuture::join).reduce((a, b) -> a + b).get(), totalEvents);

        // Assert that from the truncation call onwards, the available segments are the ones after scaling.
        List<Long> currentSegments = controller.getCurrentSegments(scope, streamName).join().getSegments().stream()
                                                  .map(Segment::getSegmentId)
                                                  .sorted()
                                                  .collect(toList());
        currentSegments.removeAll(Lists.newArrayList(one, two));
        assertTrue(currentSegments.isEmpty());

        // The new set of readers, should only read the events beyond truncation point (segments 1 and 2).
        final String newReaderGroupName = readerGroupName + "new";
        groupManager.createReaderGroup(newReaderGroupName, ReaderGroupConfig.builder().stream(Stream.of(scope, streamName)).build());
        futures = readDummyEvents(clientFactory, newReaderGroupName, parallelism);
        Futures.allOf(futures).join();
        assertEquals((int) futures.stream().map(CompletableFuture::join).reduce((a, b) -> a + b).get(), totalEvents / 2);
    }

    /**
     * This test checks the behavior of a reader (or group of readers) that gets a delete event while reading. Note that
     * the timeout is larger than in other tests due to the behavior of the system in this situation. That is, while the
     * client is reading events (Segment Store) the test deletes the Stream (Controller and metadata). Once the client
     * reads all the events and reaches the end of segment, it contacts the Controller to retrieve subsequent segments
     * (if any). However, the Stream-related metadata to answer this request has been previously deleted. The current
     * behavior is that the Controller keeps looking for the Stream metadata for a non-negligible period of time (+100
     * sec.) while the client waits for a response. After this period, a RetriesExhaustedException is thrown to the
     * client, which is expected in this situation.
     */
    @Test(timeout = 200000)
    public void testDeleteStreamWhileReading() {
        final String scope = "truncationTests";
        final String streamName = "testDeleteStreamWhileReading";
        final String readerGroup = "RGTestDeleteStreamWhileReading";
        final int totalEvents = 100;
        final int parallelism = 1;

        StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                                                                     .scalingPolicy(ScalingPolicy.fixed(parallelism))
                                                                     .build();
        StreamManager streamManager = StreamManager.create(controllerURI);
        streamManager.createScope(scope);
        streamManager.createStream(scope, streamName, streamConfiguration);
        @Cleanup
        ClientFactory clientFactory = ClientFactory.withScope(scope, controllerURI);

        // Write totalEvents to the Stream.
        writeDummyEvents(clientFactory, streamName, totalEvents);

        // Instantiate readers to consume from Stream.
        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope(scope, controllerURI);
        groupManager.createReaderGroup(readerGroup, ReaderGroupConfig.builder().stream(Stream.of(scope, streamName)).build());
        final List<CompletableFuture<Integer>> futures = readDummyEvents(clientFactory, readerGroup, parallelism);

        // Wait some time to let readers read and then execute deletion.
        Exceptions.handleInterrupted(() -> Thread.sleep(500));
        assertTrue(streamManager.sealStream(scope, streamName));
        assertTrue(streamManager.deleteStream(scope, streamName));

        // At the control plane, we expect a RetriesExhaustedException as readers try to get successor segments from a deleted stream.
        assertThrows(RetriesExhaustedException.class, () -> Futures.allOf(futures).join());
        assertTrue(!streamManager.deleteStream(scope, streamName));
    }

    // start region utils

    private List<CompletableFuture<Integer>> readDummyEvents(ClientFactory client, String rGroup, int numReaders, int limit) {
        List<EventStreamReader<String>> readers = new ArrayList<>();
        for (int i = 0; i < numReaders; i++) {
            readers.add(client.createReader(String.valueOf(i), rGroup, new JavaSerializer<>(), ReaderConfig.builder().build()));
        }

        return readers.stream().map(r -> CompletableFuture.supplyAsync(() -> readEvents(r, limit))).collect(toList());
    }

    private List<CompletableFuture<Integer>> readDummyEvents(ClientFactory clientFactory, String readerGroup, int numReaders) {
        return readDummyEvents(clientFactory, readerGroup, numReaders, Integer.MAX_VALUE);
    }

    @SneakyThrows
    private <T> int readEvents(EventStreamReader<T> reader, int limit) {
        final int timeout = 1000;
        final int interReadWait = 50;
        EventRead<T> event;
        int validEvents = 0;
        try {
            do {
                event = reader.readNextEvent(timeout);
                Exceptions.handleInterrupted(() -> Thread.sleep(interReadWait));
                if (event.getEvent() != null) {
                    validEvents++;
                }
            } while ((event.getEvent() != null || event.isCheckpoint()) && validEvents < limit);

            reader.close();
        } catch (TruncatedDataException e) {
            reader.close();
            throw new TruncatedDataException(e.getCause());
        } catch (RuntimeException e) {
            if (e.getCause() instanceof RetriesExhaustedException) {
                throw new RetriesExhaustedException(e.getCause());
            } else {
                throw e;
            }
        }

        return validEvents;
    }

    private void writeDummyEvents(ClientFactory clientFactory, String streamName, int totalEvents, int offset) {
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        for (int i = offset; i < totalEvents; i++) {
            writer.writeEvent(String.valueOf(i)).join();
            log.info("Writing event: {} to stream {}", i, streamName);
        }
    }

    private void writeDummyEvents(ClientFactory clientFactory, String streamName, int totalEvents) {
        writeDummyEvents(clientFactory, streamName, totalEvents, 0);
    }

    // End region utils
}

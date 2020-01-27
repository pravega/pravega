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

import static io.pravega.shared.NameUtils.computeSegmentId;
import static io.pravega.test.common.AssertExtensions.assertFutureThrows;
import static io.pravega.test.common.AssertExtensions.assertThrows;
import static io.pravega.test.integration.ReadWriteUtils.readEvents;
import static io.pravega.test.integration.ReadWriteUtils.writeEvents;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.pravega.client.security.auth.DelegationTokenProviderFactory;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.segment.impl.NoSuchSegmentException;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentMetadataClient;
import io.pravega.client.segment.impl.SegmentMetadataClientFactory;
import io.pravega.client.segment.impl.SegmentMetadataClientFactoryImpl;
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.InvalidStreamException;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.TruncatedDataException;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.client.stream.mock.MockClientFactory;
import io.pravega.client.stream.mock.MockController;
import io.pravega.client.stream.mock.MockStreamManager;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.server.eventProcessor.LocalController;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.ReadWriteUtils;
import io.pravega.test.integration.demo.ControllerWrapper;
import lombok.Cleanup;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EndToEndTruncationTest {

    private final String serviceHost = "localhost";
    private final int containerCount = 4;
    private int controllerPort;
    private URI controllerURI; 
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
        TableStore tableStore = serviceBuilder.createTableStoreService();
        controllerPort = TestUtils.getAvailableListenPort();
        controllerURI = URI.create("tcp://" + serviceHost + ":" + controllerPort);
        int servicePort = TestUtils.getAvailableListenPort();
        server = new PravegaConnectionListener(false, servicePort, store, tableStore);
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
    
    @Test(timeout = 7000)
    public void testTruncationOffsets() throws InterruptedException, ExecutionException, TimeoutException,
                                        TruncatedDataException, ReinitializationRequiredException {
        String endpoint = "localhost";
        String scope = "scope";
        String streamName = "abc";
        int port = TestUtils.getAvailableListenPort();
        String testString = "Hello world\n";
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, tableStore);
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(scope, endpoint, port);
        @Cleanup
        MockClientFactory clientFactory = streamManager.getClientFactory();
        streamManager.createScope(scope);
        streamManager.createStream(scope, streamName, null);
        Serializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<String> producer = clientFactory.createEventWriter(streamName, serializer,
                                                                             EventWriterConfig.builder().build());
        Future<Void> ack = producer.writeEvent(testString);
        ack.get(5, TimeUnit.SECONDS);

        MockController controller = new MockController(endpoint, port, streamManager.getConnectionFactory(), true);
        SegmentMetadataClientFactory metadataClientFactory = new SegmentMetadataClientFactoryImpl(controller,
                                                                                                  streamManager.getConnectionFactory());
        Segment segment = new Segment(scope, streamName, 0);
        SegmentMetadataClient metadataClient = metadataClientFactory.createSegmentMetadataClient(segment,
                DelegationTokenProviderFactory.createWithEmptyToken());
        assertEquals(0, metadataClient.getSegmentInfo().getStartingOffset());
        long writeOffset = metadataClient.getSegmentInfo().getWriteOffset();
        assertEquals(writeOffset, metadataClient.fetchCurrentSegmentLength());
        assertTrue(metadataClient.getSegmentInfo().getWriteOffset() > testString.length());
        metadataClient.truncateSegment(writeOffset);
        assertEquals(writeOffset, metadataClient.getSegmentInfo().getStartingOffset());
        assertEquals(writeOffset, metadataClient.getSegmentInfo().getWriteOffset());
        assertEquals(writeOffset, metadataClient.fetchCurrentSegmentLength());

        ack = producer.writeEvent(testString);
        ack.get(5, TimeUnit.SECONDS);

        ReaderGroupConfig groupConfig = ReaderGroupConfig.builder()
                                                         .disableAutomaticCheckpoints()
                                                         .stream(new StreamImpl(scope, streamName))
                                                         .build();
        streamManager.createReaderGroup("ReaderGroup", groupConfig);
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader("reader", "ReaderGroup", serializer,
                                                                      ReaderConfig.builder().build());
        AssertExtensions.assertThrows(TruncatedDataException.class, () -> reader.readNextEvent(2000));
        EventRead<String> event = reader.readNextEvent(2000);
        assertEquals(testString, event.getEvent());
        event = reader.readNextEvent(100);
        assertEquals(null, event.getEvent());
    }

    @Test(timeout = 30000)
    public void testTruncation() throws Exception {
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 2))
                                                        .build();
        LocalController controller = (LocalController) controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope("test").get();
        controller.createStream("test", "test", config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder()
                                                                                    .controllerURI(URI.create("tcp://" + serviceHost))
                                                                                    .build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl("test", controller, connectionFactory);
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
        streamCutPositions.put(computeSegmentId(2, 1), 0L);
        streamCutPositions.put(computeSegmentId(3, 1), 0L);
        streamCutPositions.put(computeSegmentId(4, 1), 0L);

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

    @Test(timeout = 30000)
    public void testWriteDuringTruncationAndDeletion() throws Exception {
        StreamConfiguration config = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 2))
                .build();
        LocalController controller = (LocalController) controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope("test").get();
        controller.createStream("test", "test", config).get();
        
        config = StreamConfiguration.builder()
                                    .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 1))
                                    .build();
        controller.updateStream("test", "test", config).get();

        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder()
                .controllerURI(URI.create("tcp://" + serviceHost))
                .build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl("test", controller, connectionFactory);
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter("test", new JavaSerializer<>(),
                EventWriterConfig.builder().build());

        // routing key "0" translates to key 0.8. This write happens to segment 1.
        writer.writeEvent("0", "truncationTest1").get();

        // scale down to one segment.
        Stream stream = new StreamImpl("test", "test");
        Map<Double, Double> map = new HashMap<>();
        map.put(0.0, 1.0);
        assertTrue("Stream Scale down", controller.scaleStream(stream, Lists.newArrayList(0L, 1L), map, executor).getFuture().get());

        // truncate stream at segment 2, offset 0.
        Map<Long, Long> streamCutPositions = new HashMap<>();
        streamCutPositions.put(computeSegmentId(2, 1), 0L);
        assertTrue("Truncate stream", controller.truncateStream("test", "test", streamCutPositions).get());

        // routing key "2" translates to key 0.2.
        // this write translates to a write to Segment 0, but since segment 0 is truncated the write should happen on segment 2.
        // write to segment 0
        writer.writeEvent("2", "truncationTest2").get();

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

        //Seal and Delete stream.
        assertTrue(controller.sealStream("test", "test").get());
        assertTrue(controller.deleteStream("test", "test").get());

        //write by an existing writer to a deleted stream should complete exceptionally.
        assertFutureThrows("Should throw NoSuchSegmentException",
                writer.writeEvent("2", "write to deleted stream"),
                e -> NoSuchSegmentException.class.isAssignableFrom(e.getClass()));
        
        //subsequent writes will throw an exception to the application.
        assertThrows(RuntimeException.class, () -> writer.writeEvent("test"));
    }

    @Test(timeout = 50000)
    public void testWriteDuringScaleAndTruncation() throws Exception {
        Stream stream = new StreamImpl("test", "test");
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 2))
                                                        .build();
        LocalController controller = (LocalController) controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope("test").get();
        controller.createStream("test", "test", config).get();
        config = StreamConfiguration.builder()
                                    .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 1))
                                    .build();
        controller.updateStream("test", "test", config).get();

        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder()
                                                                                    .controllerURI(URI.create("tcp://" + serviceHost))
                                                                                    .build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl("test", controller, connectionFactory);
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter("test", new JavaSerializer<>(),
                EventWriterConfig.builder().build());

        // routing key "0" translates to key 0.8. This write happens to segment 1.
        writer.writeEvent("0", "truncationTest1").get();

        //Peform scaling operations on the stream.
        ImmutableMap<Double, Double> singleSegmentKeyRange = ImmutableMap.of(0.0, 1.0);
        ImmutableMap<Double, Double> twoSegmentKeyRange = ImmutableMap.of(0.0, 0.5, 0.5, 1.0);
        // scale down to 1 segment.
        assertTrue("Stream Scale down", controller.scaleStream(stream, Lists.newArrayList(0L, 1L),
                singleSegmentKeyRange, executor).getFuture().get());
        // scale up to 2 segments.
        assertTrue("Stream Scale up", controller.scaleStream(stream,
                Lists.newArrayList(computeSegmentId(2, 1)), twoSegmentKeyRange, executor).getFuture().get());
        // scale down to 1 segment.
        assertTrue("Stream Scale down", controller.scaleStream(stream,
                Lists.newArrayList(computeSegmentId(3, 2), computeSegmentId(4, 2)), singleSegmentKeyRange, executor).getFuture().get());
        // scale up to 2 segments.
        assertTrue("Stream Scale up", controller.scaleStream(stream,
                Lists.newArrayList(computeSegmentId(5, 3)), twoSegmentKeyRange, executor).getFuture().get());
        //truncateStream.
        Map<Long, Long> streamCutPositions = new HashMap<>();
        streamCutPositions.put(computeSegmentId(3, 2), 0L);
        streamCutPositions.put(computeSegmentId(4, 2), 0L);
        assertTrue("Truncate stream", controller.truncateStream("test", "test", streamCutPositions).get());

        //write an event.
        writer.writeEvent("0", "truncationTest3");
        writer.flush();

        //Read the event back.
        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl("test", controller, clientFactory,
                connectionFactory);
        groupManager.createReaderGroup("reader", ReaderGroupConfig.builder().disableAutomaticCheckpoints().groupRefreshTimeMillis(0)
                                                                  .stream("test/test").build());
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader("readerId", "reader", new JavaSerializer<>(),
                ReaderConfig.builder().build());
        EventRead<String> event = reader.readNextEvent(200);
        assertNull(event.getEvent());
        groupManager.getReaderGroup("reader").initiateCheckpoint("cp1", executor);
        event = reader.readNextEvent(2000);
        assertEquals("cp1", event.getCheckpointName());
        event = reader.readNextEvent(200);
        assertNull(event.getEvent());
        groupManager.getReaderGroup("reader").initiateCheckpoint("cp2", executor);
        event = reader.readNextEvent(2000);
        assertEquals("cp2", event.getCheckpointName());
        event = reader.readNextEvent(10000);
        assertEquals("truncationTest3", event.getEvent());
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
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope,
                ClientConfig.builder().controllerURI(controllerURI).build());
        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope(scope, controllerURI);
        groupManager.createReaderGroup(readerGroupName, ReaderGroupConfig.builder().disableAutomaticCheckpoints()
                                                                         .stream(scope + "/" + streamName)
                                                                         .build());
        ReaderGroup readerGroup = groupManager.getReaderGroup(readerGroupName);

        // Write two events to the Stream.
        writeEvents(clientFactory, streamName, 2);

        // Read only the first one.
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader(readerGroupName + "1", readerGroupName,
                new UTF8StringSerializer(), ReaderConfig.builder().build());
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
                newReaderGroupName, new UTF8StringSerializer(), ReaderConfig.builder().build());

        assertEquals("Expected read event: ", "1", newReader.readNextEvent(1000).getEvent());
        assertNull(newReader.readNextEvent(1000).getEvent());
    }

    /**
     * This test verifies that truncation works specifying an offset that applies to multiple segments. To this end,
     * the test first writes a set of events on a Stream (with multiple segments) and truncates it at a specified offset
     * (truncatedEvents). The tests asserts that readers gets a TruncatedDataException after truncation and then it
     * (only) reads the remaining events that have not been truncated.
     */
    @Test(timeout = 600000)
    public void testParallelSegmentOffsetTruncation() {
        final String scope = "truncationTests";
        final String streamName = "testParallelSegmentOffsetTruncation";
        final int parallelism = 2;
        final int totalEvents = 100;
        final int truncatedEvents = 25;
        StreamConfiguration streamConf = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(parallelism)).build();
        @Cleanup
        StreamManager streamManager = StreamManager.create(controllerURI);
        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope(scope, controllerURI);
        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, ClientConfig.builder().controllerURI(controllerURI).build());
        streamManager.createScope(scope);

        // Test truncation in new and re-created tests.
        for (int i = 0; i < 2; i++) {
            final String readerGroupName = "RGTestParallelSegmentOffsetTruncation" + i;
            streamManager.createStream(scope, streamName, streamConf);
            groupManager.createReaderGroup(readerGroupName, ReaderGroupConfig.builder().disableAutomaticCheckpoints()
                                                                             .stream(Stream.of(scope, streamName)).build());
            ReaderGroup readerGroup = groupManager.getReaderGroup(readerGroupName);

            // Write events to the Stream.
            writeEvents(clientFactory, streamName, totalEvents);

            // Instantiate readers to consume from Stream up to truncatedEvents.
            List<CompletableFuture<Integer>> futures = ReadWriteUtils.readEvents(clientFactory, readerGroupName, parallelism, truncatedEvents);
            Futures.allOf(futures).join();

            // Perform truncation on stream segment
            Checkpoint cp = readerGroup.initiateCheckpoint("myCheckpoint" + i, executor).join();
            StreamCut streamCut = cp.asImpl().getPositions().values().iterator().next();
            assertTrue(streamManager.truncateStream(scope, streamName, streamCut));

            // Just after the truncation, trying to read the whole stream should raise a TruncatedDataException.
            final String newGroupName = readerGroupName + "new";
            groupManager.createReaderGroup(newGroupName, ReaderGroupConfig.builder().stream(Stream.of(scope, streamName)).build());
            futures = readEvents(clientFactory, newGroupName, parallelism);
            Futures.allOf(futures).join();
            assertEquals("Expected read events: ", totalEvents - (truncatedEvents * parallelism),
                    (int) futures.stream().map(CompletableFuture::join).reduce((a, b) -> a + b).get());
            assertTrue(streamManager.sealStream(scope, streamName));
            assertTrue(streamManager.deleteStream(scope, streamName));
        }
    }

    /**
     * This test checks the behavior of a reader (or group of readers) based on whether segment truncation takes place
     * while reading (first part of the test) or before starting reading (second part).
     *
     * @throws InterruptedException If the current thread is interrupted while waiting for the Controller service.
     */
    @Test(timeout = 60000)
    public void testSegmentTruncationWhileReading() throws InterruptedException {
        final int totalEvents = 100;
        final String scope = "truncationTests";
        final String streamName = "testSegmentTruncationWhileReading";
        final String readerGroupName = "RGTestSegmentTruncationWhileReading";

        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 1))
                                                        .build();
        LocalController controller = (LocalController) controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope(scope).join();
        controller.createStream(scope, streamName, config).join();
        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().controllerURI(controllerURI)
                                                                                    .build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory);

        // Write half of totalEvents to the Stream.
        writeEvents(clientFactory, streamName, totalEvents / 2);

        // Seal current segment (0) and split it into two segments (1,2).
        Stream stream = new StreamImpl(scope, streamName);
        Map<Double, Double> map = new HashMap<>();
        map.put(0.0, 0.5);
        map.put(0.5, 1.0);
        assertTrue(controller.scaleStream(stream, Lists.newArrayList(0L), map, executor).getFuture().join());

        long one = computeSegmentId(1, 1);
        long two = computeSegmentId(2, 1);
        // Write rest of events to the new Stream segments.
        ReadWriteUtils.writeEvents(clientFactory, streamName, totalEvents, totalEvents / 2);

        // Instantiate readers to consume from Stream.
        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl(scope, controller, clientFactory, connectionFactory);
        groupManager.createReaderGroup(readerGroupName,
                                       ReaderGroupConfig.builder()
                                                        .automaticCheckpointIntervalMillis(100)
                                                        .stream(Stream.of(scope, streamName))
                                                        .build());
        EventStreamReader<String> reader = clientFactory.createReader(String.valueOf(0), readerGroupName,
                                                                      new UTF8StringSerializer(),
                                                                      ReaderConfig.builder().build());
        int read = 0;
        while (read < 75) {
            EventRead<String> event = reader.readNextEvent(1000);
            if (event.getEvent() != null) {
                read++;
            }
        }
        // Let readers to consume some events and truncate segment while readers are consuming events
        Exceptions.handleInterrupted(() -> Thread.sleep(500));
        Map<Long, Long> streamCutPositions = new HashMap<>();
        streamCutPositions.put(one, 0L);
        streamCutPositions.put(two, 0L);
        assertTrue(controller.truncateStream(scope, streamName, streamCutPositions).join());

        // Wait for readers to complete and assert that they have read all the events (totalEvents).
        while (read < totalEvents) {
            EventRead<String> event = reader.readNextEvent(1000);
            if (event.getEvent() != null) {
                read++;
            }
        }
        assertEquals(read, totalEvents);
        assertEquals(null, reader.readNextEvent(0).getEvent());

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
        List<CompletableFuture<Integer>> futures = readEvents(clientFactory, newReaderGroupName, 1);
        Futures.allOf(futures).join();
        assertEquals((int) futures.stream().map(CompletableFuture::join).reduce((a, b) -> a + b).get(), totalEvents / 2);
    }

    /**
     * This test checks the behavior of a reader (or group of readers) that gets a delete event while reading. While the
     * client is reading events (Segment Store) the test deletes the Stream (Controller and metadata). Once the client
     * reads all the events and reaches the end of segment, it contacts the Controller to retrieve subsequent segments
     * (if any). However, the Stream-related metadata to answer this request has been previously deleted.
     */
    //@Ignore //TODO: The controller does not currently handle the stream being deleted properly.
    //Once it does so the client will need to throw an appropriate exception, and this test should reflect it.
    @Test(timeout = 20000)
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
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, ClientConfig.builder().controllerURI(controllerURI).build());

        // Write totalEvents to the Stream.
        writeEvents(clientFactory, streamName, totalEvents);

        // Instantiate readers to consume from Stream.
        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope(scope, controllerURI);
        groupManager.createReaderGroup(readerGroup, ReaderGroupConfig.builder().automaticCheckpointIntervalMillis(500).stream(Stream.of(scope, streamName)).build());
        EventStreamReader<String> reader = clientFactory.createReader(String.valueOf(0), readerGroup, new UTF8StringSerializer(), ReaderConfig.builder().build());
        assertEquals(totalEvents / 2, ReadWriteUtils.readEvents(reader, totalEvents / 2, 0));
        reader.close();
        
        val readerRecreated = clientFactory.createReader(String.valueOf(0), readerGroup, new JavaSerializer<>(), ReaderConfig.builder().build());
        
        assertTrue(streamManager.sealStream(scope, streamName));
        assertTrue(streamManager.deleteStream(scope, streamName));

        assertThrows(InvalidStreamException.class, () -> 
            clientFactory.createReader(String.valueOf(1), readerGroup, new JavaSerializer<>(), ReaderConfig.builder().build())
        );

        // At the control plane, we expect a RetriesExhaustedException as readers try to get successor segments from a deleted stream.
        assertThrows(TruncatedDataException.class,
                     () -> ReadWriteUtils.readEvents(readerRecreated, totalEvents / 2, 0));
        assertTrue(!streamManager.deleteStream(scope, streamName));
    }
}

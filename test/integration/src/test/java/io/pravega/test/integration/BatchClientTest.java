/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.pravega.client.ClientFactory;
import io.pravega.client.batch.BatchClient;
import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.batch.impl.SegmentRangeImpl;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.TruncatedDataException;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.hash.RandomFactory;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static io.pravega.shared.segment.StreamSegmentNameUtils.computeSegmentId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
public class BatchClientTest {

    private static final String SCOPE = "testScope";
    private static final String STREAM = "testBatchStream";
    private static final String DATA_OF_SIZE_30 = "data of size 30"; // data length = 22 bytes , header = 8 bytes

    @Rule
    public final Timeout globalTimeout = new Timeout(50, TimeUnit.SECONDS);
    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final URI controllerUri = URI.create("tcp://localhost:" + String.valueOf(controllerPort));
    private final String serviceHost = "localhost";
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int containerCount = 4;
    private final Random random = RandomFactory.create();
    private TestingServer zkTestServer;
    private PravegaConnectionListener server;
    private ControllerWrapper controllerWrapper;
    private ServiceBuilder serviceBuilder;
    private ScheduledExecutorService executor;
    private JavaSerializer<String> serializer;

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
        serializer = new JavaSerializer<>();
    }

    @After
    public void tearDown() throws Exception {
        ExecutorServiceHelpers.shutdown(executor);
        controllerWrapper.close();
        server.close();
        serviceBuilder.close();
        zkTestServer.close();
    }

    @Test
    public void testBatchClient() throws Exception {
        @Cleanup
        ClientFactory clientFactory = ClientFactory.withScope(SCOPE, controllerUri);
        createTestStreamWithEvents(clientFactory);

        BatchClient batchClient = clientFactory.createBatchClient();

        // List out all the segments in the stream.
        ArrayList<SegmentRange> segments = Lists.newArrayList(batchClient.getSegments(Stream.of(SCOPE, STREAM), null, null).getIterator());
        assertEquals("Expected number of segments", 6, segments.size());

        // Batch read all events from stream.
        List<String> batchEventList = new ArrayList<>();
        segments.forEach(segInfo -> {
            @Cleanup
            SegmentIterator<String> segmentIterator = batchClient.readSegment(segInfo, serializer);
            batchEventList.addAll(Lists.newArrayList(segmentIterator));
        });
        assertEquals("Event count", 9, batchEventList.size());

        // Read from a given offset.
        Segment seg0 = new Segment(SCOPE, STREAM, 0);
        SegmentRange seg0Info = SegmentRangeImpl.builder().segment(seg0).startOffset(60).endOffset(90).build();
        @Cleanup
        SegmentIterator<String> seg0Iterator = batchClient.readSegment(seg0Info, serializer);
        ArrayList<String> dataAtOffset = Lists.newArrayList(seg0Iterator);
        assertEquals(1, dataAtOffset.size());
        assertEquals(DATA_OF_SIZE_30, dataAtOffset.get(0));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testBatchClientWithStreamTruncation() throws Exception {
        @Cleanup
        ClientFactory clientFactory = ClientFactory.withScope(SCOPE, controllerUri);
        createTestStreamWithEvents(clientFactory);
        BatchClient batchClient = clientFactory.createBatchClient();

        // 1. Create a StreamCut after 2 events(offset = 2 * 30 = 60).
        StreamCut streamCut60L = new StreamCutImpl(Stream.of(SCOPE, STREAM), ImmutableMap.of(new Segment(SCOPE, STREAM, 0), 60L));
        // 2. Truncate stream.
        assertTrue("truncate stream", controllerWrapper.getController().truncateStream(SCOPE, STREAM, streamCut60L).join());
        // 3a. Fetch Segments using StreamCut.UNBOUNDED>
        ArrayList<SegmentRange> segmentsPostTruncation1 = Lists.newArrayList(batchClient.getSegments(Stream.of(SCOPE, STREAM), StreamCut.UNBOUNDED, StreamCut.UNBOUNDED).getIterator());
        // 3b. Fetch Segments using getStreamInfo() api.
        io.pravega.client.batch.StreamInfo streamInfo = batchClient.getStreamInfo(Stream.of(SCOPE, STREAM)).join();
        ArrayList<SegmentRange> segmentsPostTruncation2 = Lists.newArrayList(batchClient.getSegments(Stream.of(SCOPE, STREAM), streamInfo.getHeadStreamCut(), streamInfo.getTailStreamCut()).getIterator());
        // Validate results.
        validateSegmentCountAndEventCount(batchClient, segmentsPostTruncation1);
        validateSegmentCountAndEventCount(batchClient, segmentsPostTruncation2);
    }

    @Test(expected = TruncatedDataException.class)
    public void testBatchClientWithStreamTruncationPostGetSegments() throws Exception {
        @Cleanup
        ClientFactory clientFactory = ClientFactory.withScope(SCOPE, controllerUri);
        createTestStreamWithEvents(clientFactory);
        BatchClient batchClient = clientFactory.createBatchClient();

        // 1. Fetch Segments.
        ArrayList<SegmentRange> segmentsPostTruncation = Lists.newArrayList(batchClient.getSegments(Stream.of(SCOPE, STREAM), StreamCut.UNBOUNDED, StreamCut.UNBOUNDED).getIterator());

        // 2. Create a StreamCut at the end of segment 0 ( offset = 3 * 30 = 90)
        StreamCut streamCut90L = new StreamCutImpl(Stream.of(SCOPE, STREAM), ImmutableMap.of(new Segment(SCOPE, STREAM, 0), 90L));
        // 3. Truncate stream.
        assertTrue("truncate stream", controllerWrapper.getController().truncateStream(SCOPE, STREAM, streamCut90L).join());
        // 4. Use SegmentRange obtained before truncation.
        SegmentRange s0 = segmentsPostTruncation.stream().filter(segmentRange -> segmentRange.getSegmentId() == 0L).findFirst().get();
        // 5. Read non existent segment.
        List<String> eventList = new ArrayList<>();
        @Cleanup
        SegmentIterator<String> segmentIterator = batchClient.readSegment(s0, serializer);
        eventList.addAll(Lists.newArrayList(segmentIterator));
    }

    private void validateSegmentCountAndEventCount(BatchClient batchClient, ArrayList<SegmentRange> segmentsPostTruncation) {
        //expected segments = 1+ 3 + 2 = 6
        assertEquals("Expected number of segments post truncation", 6, segmentsPostTruncation.size());
        List<String> eventsPostTruncation = new ArrayList<>();
        segmentsPostTruncation.forEach(segInfo -> {
            @Cleanup
            SegmentIterator<String> segmentIterator = batchClient.readSegment(segInfo, serializer);
            eventsPostTruncation.addAll(Lists.newArrayList(segmentIterator));
        });
        assertEquals("Event count post truncation", 7, eventsPostTruncation.size());
    }

    /*
     * Create a test stream with 1 segment which is scaled-up to 3 segments and later scaled-down to 2 segments.
     * Events of constant size are written to the stream before and after scale operation.
     */
    private void createTestStreamWithEvents(ClientFactory clientFactory) throws Exception {
        createStream();
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(STREAM, serializer,
                EventWriterConfig.builder().build());

        // write events to stream with 1 segment.
        write30ByteEvents(3, writer);

        // scale up and write events.
        Map<Double, Double> map = new HashMap<>();
        map.put(0.0, 0.33);
        map.put(0.33, 0.66);
        map.put(0.66, 1.0);
        Boolean result;
        assertTrue("Scale up operation", controllerWrapper.getController().scaleStream(Stream.of(SCOPE, STREAM), Collections.singletonList(0L), map, executor).getFuture().join());
        write30ByteEvents(3, writer);

        //scale down and write events.
        map = new HashMap<>();
        map.put(0.0, 0.5);
        map.put(0.5, 1.0);
        result = controllerWrapper.getController().scaleStream(Stream.of(SCOPE, STREAM), Arrays.asList(computeSegmentId(1, 1),
                computeSegmentId(2, 1), computeSegmentId(3, 1)), map, executor).getFuture().get();
        assertTrue("Scale down operation result", result);
        write30ByteEvents(3, writer);
    }

    private void createStream() throws Exception {
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.fixed(1))
                                                        .build();

        controllerWrapper.getControllerService().createScope(SCOPE).join();
        assertTrue("Create Stream operation", controllerWrapper.getController().createStream(SCOPE, STREAM, config).join());
    }

    private void write30ByteEvents(int numberOfEvents, EventStreamWriter<String> writer) {
        Supplier<String> routingKeyGenerator = () -> String.valueOf(random.nextInt());
        IntStream.range(0, numberOfEvents).forEach( v -> writer.writeEvent(routingKeyGenerator.get(), DATA_OF_SIZE_30).join());
    }
}

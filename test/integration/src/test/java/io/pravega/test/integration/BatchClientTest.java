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
package io.pravega.test.integration;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.pravega.client.BatchClientFactory;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamInfo;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.batch.impl.SegmentRangeImpl;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.segment.impl.NoSuchSegmentException;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.TruncatedDataException;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.hash.RandomFactory;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.common.ThreadPooledTestSuite;
import io.pravega.test.integration.utils.ControllerWrapper;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.shared.NameUtils.computeSegmentId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
public class BatchClientTest extends ThreadPooledTestSuite {

    private static final String SCOPE = "testScope";
    private static final String STREAM = "testBatchStream";
    private static final String DATA_OF_SIZE_30 = "data of size 30"; // data length = 22 bytes , header = 8 bytes

    protected final int controllerPort = TestUtils.getAvailableListenPort();
    protected final String serviceHost = "localhost";
    protected final int servicePort = TestUtils.getAvailableListenPort();
    protected final int containerCount = 4;

    protected TestingServer zkTestServer;

    private final Random random = RandomFactory.create();

    private PravegaConnectionListener server;
    private ControllerWrapper controllerWrapper;
    private ServiceBuilder serviceBuilder;
    private JavaSerializer<String> serializer;
    private ClientConfig clientConfig;
    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    @Before
    public void setUp() throws Exception {
        zkTestServer = new TestingServerStarter().start();

        // Create and start segment store service
        serviceBuilder = createServiceBuilder();
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();
        server = new PravegaConnectionListener(false, servicePort, store, tableStore, serviceBuilder.getLowPriorityExecutor(),
                serviceBuilder.getIndexAppendExecutor());
        server.startListening();

        // Create and start controller service
        controllerWrapper = createControllerWrapper();
        controllerWrapper.awaitRunning();
        serializer = new JavaSerializer<>();

        clientConfig = createClientConfig();
    }

    @After
    public void tearDown() throws Exception {
        controllerWrapper.close();
        server.close();
        serviceBuilder.close();
        zkTestServer.close();
    }

    //region Factory methods that may be overridden by subclasses.

    protected ServiceBuilder createServiceBuilder() {
        return ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
    }

    protected ControllerWrapper createControllerWrapper() {
        return new ControllerWrapper(zkTestServer.getConnectString(),
                false, true,
                controllerPort,
                serviceHost,
                servicePort,
                containerCount, -1);
    }

    protected ClientConfig createClientConfig() {
         return ClientConfig.builder()
                    .controllerURI(URI.create(controllerUri()))
                 .build();
    }

    protected String controllerUri() {
        return "tcp://localhost:" + controllerPort;
    }

    //endregion

    @Test(timeout = 50000)
    public void testListAndReadUsingBatchClient() throws InterruptedException, ExecutionException {
        listAndReadSegmentsUsingBatchClient();
    }

    @Test(timeout = 50000)
    @SuppressWarnings("deprecation")
    public void testBatchClientWithStreamTruncation() throws InterruptedException, ExecutionException {
        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(SCOPE, clientConfig);
        createTestStreamWithEvents(clientFactory);
        log.info("Done creating a test stream with test events");

        @Cleanup
        BatchClientFactory batchClient = BatchClientFactory.withScope(SCOPE, clientConfig);
        log.info("Done creating batch client factory");

        // 1. Create a StreamCut after 2 events(offset = 2 * 30 = 60).
        StreamCut streamCut60L = new StreamCutImpl(Stream.of(SCOPE, STREAM),
                ImmutableMap.of(new Segment(SCOPE, STREAM, 0), 60L));
        // 2. Truncate stream.
        assertTrue("truncate stream", controllerWrapper.getController().truncateStream(SCOPE, STREAM, streamCut60L).join());
        // 3a. Fetch Segments using StreamCut.UNBOUNDED>
        ArrayList<SegmentRange> segmentsPostTruncation1 = Lists.newArrayList(batchClient.getSegments(Stream.of(SCOPE, STREAM), StreamCut.UNBOUNDED, StreamCut.UNBOUNDED).getIterator());
        // 3b. Fetch Segments using fetchStreamInfo() api.
        StreamInfo streamInfo = streamManager.fetchStreamInfo(SCOPE, STREAM).get();
        ArrayList<SegmentRange> segmentsPostTruncation2 = Lists.newArrayList(batchClient.getSegments(Stream.of(SCOPE, STREAM), streamInfo.getHeadStreamCut(), streamInfo.getTailStreamCut()).getIterator());
        // Validate results.
        validateSegmentCountAndEventCount(batchClient, segmentsPostTruncation1);
        validateSegmentCountAndEventCount(batchClient, segmentsPostTruncation2);
    }

    @Test(expected = TruncatedDataException.class, timeout = 50000)
    public void testBatchClientWithStreamTruncationPostGetSegments() throws InterruptedException, ExecutionException {

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(SCOPE, clientConfig);
        createTestStreamWithEvents(clientFactory);
        @Cleanup
        BatchClientFactory batchClient = BatchClientFactory.withScope(SCOPE, clientConfig);

        // 1. Fetch Segments.
        ArrayList<SegmentRange> segmentsPostTruncation = Lists.newArrayList(
                batchClient.getSegments(Stream.of(SCOPE, STREAM), StreamCut.UNBOUNDED, StreamCut.UNBOUNDED).getIterator());

        // 2. Create a StreamCut at the end of segment 0 ( offset = 3 * 30 = 90)
        StreamCut streamCut90L = new StreamCutImpl(Stream.of(SCOPE, STREAM),
                ImmutableMap.of(new Segment(SCOPE, STREAM, 0), 90L));

        // 3. Truncate stream.
        assertTrue("truncate stream",
                controllerWrapper.getController().truncateStream(SCOPE, STREAM, streamCut90L).join());

        // 4. Use SegmentRange obtained before truncation.
        SegmentRange s0 = segmentsPostTruncation.stream().filter(
                segmentRange -> segmentRange.getSegmentId() == 0L).findFirst().get();

        // 5. Read non existent segment.
        List<String> eventList = new ArrayList<>();

        @Cleanup
        SegmentIterator<String> segmentIterator = batchClient.readSegment(s0, serializer);
        eventList.addAll(Lists.newArrayList(segmentIterator));
    }

    @Test(timeout = 50000)
    @SuppressWarnings("deprecation")
    public void testGetDistanceBetweenTwoSCwithStartStreamCutAsUnboundedAndEndAtOffset() throws InterruptedException, ExecutionException {
        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);
        Stream stream = new StreamImpl(SCOPE, STREAM);
        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(SCOPE, clientConfig);
        createTestStreamWithEvents(clientFactory);
        log.info("Done creating a test stream with test events");

        @Cleanup
        BatchClientFactory batchClient = BatchClientFactory.withScope(SCOPE, clientConfig);
        log.info("Done creating batch client factory");

        StreamCut endStreamCut10L = new StreamCutImpl(Stream.of(SCOPE, STREAM),
                ImmutableMap.of(new Segment(SCOPE, STREAM, 0), 10L));
        CompletableFuture<Long> distance =  streamManager.getDistanceBetweenTwoStreamCuts(stream, StreamCut.UNBOUNDED, endStreamCut10L);
        assertEquals(Long.valueOf(10), distance.join());
    }

    @Test(timeout = 50000)
    @SuppressWarnings("deprecation")
    public void testGetDistanceBetweenTwoSCwithStartSCAsUnboundedAndStreamTruncatedatOffset() throws InterruptedException, ExecutionException {
        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);
        Stream stream = new StreamImpl(SCOPE, STREAM);
        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(SCOPE, clientConfig);
        createTestStreamWithEvents(clientFactory);
        log.info("Done creating a test stream with test events");

        @Cleanup
        BatchClientFactory batchClient = BatchClientFactory.withScope(SCOPE, clientConfig);
        log.info("Done creating batch client factory");

        StreamCut streamCut10L = new StreamCutImpl(Stream.of(SCOPE, STREAM),
                ImmutableMap.of(new Segment(SCOPE, STREAM, 0), 10L));
        StreamCut streamCut30L = new StreamCutImpl(Stream.of(SCOPE, STREAM),
                ImmutableMap.of(new Segment(SCOPE, STREAM, 0), 30L));
        assertTrue("truncate stream", controllerWrapper.getController().truncateStream(SCOPE, STREAM, streamCut10L).join());

        CompletableFuture<Long> distance =  streamManager.getDistanceBetweenTwoStreamCuts(stream, StreamCut.UNBOUNDED, streamCut30L);
        assertEquals(Long.valueOf(20), distance.join());
    }

    @Test(timeout = 50000)
    public void testNextStreamCut() throws ExecutionException, InterruptedException {
        StreamConfiguration config = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        Controller controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope(SCOPE + "-1", 0L).get();
        controller.createStream(SCOPE + "-1", STREAM + "-1", config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder().build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE + "-1", controller, connectionFactory);
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(STREAM + "-1", new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        // write events to stream
        write30ByteEvents(10, writer);
        Segment segment = new Segment(SCOPE + "-1", STREAM + "-1", 0);
        StreamCut streamCut = new StreamCutImpl(Stream.of(SCOPE + "-1", STREAM + "-1"),
                ImmutableMap.of(segment, 270L));
        @Cleanup
        BatchClientFactory batchClient = BatchClientFactory.withScope(SCOPE + "-1", clientConfig);
        log.info("Done creating batch client factory");

        StreamCut  nextStreamCut = batchClient.getNextStreamCut(streamCut, 93L);
        assertTrue(nextStreamCut != null);
        assertTrue(nextStreamCut.asImpl().getPositions().size() == 1);
        assertTrue(nextStreamCut.asImpl().getPositions().get(segment).equals(300L));
    }

    @Test(timeout = 50000)
    public void testNextStreamCutWithScaleUp() throws ExecutionException, InterruptedException {
        StreamConfiguration config = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.byEventRate(1, 2, 1))
                .build();
        Controller controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope(SCOPE + "-2", 0L).get();
        controller.createStream(SCOPE + "-2", STREAM + "-2", config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder().build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE + "-2", controller, connectionFactory);
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(STREAM + "-2", new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        // write events to stream
        write30ByteEvents(4, writer);

        Map<Double, Double> keyRanges = new HashMap<>();
        keyRanges.put(0.0, 0.5);
        keyRanges.put(0.5, 1.0);
        Stream stream = new StreamImpl(SCOPE + "-2", STREAM + "-2");
        // Stream scaling up
        Boolean status = controller.scaleStream(stream,
                Collections.singletonList(0L),
                keyRanges,
                executorService()).getFuture().get();
        assertTrue(status);

        write30ByteEvents(6, writer);

        Segment segment0 = new Segment(SCOPE + "-2", STREAM + "-2", 0);
        Segment segment1 = Segment.fromScopedName(SCOPE + "-2/" + STREAM + "-2/1.#epoch.1");
        Segment segment2 = Segment.fromScopedName(SCOPE + "-2/" + STREAM + "-2/2.#epoch.1");
        StreamCut streamCut = new StreamCutImpl(Stream.of(SCOPE + "-2", STREAM + "-2"),
                ImmutableMap.of(segment0, 60L));

        @Cleanup
        BatchClientFactory batchClient = BatchClientFactory.withScope(SCOPE + "-2", clientConfig);
        log.info("Done creating batch client factory");

        ArrayList<SegmentRange> allSegmentsList = Lists.newArrayList(
                batchClient.getSegments(Stream.of(SCOPE + "-2", STREAM + "-2"), StreamCut.UNBOUNDED, StreamCut.UNBOUNDED).getIterator());
        long approxDistanceToNextOffset = 80L;
        Map<Segment, Long> map = allSegmentsList.stream().collect(
                Collectors.toMap(SegmentRange::getSegment, value -> value.getEndOffset() > approxDistanceToNextOffset / 2 ? 60L : value.getEndOffset()));

        StreamCut  nextStreamCut = batchClient.getNextStreamCut(streamCut, approxDistanceToNextOffset);
        assertTrue(nextStreamCut != null);
        assertTrue(nextStreamCut.asImpl().getPositions().size() == 1);
        assertTrue(nextStreamCut.asImpl().getPositions().get(segment0).equals(120L));

        StreamCut  nextStreamCut2 = batchClient.getNextStreamCut(nextStreamCut, approxDistanceToNextOffset);
        assertTrue(nextStreamCut2 != null);
        assertTrue(nextStreamCut2.asImpl().getPositions().size() == 2);
        assertTrue(nextStreamCut2.asImpl().getPositions().containsKey(segment1) &&
                nextStreamCut2.asImpl().getPositions().containsKey(segment2));
        assertTrue(nextStreamCut2.asImpl().getPositions().get(segment1).equals(map.get(segment1)) &&
                nextStreamCut2.asImpl().getPositions().get(segment2).equals(map.get(segment2)));
    }

    @Test(timeout = 50000)
    public void testNextStreamCutWithScaleDown() throws ExecutionException, InterruptedException {
        StreamConfiguration config = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        Controller controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope(SCOPE + "-3", 0L).get();
        controller.createStream(SCOPE + "-3", STREAM + "-3", config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder().build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE + "-3", controller, connectionFactory);
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(STREAM + "-3", new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        // write events to stream with 1 segment.
        write30ByteEvents(4, writer);

        Map<Double, Double> keyRanges = new HashMap<>();
        keyRanges.put(0.0, 0.5);
        keyRanges.put(0.5, 1.0);
        Stream stream = new StreamImpl(SCOPE + "-3", STREAM + "-3");
        // Stream scaling up
        Boolean status = controller.scaleStream(stream,
                Collections.singletonList(0L),
                keyRanges,
                executorService()).getFuture().get();
        assertTrue(status);

        write30ByteEvents(7, writer);
        ArrayList<Long> toSeal = new ArrayList<>();
        toSeal.add(computeSegmentId(1, 1));
        toSeal.add(computeSegmentId(2, 1));

        Map<Double, Double> keyRanges1 = new HashMap<>();
        keyRanges1.put(0.0, 1.0);
        Stream stream1 = new StreamImpl(SCOPE + "-3", STREAM + "-3");
        // Stream scaling down
        Boolean status1 = controller.scaleStream(stream1,
                Collections.unmodifiableList(toSeal),
                keyRanges1,
                executorService()).getFuture().get();
        assertTrue(status1);

        write30ByteEvents(5, writer);

        @Cleanup
        BatchClientFactory batchClient = BatchClientFactory.withScope(SCOPE + "-3", clientConfig);
        log.info("Done creating batch client factory");
        Segment segment1 = Segment.fromScopedName(SCOPE + "-3/" + STREAM + "-3/1.#epoch.1");
        Segment segment2 = Segment.fromScopedName(SCOPE + "-3/" + STREAM + "-3/2.#epoch.1");
        Segment segment3 = Segment.fromScopedName(SCOPE + "-3/" + STREAM + "-3/3.#epoch.2");

        ArrayList<SegmentRange> allSegmentsList = Lists.newArrayList(
                batchClient.getSegments(Stream.of(SCOPE + "-3", STREAM + "-3"), StreamCut.UNBOUNDED, StreamCut.UNBOUNDED).getIterator());

        Map<Segment, Long> map = allSegmentsList.stream().collect(
                Collectors.toMap(SegmentRange::getSegment, SegmentRange::getEndOffset));
        // Getting streamcut at the tail of segment 1 and segment 2
        StreamCut streamCut = new StreamCutImpl(Stream.of(SCOPE + "-3", STREAM + "-3"),
                ImmutableMap.of(segment1, map.get(segment1), segment2, map.get(segment2)));

        StreamCut  nextStreamCut = batchClient.getNextStreamCut(streamCut, 80L);
        assertTrue(nextStreamCut != null);
        assertTrue(nextStreamCut.asImpl().getPositions().size() == 1);
        assertTrue(nextStreamCut.asImpl().getPositions().get(segment3).equals(90L));
    }

    @Test(timeout = 50000)
    public void testNextStreamCutScaleDownNotForwarded() throws ExecutionException, InterruptedException {
        StreamConfiguration config = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        Controller controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope(SCOPE + "-4", 0L).get();
        controller.createStream(SCOPE + "-4", STREAM + "-4", config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder().build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE + "-4", controller, connectionFactory);
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(STREAM + "-4", new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        // write events to stream with 1 segment.
        write30ByteEvents(4, writer);

        Map<Double, Double> keyRanges = new HashMap<>();
        keyRanges.put(0.0, 0.5);
        keyRanges.put(0.5, 1.0);
        Stream stream = new StreamImpl(SCOPE + "-4", STREAM + "-4");
        // Stream scaling up
        Boolean status = controller.scaleStream(stream,
                Collections.singletonList(0L),
                keyRanges,
                executorService()).getFuture().get();
        assertTrue(status);

        write30ByteEvents(7, writer);
        ArrayList<Long> toSeal = new ArrayList<>();
        toSeal.add(computeSegmentId(1, 1));
        toSeal.add(computeSegmentId(2, 1));

        Map<Double, Double> keyRanges1 = new HashMap<>();
        keyRanges1.put(0.0, 1.0);
        Stream stream1 = new StreamImpl(SCOPE + "-4", STREAM + "-4");
        // Stream scaling down
        Boolean status1 = controller.scaleStream(stream1,
                Collections.unmodifiableList(toSeal),
                keyRanges1,
                executorService()).getFuture().get();
        assertTrue(status1);

        write30ByteEvents(5, writer);

        @Cleanup
        BatchClientFactory batchClient = BatchClientFactory.withScope(SCOPE + "-4", clientConfig);
        log.info("Done creating batch client factory");
        Segment segment1 = Segment.fromScopedName(SCOPE + "-4/" + STREAM + "-4/1.#epoch.1");
        Segment segment2 = Segment.fromScopedName(SCOPE + "-4/" + STREAM + "-4/2.#epoch.1");

        ArrayList<SegmentRange> allSegmentsList = Lists.newArrayList(
                batchClient.getSegments(Stream.of(SCOPE + "-4", STREAM + "-4"), StreamCut.UNBOUNDED, StreamCut.UNBOUNDED).getIterator());

        Map<Segment, Long> map = allSegmentsList.stream().collect(
                Collectors.toMap(SegmentRange::getSegment, SegmentRange::getEndOffset));
        // Getting streamcut at prior to tail of segment 1 and prior to tail for segment 2.
        long segment1Offset = map.get(segment1) > 0L ? map.get(segment1)-30L : 0L;
        long segment2Offset = map.get(segment2) > 0L ? map.get(segment2)-30L : 0L;
        StreamCut streamCut = new StreamCutImpl(Stream.of(SCOPE + "-4", STREAM + "-4"),
                ImmutableMap.of(segment1, segment1Offset, segment2, segment2Offset));

        long expectedSegment1Offset = map.get(segment1) > 0L ? (segment1Offset + 30L) : 0;
        long expectedSegment2Offset = map.get(segment2) > 0L ? (segment2Offset + 30L) : 0;
        StreamCut  nextStreamCut = batchClient.getNextStreamCut(streamCut, 80L);
        assertTrue(nextStreamCut != null);
        assertTrue(nextStreamCut.asImpl().getPositions().size() == 2);
        assertTrue(nextStreamCut.asImpl().getPositions().containsKey(segment1) &&
                nextStreamCut.asImpl().getPositions().containsKey(segment2));
        assertTrue(nextStreamCut.asImpl().getPositions().get(segment1).equals(expectedSegment1Offset) &&
                nextStreamCut.asImpl().getPositions().get(segment2).equals(expectedSegment2Offset));

        // Getting streamcut at prior to tail of one of the segment.
        streamCut = map.get(segment1) == 0L ? new StreamCutImpl(Stream.of(SCOPE + "-4", STREAM + "-4"),
                ImmutableMap.of(segment1, 0L, segment2, map.get(segment2)-30L)) : new StreamCutImpl(Stream.of(SCOPE + "-4", STREAM + "-4"),
                ImmutableMap.of(segment1, map.get(segment1)-30L, segment2, map.get(segment2)));

        expectedSegment1Offset = map.get(segment1) == 0L ? 0L : map.get(segment1);
        expectedSegment2Offset = map.get(segment2);

        nextStreamCut = batchClient.getNextStreamCut(streamCut, 80L);
        assertTrue(nextStreamCut != null);
        assertTrue(nextStreamCut.asImpl().getPositions().size() == 2);
        assertTrue(nextStreamCut.asImpl().getPositions().containsKey(segment1) &&
                nextStreamCut.asImpl().getPositions().containsKey(segment2));
        assertTrue(nextStreamCut.asImpl().getPositions().get(segment1).equals(expectedSegment1Offset) &&
                nextStreamCut.asImpl().getPositions().get(segment2).equals(expectedSegment2Offset));
    }

    @Test(timeout = 50000)
    public void testNextStreamCutException() {
        @Cleanup
        BatchClientFactory batchClient = BatchClientFactory.withScope(SCOPE + "-4", clientConfig);
        log.info("Done creating batch client factory");
        Segment segment0 = new Segment(SCOPE + "-5", STREAM + "-5", 0);
        StreamCut streamCut = new StreamCutImpl(Stream.of(SCOPE + "-5", STREAM + "-5"),
                ImmutableMap.of(segment0, 120L));

        AssertExtensions.assertThrows(NoSuchSegmentException.class, () -> batchClient.getNextStreamCut(streamCut, 80L));
    }

    @Test(timeout = 50000)
    public void testNextStreamCutScaleUpAndDown() throws ExecutionException, InterruptedException {
        StreamConfiguration config = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        Controller controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope(SCOPE + "-6", 0L).get();
        controller.createStream(SCOPE + "-6", STREAM + "-6", config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder().build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE + "-6", controller, connectionFactory);
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(STREAM + "-6", new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        // write events to stream with 1 segment.
        write30ByteEvents(4, writer);

        Map<Double, Double> keyRanges = new HashMap<>();
        keyRanges.put(0.0, 0.25);
        keyRanges.put(0.25, 0.5);
        keyRanges.put(0.5, 0.75);
        keyRanges.put(0.75, 1.0);
        Stream stream = new StreamImpl(SCOPE + "-6", STREAM + "-6");
        // Stream scaling up
        Boolean status = controller.scaleStream(stream,
                Collections.singletonList(0L),
                keyRanges,
                executorService()).getFuture().get();
        assertTrue(status);

        write30ByteEvents(17, writer);
        ArrayList<Long> toSeal = new ArrayList<>();
        toSeal.add(computeSegmentId(2, 1));
        toSeal.add(computeSegmentId(3, 1));

        Map<Double, Double> keyRanges1 = new HashMap<>();
        keyRanges1.put(0.25, 0.75);
        Stream stream1 = new StreamImpl(SCOPE + "-6", STREAM + "-6");
        // Stream scaling down
        Boolean status1 = controller.scaleStream(stream1,
                Collections.unmodifiableList(toSeal),
                keyRanges1,
                executorService()).getFuture().get();
        assertTrue(status1);

        Map<Double, Double> keyRanges2 = new HashMap<>();
        keyRanges2.put(0., 0.12);
        keyRanges2.put(0.12, 0.25);
        // Stream scaling up
        Boolean status2 = controller.scaleStream(stream,
                Collections.singletonList(computeSegmentId(1, 1)),
                keyRanges2,
                executorService()).getFuture().get();
        assertTrue(status2);

        @Cleanup
        BatchClientFactory batchClient = BatchClientFactory.withScope(SCOPE + "-6", clientConfig);
        log.info("Done creating batch client factory");
        Segment segment1 = Segment.fromScopedName(SCOPE + "-6/" + STREAM + "-6/1.#epoch.1");
        Segment segment2 = Segment.fromScopedName(SCOPE + "-6/" + STREAM + "-6/2.#epoch.1");
        Segment segment3 = Segment.fromScopedName(SCOPE + "-6/" + STREAM + "-6/3.#epoch.1");
        Segment segment4 = Segment.fromScopedName(SCOPE + "-6/" + STREAM + "-6/4.#epoch.1");
        Segment segment5 = Segment.fromScopedName(SCOPE + "-6/" + STREAM + "-6/5.#epoch.2");
        Segment segment6 = Segment.fromScopedName(SCOPE + "-6/" + STREAM + "-6/6.#epoch.3");
        Segment segment7 = Segment.fromScopedName(SCOPE + "-6/" + STREAM + "-6/7.#epoch.3");

        ArrayList<SegmentRange> allSegmentsList = Lists.newArrayList(
                batchClient.getSegments(Stream.of(SCOPE + "-6", STREAM + "-6"), StreamCut.UNBOUNDED, StreamCut.UNBOUNDED).getIterator());

        Map<Segment, Long> map = allSegmentsList.stream().collect(
                Collectors.toMap(SegmentRange::getSegment, SegmentRange::getEndOffset));

        // writing to segments 4,5,6,7
        write30ByteEvents(15, writer);

        StreamCut streamCut = new StreamCutImpl(Stream.of(SCOPE + "-6", STREAM + "-6"),
                ImmutableMap.of(segment1, map.get(segment1), segment2, map.get(segment2), segment3, map.get(segment3), segment4, map.get(segment4)));

        allSegmentsList = Lists.newArrayList(
                batchClient.getSegments(Stream.of(SCOPE + "-6", STREAM + "-6"), StreamCut.UNBOUNDED, StreamCut.UNBOUNDED).getIterator());
        Map<Segment, Long> newPositionMap = allSegmentsList.stream().collect(
                Collectors.toMap(SegmentRange::getSegment, SegmentRange::getEndOffset));
        long expectedSegment4Offset = newPositionMap.get(segment4) - map.get(segment4) > 30L ? map.get(segment4) + 30L : map.get(segment4);
        long expectedSegment5Offset = newPositionMap.get(segment5) > 60L ? 60L : newPositionMap.get(segment5);
        long expectedSegment6Offset = newPositionMap.get(segment6) > 30L ? 30L : newPositionMap.get(segment6);
        long expectedSegment7Offset = newPositionMap.get(segment7) > 30L ? 30L : newPositionMap.get(segment7);

        StreamCut  nextStreamCut = batchClient.getNextStreamCut(streamCut, 80L);
        assertTrue(nextStreamCut != null);
        assertTrue(nextStreamCut.asImpl().getPositions().size() == 4);
        assertTrue(nextStreamCut.asImpl().getPositions().containsKey(segment4) &&
                nextStreamCut.asImpl().getPositions().containsKey(segment5) &&
                nextStreamCut.asImpl().getPositions().containsKey(segment6) &&
                nextStreamCut.asImpl().getPositions().containsKey(segment7));
        assertTrue(nextStreamCut.asImpl().getPositions().get(segment4).equals(expectedSegment4Offset) &&
                nextStreamCut.asImpl().getPositions().get(segment5).equals(expectedSegment5Offset) &&
                nextStreamCut.asImpl().getPositions().get(segment6).equals(expectedSegment6Offset) &&
                nextStreamCut.asImpl().getPositions().get(segment7).equals(expectedSegment7Offset));
    }

    //region Private helper methods

    private void listAndReadSegmentsUsingBatchClient() throws InterruptedException, ExecutionException {
        listAndReadSegmentsUsingBatchClient(SCOPE, STREAM, clientConfig);
    }

    protected void listAndReadSegmentsUsingBatchClient(String scopeName, String streamName, ClientConfig config)
            throws InterruptedException, ExecutionException {
        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scopeName, config);
        createTestStreamWithEvents(clientFactory);
        log.info("Done creating test event stream with test events");

        @Cleanup
        BatchClientFactory batchClient = BatchClientFactory.withScope(scopeName, config);

        // List out all the segments in the stream.
        ArrayList<SegmentRange> segments = Lists.newArrayList(
                batchClient.getSegments(Stream.of(scopeName, streamName), null, null).getIterator());
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
        Segment seg0 = new Segment(scopeName, streamName, 0);
        SegmentRange seg0Info = SegmentRangeImpl.builder().segment(seg0).startOffset(60).endOffset(90).build();
        @Cleanup
        SegmentIterator<String> seg0Iterator = batchClient.readSegment(seg0Info, serializer);
        ArrayList<String> dataAtOffset = Lists.newArrayList(seg0Iterator);
        assertEquals(1, dataAtOffset.size());
        assertEquals(DATA_OF_SIZE_30, dataAtOffset.get(0));
    }

    private void validateSegmentCountAndEventCount(BatchClientFactory batchClient, ArrayList<SegmentRange> segmentsPostTruncation) {
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
    protected void createTestStreamWithEvents(EventStreamClientFactory clientFactory) throws InterruptedException, ExecutionException {
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
        assertTrue("Scale up operation", controllerWrapper.getController().scaleStream(Stream.of(SCOPE, STREAM), Collections.singletonList(0L), map, executorService()).getFuture().join());
        write30ByteEvents(3, writer);

        //scale down and write events.
        map = new HashMap<>();
        map.put(0.0, 0.5);
        map.put(0.5, 1.0);
        result = controllerWrapper.getController().scaleStream(Stream.of(SCOPE, STREAM), Arrays.asList(computeSegmentId(1, 1),
                computeSegmentId(2, 1), computeSegmentId(3, 1)), map, executorService()).getFuture().get();
        assertTrue("Scale down operation result", result);
        write30ByteEvents(3, writer);
    }

    private void createStream() {
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.fixed(1))
                                                        .build();
        controllerWrapper.getControllerService().createScope(SCOPE, 0L).join();
        assertTrue("Create Stream operation", controllerWrapper.getController().createStream(SCOPE, STREAM, config).join());
    }

    private void write30ByteEvents(int numberOfEvents, EventStreamWriter<String> writer) {
        Supplier<String> routingKeyGenerator = () -> String.valueOf(random.nextInt());
        IntStream.range(0, numberOfEvents).forEach( v -> writer.writeEvent(routingKeyGenerator.get(), DATA_OF_SIZE_30).join());
    }

    //endregion
}
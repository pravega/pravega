/**
 * Copyright Pravega Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.test.system;

import com.google.common.collect.Lists;
import io.pravega.client.BatchClientFactory;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentTruncatedException;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.hash.RandomFactory;
import io.pravega.test.common.InlineExecutor;
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
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.pravega.shared.NameUtils.computeSegmentId;
import static org.junit.Assert.*;

@Slf4j
@RunWith(SystemTestRunner.class)
public class SegmentReaderAPITest extends AbstractReadWriteTest {
    private final Random random = RandomFactory.create();
    private static final String DATA_OF_SIZE_30 = "data of size 30"; // data length = 22 bytes , header = 8 bytes

    @Rule
    public Timeout globalTimeout = Timeout.seconds(5 * 60);
    private URI controllerURI = null;
    private StreamManager streamManager = null;
    private Controller controller = null;
    private ConnectionFactory connectionFactory = null;
    private final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "executor");

    /**
     * This is used to setup the various services required by the system test framework.
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
        controllerURI = fetchControllerURI();
        streamManager = StreamManager.create(Utils.buildClientConfig(controllerURI));

        connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder().build());
        controller = new ControllerImpl(ControllerImplConfig.builder()
                .clientConfig(Utils.buildClientConfig(controllerURI))
                .build(), connectionFactory.getInternalExecutor());
    }

    @After
    public void tearDown() {
        streamManager.close();
        controller.close();
        connectionFactory.close();
    }

    @Test
    public void getNextStreamCutWithScaleUpTest() throws SegmentTruncatedException, ExecutionException, InterruptedException, TimeoutException {
        String STREAM_NAME = "testStreamSegment";
        String STREAM_SCOPE = "testScopeSegment";
        String READER_GROUP = "testReaderGroupSegment";
        String READER_NAME = UUID.randomUUID().toString();
        long CLOCK_ADVANCE_INTERVAL = 60 * 1000000000L;

        StreamConfiguration config = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(fetchControllerURI())
                .build();
        URI controllerURI = clientConfig.getControllerURI();

        Stream stream = Stream.of(STREAM_SCOPE, STREAM_NAME);

        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(STREAM_SCOPE, controller, connectionFactory);

        assertTrue(controller.createScope(STREAM_SCOPE).join());
        assertTrue(controller.createStream(STREAM_SCOPE, STREAM_NAME, config).join());

        @Cleanup
        BatchClientFactory batchClient = BatchClientFactory.withScope(STREAM_SCOPE, clientConfig);

        //Not required later can be removed.
        List<SegmentRange> ranges = Lists.newArrayList(batchClient.getSegments(stream, StreamCut.UNBOUNDED, StreamCut.UNBOUNDED).getIterator());
        List<Segment> list = ranges.stream().map(SegmentRange::getSegment).collect(Collectors.toList());
        assertEquals(1, list.size());
        log.info("Segment name ::{}", list.get(0).getScopedName());

        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(STREAM_NAME, new JavaSerializer<>(),
                EventWriterConfig.builder().build());

        // write events to stream 30 *10  = 300 bytes
        write30ByteEvents(10, writer);

        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope(STREAM_SCOPE, controllerURI);
        groupManager.createReaderGroup(READER_GROUP, ReaderGroupConfig.builder().stream(Stream.of(STREAM_SCOPE, STREAM_NAME)).build());

        @Cleanup
        ReaderGroup readerGroup = groupManager.getReaderGroup(READER_GROUP);

        Map<Stream, StreamCut> streamCutMap0 = new HashMap<>(readerGroup.getStreamCuts());
        StreamCut streamCut0 = streamCutMap0.get(stream);
        log.info("Initial stream cut0 {}", streamCut0);

        //Requested next stream cut at a distance of 170 bytes, and getting 180 offset as a response.
        StreamCut streamCut1 = batchClient.getNextStreamCut(streamCutMap0.get(stream), 170L);
        Map<Stream, StreamCut> streamCutMap1 = new HashMap<>();
        streamCutMap1.put(stream, streamCut1);
        log.info("Next stream cut1 {}", streamCut1);

        // For reading the events between the two streamCut, startStreamCUt = streamCutMap0 and endStreamCut =streamCutMap1
        AtomicLong clock = new AtomicLong();
        @Cleanup
        EventStreamReader<String> reader0 = clientFactory.createReader(READER_NAME,
                READER_GROUP,
                new JavaSerializer<>(),
                ReaderConfig.builder().build(), clock::get,
                clock::get);

        readEvent(reader0);

        assertEquals(180, streamCut1.asImpl().getPositions().get(list.get(0)).longValue());

        //TODO start
        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        final InlineExecutor backgroundExecutor = new InlineExecutor();
        CompletableFuture<Checkpoint> checkPointerReader = readerGroup.initiateCheckpoint("CheckpointReader1", backgroundExecutor);
        assertFalse(checkPointerReader.isDone());

        // reader read next event at checkpoint
        EventRead<String> readEvent0 = reader0.readNextEvent(100);
        log.info("**********readEvent0.isCheckpoint():{}  ****readEvent0.getCheckpointName():{} *******event::{}"
                , readEvent0.isCheckpoint(), readEvent0.getCheckpointName(), readEvent0.getEvent());

        assertTrue(readEvent0.isCheckpoint());
        assertEquals("CheckpointReader1", readEvent0.getCheckpointName());
        assertNull(readEvent0.getEvent());

        readEvent0 = reader0.readNextEvent(100);
        assertNull(readEvent0.getEvent());

        Checkpoint cpResultReader1 = checkPointerReader.get(10, TimeUnit.SECONDS);

        log.info("**********checkPointerReader.isDone()::{}******", checkPointerReader.isDone());
        log.info("**********isReadCompleted*****{}", reader0.readNextEvent(100).isReadCompleted());
        assertTrue(checkPointerReader.isDone());
        //TODO end

        // For reading the events between the two streamCut, startStreamCUt = streamCutMap1 and endStreamCut =streamCutMap2
        StreamCut streamCut2 = batchClient.getNextStreamCut(streamCutMap0.get(stream), 250L);
        Map<Stream, StreamCut> streamCutMap2 = new HashMap<>();
        streamCutMap2.put(stream, streamCut2);
        log.info("Next stream cut2 {}", streamCutMap2);

        ReaderGroupConfig readerGroupConfig2 = getReaderGroupConfig(streamCutMap1, streamCutMap2);

        // Need to do the assertion
        readerGroup.resetReaderGroup(readerGroupConfig2);
        reader0.close();

        reader0 = clientFactory.createReader(READER_NAME,
                READER_GROUP,
                new JavaSerializer<>(),
                ReaderConfig.builder().build());
        readEvent(reader0);

        EventRead event2 = reader0.readNextEvent(100);
        assertNull(event2.getEvent());
        assertFalse(event2.isCheckpoint());
        assertEquals(270, streamCut2.asImpl().getPositions().get(list.get(0)).longValue());

        // For reading the events between the two streamCut, startStreamCUt = streamCutMap2 and endStreamCut =streamCutMap3
        long approxDistanceToNextOffset = 350L;
        StreamCut streamCut3 = batchClient.getNextStreamCut(streamCutMap0.get(stream), approxDistanceToNextOffset);
        Map<Stream, StreamCut> streamCutMap3 = new HashMap<>();
        streamCutMap3.put(stream, streamCut3);
        log.info("Next stream cut3 {}", streamCutMap3);

        ReaderGroupConfig readerGroupConfig3 = getReaderGroupConfig(streamCutMap2, streamCutMap3);

        // Need to do the assertion
        readerGroup.resetReaderGroup(readerGroupConfig3);
        reader0.close();

        reader0 = clientFactory.createReader(READER_NAME,
                READER_GROUP,
                new JavaSerializer<>(),
                ReaderConfig.builder().build());
        readEvent(reader0);
        EventRead event3 = reader0.readNextEvent(100);

        assertNull(event3.getEvent());
        assertFalse(event3.isCheckpoint());
        assertEquals(300, streamCut3.asImpl().getPositions().get(list.get(0)).longValue());
        reader0.close();

        StreamCut streamCut4 = batchClient.getNextStreamCut(streamCut3, approxDistanceToNextOffset);
        log.info("Next stream cut4 {}", streamCut4);

        //Scaling up begin
        Map<Double, Double> keyRanges = new HashMap<>();
        keyRanges.put(0.0, 0.5);
        keyRanges.put(0.5, 1.0);

        // Stream scaling up
        Boolean status = controller.scaleStream(stream,
                Collections.singletonList(0L),
                keyRanges,
                executorService).getFuture().get();
        assertTrue(status);
        write30ByteEvents(5, writer);

        StreamCut nextStreamCut5 = batchClient.getNextStreamCut(streamCut4, approxDistanceToNextOffset);
        log.info("Next stream cut5 {}", nextStreamCut5);

        Segment segment1 = Segment.fromScopedName(STREAM_SCOPE + "/" + STREAM_NAME + "/1.#epoch.1");
        Segment segment2 = Segment.fromScopedName(STREAM_SCOPE + "/" + STREAM_NAME + "/2.#epoch.1");
        log.info("Segment1 name {} and Segment2 name {}", segment1.getScopedName(), segment2.getScopedName());

        ArrayList<SegmentRange> segmentList1 = Lists.newArrayList(batchClient.getSegments(stream, StreamCut.UNBOUNDED, StreamCut.UNBOUNDED).getIterator());
        log.info("Segment List1 :{}", segmentList1);

        Map<Segment, Long> map = segmentList1.stream().collect(Collectors.toMap(SegmentRange::getSegment, value -> value.getEndOffset()));

        assertTrue(nextStreamCut5 != null);
        assertTrue(nextStreamCut5.asImpl().getPositions().size() == 2);
        assertTrue(nextStreamCut5.asImpl().getPositions().containsKey(segment1) &&
                nextStreamCut5.asImpl().getPositions().containsKey(segment2));
        //Scaling up end
    }

    @Test
    public void getNextStreamCutWithScaleDownTest() throws SegmentTruncatedException, ExecutionException, InterruptedException {
        String STREAM_NAME = "testStreamSegmentScaleDown";
        String STREAM_SCOPE = "testScopeSegmentScaleDown";
        String READER_GROUP = "testReaderGroupSegmentScaleDown";
        String READER_NAME = UUID.randomUUID().toString();

        StreamConfiguration config = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        ClientConfig clientConfig = ClientConfig.builder()
                .controllerURI(fetchControllerURI())
                .build();
        URI controllerURI = clientConfig.getControllerURI();

        Stream stream = Stream.of(STREAM_SCOPE, STREAM_NAME);

        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(STREAM_SCOPE, controller, connectionFactory);

        assertTrue(controller.createScope(STREAM_SCOPE).join());
        assertTrue(controller.createStream(STREAM_SCOPE, STREAM_NAME, config).join());

        @Cleanup
        BatchClientFactory batchClient = BatchClientFactory.withScope(STREAM_SCOPE, clientConfig);

        List<SegmentRange> ranges = Lists.newArrayList(batchClient.getSegments(stream, StreamCut.UNBOUNDED, StreamCut.UNBOUNDED).getIterator());
        List<Segment> list = ranges.stream().map(SegmentRange::getSegment).collect(Collectors.toList());
        assertEquals(1, list.size());

        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(STREAM_NAME, new JavaSerializer<>(),
                EventWriterConfig.builder().build());

        // write events to stream 30 * 5  = 150 bytes
        write30ByteEvents(5, writer);

        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope(STREAM_SCOPE, controllerURI);
        groupManager.createReaderGroup(READER_GROUP, ReaderGroupConfig.builder().stream(Stream.of(STREAM_SCOPE, STREAM_NAME)).build());

        @Cleanup
        ReaderGroup readerGroup = groupManager.getReaderGroup(READER_GROUP);

        Map<Stream, StreamCut> streamCutMap0 = new HashMap<>(readerGroup.getStreamCuts());
        StreamCut streamCut0 = streamCutMap0.get(stream);
        log.info("Initial stream cut0 {}", streamCut0);

        //Requested next stream cut at a distance of 170 bytes, and getting 180 offset as a response.
        StreamCut streamCut1 = batchClient.getNextStreamCut(streamCutMap0.get(stream), 170L);
        Map<Stream, StreamCut> streamCutMap1 = new HashMap<>();
        streamCutMap1.put(stream, streamCut1);
        log.info("Next stream cut1 {}", streamCut1);

        // For reading the events between the two streamCut, startStreamCUt = streamCutMap0 and endStreamCut =streamCutMap1
        @Cleanup
        EventStreamReader<String> reader0 = clientFactory.createReader(READER_NAME,
                READER_GROUP,
                new JavaSerializer<>(),
                ReaderConfig.builder().build());
        readEvent(reader0);

        assertEquals(150, streamCut1.asImpl().getPositions().get(list.get(0)).longValue());
        assertNull(reader0.readNextEvent(100).getEvent());
        reader0.close();

        //Scaling up begin
        Map<Double, Double> keyRanges = new HashMap<>();
        keyRanges.put(0.0, 0.5);
        keyRanges.put(0.5, 1.0);

        Boolean status = controller.scaleStream(stream,
                Collections.singletonList(0L),
                keyRanges,
                executor).getFuture().get();
        assertTrue(status);

        write30ByteEvents(5, writer);

        long approxDistanceToNextOffset = 180L;
        StreamCut nextStreamCut2 = batchClient.getNextStreamCut(streamCut1, approxDistanceToNextOffset);
        log.info("Next stream cut2 {}", nextStreamCut2);

        Segment segment1 = Segment.fromScopedName(STREAM_SCOPE + "/" + STREAM_NAME + "/1.#epoch.1");
        Segment segment2 = Segment.fromScopedName(STREAM_SCOPE + "/" + STREAM_NAME + "/2.#epoch.1");
        log.info("Segment1 name {} and Segment2 name {}", segment1.getScopedName(), segment2.getScopedName());

        ArrayList<SegmentRange> segmentList1 = Lists.newArrayList(batchClient.getSegments(stream, StreamCut.UNBOUNDED, StreamCut.UNBOUNDED).getIterator());

        Map<Segment, Long> map = segmentList1.stream().collect(Collectors.toMap(SegmentRange::getSegment, value -> value.getEndOffset()));
        log.info("Segment map :{}", map);

        assertTrue(nextStreamCut2 != null);
        assertTrue(nextStreamCut2.asImpl().getPositions().size() == 2);
        assertTrue(nextStreamCut2.asImpl().getPositions().containsKey(segment1) &&
                nextStreamCut2.asImpl().getPositions().containsKey(segment2));
        //Scaling up end

        //Scaling down start
        ArrayList<Long> toSeal = new ArrayList<>();
        toSeal.add(computeSegmentId(1, 1));
        toSeal.add(computeSegmentId(2, 1));

        Map<Double, Double> keyRanges1 = new HashMap<>();
        keyRanges1.put(0.0, 1.0);

        Boolean status1 = controller.scaleStream(stream,
                Collections.unmodifiableList(toSeal),
                keyRanges1,
                executor).getFuture().get();
        assertTrue(status1);

        write30ByteEvents(5, writer);

        Segment segment3 = Segment.fromScopedName(STREAM_SCOPE + "/" + STREAM_NAME + "/3.#epoch.2");
        log.info("segment3 name :{}", segment3.getScopedName());

        ArrayList<SegmentRange> segmentList2 = Lists.newArrayList(
                batchClient.getSegments(stream, StreamCut.UNBOUNDED, StreamCut.UNBOUNDED).getIterator());
        log.info("Segment List2 :{}", segmentList2);

        Map<Segment, Long> map1 = segmentList2.stream().collect(Collectors.toMap(SegmentRange::getSegment, value -> value.getEndOffset()));
        StreamCut nextStreamCut3 = batchClient.getNextStreamCut(nextStreamCut2, approxDistanceToNextOffset);

        assertTrue(nextStreamCut3 != null);
        assertEquals(150, nextStreamCut3.asImpl().getPositions().get(segment3).longValue());
        assertTrue(map1.containsKey(segment1));
    }

    private URI fetchControllerURI() {
        Service conService = Utils.createPravegaControllerService(null);
        List<URI> ctlURIs = conService.getServiceDetails();
        return ctlURIs.get(0);
    }

    private void write30ByteEvents(int numberOfEvents, EventStreamWriter<String> writer) {
        Supplier<String> routingKeyGenerator = () -> String.valueOf(random.nextInt());
        IntStream.range(0, numberOfEvents).forEach(v -> writer.writeEvent(routingKeyGenerator.get(), DATA_OF_SIZE_30).join());
    }

    public static void readEvent(EventStreamReader<String> reader) {
        String event = "";
        while (event != null) {
            event = reader.readNextEvent(10000).getEvent();
            log.info("Read Event Data : {}", event);
        }
    }

    private static ReaderGroupConfig getReaderGroupConfig(Map<Stream, StreamCut> streamCutMap1, Map<Stream, StreamCut> streamCutMap2) {
        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .disableAutomaticCheckpoints()
                .startFromStreamCuts(streamCutMap1)
                .endingStreamCuts(streamCutMap2)
                .build();
        return readerGroupConfig;
    }
}
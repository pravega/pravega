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

import com.google.common.collect.Lists;
import io.pravega.client.BatchClientFactory;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentTruncatedException;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.hash.RandomFactory;
import io.pravega.test.system.framework.Environment;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.Utils;
import io.pravega.test.system.framework.services.Service;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import mesosphere.marathon.client.MarathonException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.URI;
import java.util.Random;
import java.util.UUID;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.pravega.shared.NameUtils.computeSegmentId;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;

@Slf4j
@RunWith(SystemTestRunner.class)
public class SegmentReaderAPITest extends AbstractReadWriteTest {
    private static final String DATA_OF_SIZE_30 = "this is a test strings"; // data length = 22 bytes , header = 8 bytes
    private static final long CLOCK_ADVANCE_INTERVAL = 5 * 1000000000L;
    private final Random random = RandomFactory.create();

    private URI controllerURI = null;

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
        Service conService = Utils.createPravegaControllerService(null);
        List<URI> ctlURIs = conService.getServiceDetails();
        controllerURI = ctlURIs.get(0);
    }

    @After
    public void tearDown() {
        ExecutorServiceHelpers.shutdown(executor);
    }

    /**
     * This test the getNextStreamCutWithScaleUpTest api with the current streamcut containing one segment0, and written the 10 events,
     * created the streamCut0, streamCut1, streamCut2, streamCut3, streamCut4 and streamCut5 by calling getNextStreamCut.
     * Read the events between the streamCut and segment0 has scaled up(segment1, segment2) and written 5 events and validating the numbers of events written
     * by reading the events from the scaled up segment.
     */
    @Test(timeout = 120000)
    public void getNextStreamCutWithScaleUpTest() throws SegmentTruncatedException, ExecutionException, InterruptedException {
        String streamName = "testStreamSegment";
        String streamScope = "testScopeSegment";
        String readerGroupName = "testReaderGroupSegment";
        String readerName = UUID.randomUUID().toString();
        AtomicLong clock = new AtomicLong();

        final ClientConfig clientConfig = Utils.buildClientConfig(controllerURI);
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(clientConfig);
        ControllerImpl controller = new ControllerImpl(ControllerImplConfig.builder().clientConfig(clientConfig).build(),
                connectionFactory.getInternalExecutor());

        StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        Stream stream = Stream.of(streamScope, streamName);

        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(streamScope, controller, connectionFactory);
        log.info("Invoking Writer test with Controller URI: {}", controllerURI);

        StreamManager streamManager = StreamManager.create(clientConfig);
        assertTrue("Creating Scope", streamManager.createScope(streamScope));
        assertTrue("Creating stream", streamManager.createStream(streamScope, streamName, streamConfiguration));

        @Cleanup
        BatchClientFactory batchClient = BatchClientFactory.withScope(streamScope, clientConfig);

        StreamCut streamCut0 = streamManager.fetchStreamInfo(streamScope, streamName).join().getHeadStreamCut();
        log.info("Initial stream streamCut0 {}", streamCut0);
        assertEquals(1, streamCut0.asImpl().getPositions().size());

        List<Segment> list = streamCut0.asImpl().getPositions().keySet().stream().collect(Collectors.toList());

        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new UTF8StringSerializer(),
                EventWriterConfig.builder().build());

        // write events to stream 30 *10  = 300 bytes
        writeEvents(10, writer);
        writer.flush();

        //Requested next stream cut at a distance of 170 bytes, and getting the next approx offset as a response.
        StreamCut streamCut1 = batchClient.getNextStreamCut(streamCut0, 170L);
        long streamCut1Position = streamCut1.asImpl().getPositions().get(list.get(0)).longValue();
        log.info("Next stream cut1 {} streamCut1 position {}", streamCut1, streamCut1Position);
        assertTrue(180 <= streamCut1Position);

        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope(streamScope, controllerURI);
        ReaderGroupConfig readerGroupConfig1 = getReaderGroupConfig(streamCut0, streamCut1, stream);
        groupManager.createReaderGroup(readerGroupName, readerGroupConfig1);

        @Cleanup
        ReaderGroup readerGroup = groupManager.getReaderGroup(readerGroupName);

        // Reading the events between the two streamCut, startStreamCut = streamCut0 and endStreamCut = streamCut1
        @Cleanup
        EventStreamReader<String> reader0 = clientFactory.createReader(readerName,
                readerGroupName,
                new UTF8StringSerializer(),
                ReaderConfig.builder().build());

        int readCount1 = readEvent(reader0, streamCut1Position / 30);
        reader0.close();
        assertEquals(readCount1, streamCut1Position / 30);

        StreamCut streamCut2 = batchClient.getNextStreamCut(streamCut1, 80L);
        long streamCut2Position = streamCut2.asImpl().getPositions().get(list.get(0)).longValue();
        assertTrue(270L <= streamCut2Position);
        long distanceBetweenTheStreamCut2 = streamCut2Position - streamCut1Position;
        log.info("Next stream cut2 {} stream cut position {} distance between the stream cut {}", streamCut2, streamCut2Position, distanceBetweenTheStreamCut2);

        ReaderGroupConfig readerGroupConfig2 = getReaderGroupConfig(streamCut1, streamCut2, stream);
        readerGroup.resetReaderGroup(readerGroupConfig2);

        // Reading the events between the two streamCut, startStreamCut = streamCut1 and endStreamCut = streamCut2
        reader0 = clientFactory.createReader(readerName,
                readerGroupName,
                new UTF8StringSerializer(),
                ReaderConfig.builder().build());

        int readCount2 = readEvent(reader0, distanceBetweenTheStreamCut2 / 30);
        reader0.close();
        assertEquals(readCount2, distanceBetweenTheStreamCut2 / 30);

        long approxDistanceToNextOffset = 350L;
        StreamCut streamCut3 = batchClient.getNextStreamCut(streamCut2, approxDistanceToNextOffset);
        long streamCut3Position = streamCut3.asImpl().getPositions().get(list.get(0)).longValue();
        assertEquals(300L, streamCut3Position);
        long distanceBetweenTheStreamCut3 = streamCut3Position - streamCut2Position;
        log.info("Next stream cut3 {} stream cut position {} distance between the stream cut {}", streamCut3, streamCut3Position, distanceBetweenTheStreamCut3);

        ReaderGroupConfig readerGroupConfig3 = getReaderGroupConfig(streamCut2, streamCut3, stream);
        readerGroup.resetReaderGroup(readerGroupConfig3);

        // Reading the events between the two streamCut, startStreamCut = streamCut2 and endStreamCut = streamCut3
        reader0 = clientFactory.createReader(readerName,
                readerGroupName,
                new UTF8StringSerializer(),
                ReaderConfig.builder().build());

        int readCount3 = readEvent(reader0, distanceBetweenTheStreamCut3 / 30);
        reader0.close();
        assertEquals(readCount3, distanceBetweenTheStreamCut3 / 30);

        StreamCut streamCut4 = batchClient.getNextStreamCut(streamCut3, approxDistanceToNextOffset);
        long streamCut4Position = streamCut4.asImpl().getPositions().get(list.get(0)).longValue();
        assertEquals(300L, streamCut4Position);
        log.info("Next stream cut4 {} stream cut position {} distance between the stream cut {}", streamCut4, streamCut4Position);

        ReaderGroupConfig readerGroupConfig4 = getReaderGroupConfig(streamCut3, streamCut4, stream);
        readerGroup.resetReaderGroup(readerGroupConfig4);

        // Reading the events between the two streamCut, startStreamCut = streamCut2 and endStreamCut = streamCut4
        reader0 = clientFactory.createReader(readerName,
                readerGroupName,
                new UTF8StringSerializer(),
                ReaderConfig.builder().build());
        assertEquals(0, readEvent(reader0, 0));
        reader0.close();

        //Scaling up begin
        Map<Double, Double> keyRanges = new HashMap<>();
        keyRanges.put(0.0, 0.5);
        keyRanges.put(0.5, 1.0);

        // Stream scaling up
        Boolean status = controller.scaleStream(stream,
                Collections.singletonList(0L),
                keyRanges,
                executor).getFuture().get();
        assertTrue(status);
        writeEvents(4, writer);

        StreamCut nextStreamCut5 = batchClient.getNextStreamCut(streamCut4, approxDistanceToNextOffset);
        log.info("Next stream cut5 {}", nextStreamCut5);

        Segment segment1 = Segment.fromScopedName(streamScope + "/" + streamName + "/1.#epoch.1");
        Segment segment2 = Segment.fromScopedName(streamScope + "/" + streamName + "/2.#epoch.1");
        log.info("Segment1 name {} and Segment2 name {}", segment1.getScopedName(), segment2.getScopedName());

        ArrayList<SegmentRange> segmentList1 = Lists.newArrayList(batchClient.getSegments(stream, StreamCut.UNBOUNDED, StreamCut.UNBOUNDED).getIterator());
        log.info("Segment List1 :{}", segmentList1);

        Map<Segment, Long> map = segmentList1.stream().collect(Collectors.toMap(SegmentRange::getSegment, value -> value.getEndOffset()));
        assertNotNull(nextStreamCut5);
        assertEquals(2, nextStreamCut5.asImpl().getPositions().size());
        assertTrue(nextStreamCut5.asImpl().getPositions().containsKey(segment1) &&
                nextStreamCut5.asImpl().getPositions().containsKey(segment2));
        assertTrue(map.get(segment1).longValue() <= nextStreamCut5.asImpl().getPositions().get(segment1).longValue());
        assertTrue(map.get(segment2).longValue() <= nextStreamCut5.asImpl().getPositions().get(segment2).longValue());

        //After scale up reading the event
        ReaderGroupConfig readerGroupConfig5 = getReaderGroupConfig(streamCut4, nextStreamCut5, stream);
        readerGroup.resetReaderGroup(readerGroupConfig5);

        reader0 = clientFactory.createReader(readerName,
                readerGroupName,
                new UTF8StringSerializer(),
                ReaderConfig.builder().build(), clock::get, clock::get);

        //Reader does not yet see the data because there has been no checkpoint
        assertNull(reader0.readNextEvent(1000).getEvent());
        readerGroup.initiateCheckpoint("cp");
        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        EventRead cpEvent = reader0.readNextEvent(1000);
        assertEquals("cp", cpEvent.getCheckpointName());
        assertEquals(4, readEvent(reader0, 4));
        reader0.close();
        //Scaling up end
    }

    /**
     * This test the getNextStreamCutWithScaleDownTest api with the current streamcut containing one segment0, and written the 5 events.
     * Created the streamCut0, streamCut1, by calling getNextStreamCut.
     * Read the events between the streamCut0 and streamCut1 and validated it.
     * Segment0 has scaled up (segment1 and segment2) with 5 events and validating the numbers of events written
     * segment1 and segment2 has scaled down (segment3).
     */
    @Test(timeout = 90000)
    public void getNextStreamCutWithScaleDownTest() throws SegmentTruncatedException, ExecutionException, InterruptedException {
        String streamName = "testStreamSegmentScaleDown";
        String streamScope = "testScopeSegmentScaleDown";
        String readerGroupName = "testReaderGroupSegmentScaleDown";
        String readerName = UUID.randomUUID().toString();
        AtomicLong clock = new AtomicLong();
        long approxDistanceToNextOffset = 180L;

        StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();

        final ClientConfig clientConfig = Utils.buildClientConfig(controllerURI);
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(clientConfig);
        ControllerImpl controller = new ControllerImpl(ControllerImplConfig.builder().clientConfig(clientConfig).build(),
                connectionFactory.getInternalExecutor());
        Stream stream = Stream.of(streamScope, streamName);

        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(streamScope, controller, connectionFactory);
        log.info("Invoking Writer test with Controller URI: {}", controllerURI);

        StreamManager streamManager = StreamManager.create(clientConfig);
        assertTrue("Creating Scope", streamManager.createScope(streamScope));
        assertTrue("Creating stream", streamManager.createStream(streamScope, streamName, streamConfiguration));

        @Cleanup
        BatchClientFactory batchClient = BatchClientFactory.withScope(streamScope, clientConfig);

        StreamCut streamCut0 = streamManager.fetchStreamInfo(streamScope, streamName).join().getHeadStreamCut();
        log.info("Initial stream streamCut0 {}", streamCut0);
        assertEquals(1, streamCut0.asImpl().getPositions().size());

        List<Segment> list = streamCut0.asImpl().getPositions().keySet().stream().collect(Collectors.toList());

        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new UTF8StringSerializer(),
                EventWriterConfig.builder().build());

        // write events to stream 30 * 5  = 150 bytes
        writeEvents(5, writer);
        writer.flush();

        //Requested next stream cut at a distance of 180 bytes, and getting the next approx offset as a response.
        StreamCut streamCut1 = batchClient.getNextStreamCut(streamCut0, approxDistanceToNextOffset);
        long streamCut1Position = streamCut1.asImpl().getPositions().get(list.get(0)).longValue();
        log.info("Next stream cut1 {} streamCut1 position {}", streamCut1, streamCut1Position);
        assertEquals(150L, streamCut1Position);

        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope(streamScope, controllerURI);
        ReaderGroupConfig readerGroupConfig1 = getReaderGroupConfig(streamCut0, streamCut1, stream);
        groupManager.createReaderGroup(readerGroupName, readerGroupConfig1);
        @Cleanup
        ReaderGroup readerGroup = groupManager.getReaderGroup(readerGroupName);

        //Reading the events between the two streamCut, startStreamCut = streamCut0 and endStreamCut = streamCut1
        @Cleanup
        EventStreamReader<String> reader0 = clientFactory.createReader(readerName,
                readerGroupName,
                new UTF8StringSerializer(),
                ReaderConfig.builder().build());

        int readCount1 = readEvent(reader0, streamCut1Position / 30);
        assertEquals(readCount1, streamCut1Position / 30);
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
        writeEvents(5, writer);

        StreamCut streamCut2 = batchClient.getNextStreamCut(streamCut1, approxDistanceToNextOffset);
        log.info("Next stream cut2 {}", streamCut2);
        assertEquals(2, streamCut2.asImpl().getPositions().size());

        ReaderGroupConfig readerGroupConfig2 = getReaderGroupConfig(streamCut1, streamCut2, stream);
        readerGroup.resetReaderGroup(readerGroupConfig2);

        //After scale up reading the event
        reader0 = clientFactory.createReader(readerName,
                readerGroupName,
                new UTF8StringSerializer(),
                ReaderConfig.builder().build(), clock::get, clock::get);

        //Reader does not yet see the data because there has been no checkpoint
        assertNull(reader0.readNextEvent(1000).getEvent());
        readerGroup.initiateCheckpoint("cp1");
        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        EventRead cpEvent1 = reader0.readNextEvent(1000);
        assertEquals("cp1", cpEvent1.getCheckpointName());

        assertEquals(5, readEvent(reader0, 5));
        reader0.close();

        Segment segment1 = Segment.fromScopedName(streamScope + "/" + streamName + "/1.#epoch.1");
        Segment segment2 = Segment.fromScopedName(streamScope + "/" + streamName + "/2.#epoch.1");
        log.info("Segment1 name {} and Segment2 name {}", segment1.getScopedName(), segment2.getScopedName());

        assertNotNull(streamCut2);
        assertEquals(2, streamCut2.asImpl().getPositions().size());
        assertTrue(streamCut2.asImpl().getPositions().containsKey(segment1) &&
                streamCut2.asImpl().getPositions().containsKey(segment2));
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
        writeEvents(5, writer);

        StreamCut streamCut3 = batchClient.getNextStreamCut(streamCut2, approxDistanceToNextOffset);
        log.info("Next stream cut3 {}", streamCut3);

        Segment segment3 = Segment.fromScopedName(streamScope + "/" + streamName + "/3.#epoch.2");
        log.info("segment3 name :{}", segment3.getScopedName());

        ReaderGroupConfig readerGroupConfig3 = getReaderGroupConfig(streamCut2, streamCut3, stream);
        readerGroup.resetReaderGroup(readerGroupConfig3);

        //After scale down reading the event
        reader0 = clientFactory.createReader(readerName,
                readerGroupName,
                new UTF8StringSerializer(),
                ReaderConfig.builder().build(), clock::get, clock::get);

        assertNull(reader0.readNextEvent(1000).getEvent());
        readerGroup.initiateCheckpoint("cp2");
        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        EventRead cpEvent2 = reader0.readNextEvent(1000);
        assertEquals("cp2", cpEvent2.getCheckpointName());

        assertEquals(5, readEvent(reader0, 5));
        reader0.close();

        assertNotNull(streamCut3);
        assertEquals(1, streamCut3.asImpl().getPositions().size());
        assertEquals(150L, streamCut3.asImpl().getPositions().get(segment3).longValue());
    }

    private void writeEvents(int numberOfEvents, EventStreamWriter<String> writer) {
        Supplier<String> routingKeyGenerator = () -> String.valueOf(random.nextInt());
        IntStream.range(0, numberOfEvents).forEach(v -> writer.writeEvent(routingKeyGenerator.get(), DATA_OF_SIZE_30).join());
    }

    private static ReaderGroupConfig getReaderGroupConfig(StreamCut startStreamCut, StreamCut endStreamCut, Stream stream) {
        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .disableAutomaticCheckpoints()
                .stream(stream, startStreamCut, endStreamCut)
                .build();
        return readerGroupConfig;
    }

    private static int readEvent(EventStreamReader<String> reader, long readExpectedCount) {
        int readCount = 0;
        EventRead<String> event = null;
        do {
            event = reader.readNextEvent(10000);
            log.debug("Read event: {}.", event.getEvent());
            if (event.getEvent() != null) {
                readCount++;
            }
        } while (event.getEvent() != null && readCount <= readExpectedCount);
        return readCount;
    }
}
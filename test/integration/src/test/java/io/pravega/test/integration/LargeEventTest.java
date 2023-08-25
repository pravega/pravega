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

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.connection.impl.ClientConnection;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ByteBufferSerializer;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.stream.impl.StreamSegments;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.IndexAppendProcessor;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.NameUtils;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.test.common.LeakDetectorTestSuite;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.utils.ControllerWrapper;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.RandomUtils;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Slf4j
public class LargeEventTest extends LeakDetectorTestSuite {

    private static final int NUM_WRITERS = 2;
    private static final int NUM_READERS = 1;
    private static final int LARGE_EVENT_SIZE = Serializer.MAX_EVENT_SIZE * 3;
    private static final int TINY_EVENT_SIZE = 8;
    private static final int CLOSE_WRITE_COUNT = 2;
    private static final String SCOPE_NAME = "scope";

    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int controllerPort = TestUtils.getAvailableListenPort();

    private TableStore tableStore;
    private StreamSegmentStore store;
    private AtomicLong eventReadCount;
    private AtomicLong eventWriteCount;
    private AtomicBoolean stopReadFlag;
    private ServiceBuilder serviceBuilder;
    private TestingServer zkTestServer = null;
    private PravegaConnectionListener server = null;
    private ControllerWrapper controllerWrapper = null;
    private Controller controller = null;
    private ScheduledExecutorService writerPool;
    private ScheduledExecutorService readerPool;
    private ConcurrentLinkedQueue<ByteBuffer> eventsReadFromPravega;
    private ConcurrentHashMap<Integer, List<ByteBuffer>> eventsWrittenToPravega;

    @Before
    public void setup() throws Exception {
        String serviceHost = "localhost";
        int containerCount = 1;

        eventsReadFromPravega = new ConcurrentLinkedQueue<>();
        eventsWrittenToPravega = new ConcurrentHashMap<>();
        eventReadCount = new AtomicLong(); // used by readers to maintain a count of events.
        eventWriteCount = new AtomicLong();
        stopReadFlag = new AtomicBoolean(false);

        // 1. Start ZK
        this.zkTestServer = new TestingServerStarter().start();
        // 2. Start Pravega SegmentStore service.
        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        store = serviceBuilder.createStreamSegmentService();
        tableStore = serviceBuilder.createTableStoreService();
        // Start up server.
        this.server = new PravegaConnectionListener(false, servicePort, store, tableStore, serviceBuilder.getLowPriorityExecutor(),
                new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));
        this.server.startListening();
        // 3. Start Pravega Controller service
        this.controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), false,
                controllerPort, serviceHost, servicePort, containerCount);
        this.controllerWrapper.awaitRunning();
        this.controller = controllerWrapper.getController();
        this.writerPool = ExecutorServiceHelpers.newScheduledThreadPool(NUM_WRITERS, "WriterPool");
        this.readerPool = ExecutorServiceHelpers.newScheduledThreadPool(NUM_READERS, "ReaderPool");
    }

    @After
    public void tearDown() throws Exception {

        if (this.controllerWrapper != null) {
            this.controllerWrapper.close();
            this.controllerWrapper = null;
        }
        if (this.controller != null) {
            this.controller.close();
            this.controller = null;
        }
        if (this.server != null) {
            this.server.close();
            this.server = null;
        }
        if (this.serviceBuilder != null) {
            this.serviceBuilder.close();
            this.serviceBuilder = null;
        }
        if (this.zkTestServer != null) {
            this.zkTestServer.close();
            this.zkTestServer = null;
        }

        if (this.writerPool != null) {
            ExecutorServiceHelpers.shutdown(this.writerPool);
            this.writerPool = null;
        }

        if (this.readerPool != null) {
            ExecutorServiceHelpers.shutdown(this.readerPool);
            this.readerPool = null;
        }
    }


    @Test(timeout = 60000)
    public void readWriteTest() throws ExecutionException, InterruptedException {
        String readerGroupName = "testLargeEventReaderGroup";
        String streamName = "ReadWrite";
        StreamConfiguration config = getStreamConfiguration(NUM_READERS);
        createScopeStream(SCOPE_NAME, streamName, config);
        int events = 1;
        Map<Integer, List<ByteBuffer>> data = generateEventData(NUM_WRITERS, events * 0, events, LARGE_EVENT_SIZE);

        readWriteCycle(streamName, readerGroupName, data);
        val si = store.getStreamSegmentInfo(NameUtils.getIndexSegmentName(NameUtils.getQualifiedStreamSegmentName(SCOPE_NAME, streamName, 0)), Duration.ofMinutes(1)).join();
        val segmentType = SegmentType.fromAttributes(si.getAttributes());
        assertFalse(segmentType.isInternal() || segmentType.isCritical() || segmentType.isSystem() || segmentType.isTableSegment());
        assertEquals(SegmentType.STREAM_SEGMENT, segmentType);

        val attributes = si.getAttributes();
        val length = si.getLength();
        assertTrue((long) attributes.get(Attributes.EVENT_COUNT) > 0);
        assertTrue( length > 0);
        validateCleanUp(streamName);

    }

    @Test(timeout = 60000)
    public void testNormalThenLargeEvent() throws ExecutionException, InterruptedException {

        String streamName = "NormalEventLargeEvent";
        String readerGroupName = "testNormalThenLargeEvent";

        StreamConfiguration config = getStreamConfiguration(NUM_READERS);
        createScopeStream(SCOPE_NAME, streamName, config);

        int events = 1;
        AtomicInteger generation = new AtomicInteger(0);
        // Normal Event Write/Read.
        merge(eventsWrittenToPravega, generateEventData(NUM_WRITERS,  events * generation.getAndIncrement(), events, LARGE_EVENT_SIZE));

        log.info("Writing {} new events.", eventsWrittenToPravega.size());
        eventsReadFromPravega = readWriteCycle(streamName, readerGroupName, eventsWrittenToPravega);
        log.info("Read back {} events.", eventsReadFromPravega.size());
        validateEventReads(eventsReadFromPravega, eventsWrittenToPravega);

        // Large Event Write/Read.
        Map<Integer, List<ByteBuffer>> data = generateEventData(NUM_WRITERS, events * generation.getAndIncrement(), events, TINY_EVENT_SIZE);
        merge(eventsWrittenToPravega, data);

        log.info("Writing {} new events.", eventsWrittenToPravega.size());
        eventsReadFromPravega = readWriteCycle(streamName, readerGroupName, data);

        log.info("Read back {} events.", eventsReadFromPravega.size());
        validateEventReads(eventsReadFromPravega, eventsWrittenToPravega);

        validateCleanUp(streamName);
    }

    @Test(timeout = 60000)
    public void testSingleWriterMixedEvents() throws ExecutionException, InterruptedException {
        String streamName = "SingleWriterMixedEvents";
        String readerGroupName = "testSingleWriterMixedEvents";

        int writers = 1;
        AtomicInteger generation = new AtomicInteger(0);
        StreamConfiguration config = getStreamConfiguration(NUM_READERS);
        createScopeStream(SCOPE_NAME, streamName, config);

        int events = 2;
        // Normal Event Write/Read.
        merge(eventsWrittenToPravega, generateEventData(writers,  events * generation.getAndIncrement(), events, TINY_EVENT_SIZE));
        // Add two Large Events
        merge(eventsWrittenToPravega, generateEventData(writers, events * generation.getAndIncrement(), events, LARGE_EVENT_SIZE));
        // Add two normal events.
        merge(eventsWrittenToPravega, generateEventData(writers, events * generation.getAndIncrement(), events, TINY_EVENT_SIZE));

        log.info("Writing {} new events.", eventsWrittenToPravega.size());
        eventsReadFromPravega = readWriteCycle(streamName, readerGroupName, eventsWrittenToPravega);
        log.info("Read back {} events.", eventsReadFromPravega.size());
        validateEventReads(eventsReadFromPravega, eventsWrittenToPravega);

        validateCleanUp(streamName);
    }

    @Test(timeout = 60000)
    public void testReadWriteWithSegmentStoreRestart() throws ExecutionException, InterruptedException {
        String readerGroupName = "testLargeEventFailoverReaderGroup";
        String streamName = "SegmentStoreRestart";
        StreamConfiguration config = getStreamConfiguration(NUM_READERS);
        createScopeStream(SCOPE_NAME, streamName, config);

        int events = 1;
        AtomicInteger generation = new AtomicInteger(0);
        merge(eventsWrittenToPravega, generateEventData(NUM_WRITERS, events * generation.getAndIncrement(), events, LARGE_EVENT_SIZE));

        eventsReadFromPravega = readWriteCycle(streamName, readerGroupName, eventsWrittenToPravega);
        validateEventReads(eventsReadFromPravega, eventsWrittenToPravega);

        // Passing in this restart callback will override the default behavior of closing the connection.
        Runnable restart = () -> {
            // Reset the server, in effect clearing the AppendProcessor and PravegaRequestProcessor.
            this.server.close();
            this.server = new PravegaConnectionListener(false, servicePort, store, tableStore, serviceBuilder.getLowPriorityExecutor(),
                    new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));
            this.server.startListening();
        };
        restart.run();

        Map<Integer, List<ByteBuffer>> data = generateEventData(NUM_WRITERS, events * generation.getAndIncrement(), events, LARGE_EVENT_SIZE);
        merge(eventsWrittenToPravega, data);

        eventsReadFromPravega = readWriteCycle(streamName, readerGroupName, data);
        validateEventReads(eventsReadFromPravega, eventsWrittenToPravega);

        // Clear objects necessary for read-write validation.
        stopReadFlag = new AtomicBoolean(false);
        eventsReadFromPravega.clear();
        eventReadCount.set(0);
        // Generate new data.
        data = generateEventData(NUM_WRITERS, events * generation.getAndIncrement(), events, LARGE_EVENT_SIZE);
        merge(eventsWrittenToPravega, data);

        AtomicInteger sendCount = new AtomicInteger(0);
        Supplier<Boolean> predicate = () -> sendCount.getAndIncrement() == CLOSE_WRITE_COUNT;

        // Now try the restart *during* a large event write.
        AtomicReference<Boolean> latch = new AtomicReference<>(true);
        try (ConnectionExporter connectionFactory = new ConnectionExporter(ClientConfig.builder().build(), latch, restart, predicate);
             ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE_NAME, controller, connectionFactory);
             ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(SCOPE_NAME, controller, clientFactory)) {
            // Start writing events to the stream.
            val writers = createEventWriters(streamName, NUM_WRITERS, clientFactory,  data);
            Futures.allOf(writers).get();
            // Create a ReaderGroup.
            createReaderGroup(readerGroupName, readerGroupManager, streamName);
            // Create Readers.
            val readers = createEventReaders(NUM_READERS, clientFactory, readerGroupName, eventsReadFromPravega);
            stopReadFlag.set(true);

            Futures.allOf(readers).get();

            readerGroupManager.deleteReaderGroup(readerGroupName);
        }
        validateEventReads(eventsReadFromPravega, eventsWrittenToPravega);

        validateCleanUp(streamName);
    }

    @Test(timeout = 60000)
    public void testReadWriteWithConnectionReconnect() throws ExecutionException, InterruptedException, TimeoutException {
        String readerGroupName = "testLargeEventReconnectReaderGroup";
        String streamName = "ConnectionReconnect";
        StreamConfiguration config = getStreamConfiguration(NUM_READERS);

        createScopeStream(SCOPE_NAME, streamName, config);

        int numEvents = 1;
        Map<Integer, List<ByteBuffer>> data = generateEventData(NUM_WRITERS, 0, numEvents, LARGE_EVENT_SIZE);
        merge(eventsWrittenToPravega, data);
        Queue<ByteBuffer> reads = new ConcurrentLinkedQueue<>();

        // Close the connection after two `send` calls.
        AtomicInteger sendCount = new AtomicInteger(0);
        Supplier<Boolean> predicate = () -> sendCount.getAndIncrement() == CLOSE_WRITE_COUNT;

        AtomicReference<Boolean> latch = new AtomicReference<>(true);
        try (ConnectionExporter connectionFactory = new ConnectionExporter(ClientConfig.builder().build(), latch, null, predicate);
             ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE_NAME, controller, connectionFactory);
             ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(SCOPE_NAME, controller, clientFactory)) {
            // Start writing events to the stream.
            val writers = createEventWriters(streamName, NUM_WRITERS, clientFactory,  eventsWrittenToPravega);
            // Create a ReaderGroup.
            createReaderGroup(readerGroupName, readerGroupManager, streamName);
            // Create Readers.
            val readers = createEventReaders(NUM_READERS, clientFactory, readerGroupName, reads);
            Futures.allOf(writers).get();
            stopReadFlag.set(true);

            Futures.allOf(readers).get();

            readerGroupManager.deleteReaderGroup(readerGroupName);
        }

        validateCleanUp(streamName);
        validateEventReads(reads, eventsWrittenToPravega);
    }

    /**
     * Tests that if a Stream is sealed no data will continue to be produced by the {@link io.pravega.client.stream.impl.LargeEventWriter}
     */
    @Test(timeout = 60000)
    public void testReadWriteStreamSeal() throws ExecutionException, InterruptedException {
        String readerGroupName = "testLargeEventStreamSealReaderGroup";
        String streamName = "StreamSeal";
        StreamConfiguration config = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.byDataRate(500, 2, 1))
                .retentionPolicy(RetentionPolicy.bySizeBytes(Long.MAX_VALUE))
                .build();

        createScopeStream(SCOPE_NAME, streamName, config);

        int events = 1;
        AtomicInteger generation = new AtomicInteger(0);
        Map<Integer, List<ByteBuffer>> data = generateEventData(NUM_WRITERS, events * generation.getAndIncrement(), events, TINY_EVENT_SIZE);
        merge(eventsWrittenToPravega, data);

        Queue<ByteBuffer> reads = new ConcurrentLinkedQueue<>();

        // Create some data and create the stream.
        reads = readWriteCycle(streamName, readerGroupName, eventsWrittenToPravega);
        validateEventReads(reads, eventsWrittenToPravega);

        // Wait for the Stream to be sealed.
        store.sealStreamSegment(NameUtils.getQualifiedStreamSegmentName(SCOPE_NAME, streamName, 0), Duration.ofMillis(1000)).join();
        // Try another write -- should fail.
        data = generateEventData(NUM_WRITERS, events * generation.getAndIncrement(), events, LARGE_EVENT_SIZE);
        reads = readWriteCycle(streamName, readerGroupName, data);
        // Only the first data set should have succeeded.
        validateEventReads(reads, eventsWrittenToPravega);

        // Attempt to write to it again.
        validateCleanUp(streamName);
    }

    /**
     * Tests that if a Stream is sealed no data will continue to be produced by the {@link io.pravega.client.stream.impl.LargeEventWriter}
     */
    @Test(timeout = 60000)
    public void testReadWriteScaleStreamSeal() throws ExecutionException, InterruptedException, TimeoutException {
        String readerGroupName = "testLargeEventScaleStreamSealReaderGroup";
        String streamName = "ScaleStreamSeal";
        StreamConfiguration config = StreamConfiguration.builder()
                .retentionPolicy(RetentionPolicy.bySizeBytes(Long.MAX_VALUE))
                .build();

        createScopeStream(SCOPE_NAME, streamName, config);

        AtomicInteger generation = new AtomicInteger(0);
        // Creates some data to write.
        int events = 1;
        Map<Integer, List<ByteBuffer>> data = generateEventData(NUM_WRITERS, events * generation.getAndIncrement(), events, LARGE_EVENT_SIZE);
        merge(eventsWrittenToPravega, data);

        // Perform a basic read-write cycle.
        Queue<ByteBuffer> reads = readWriteCycle(streamName, readerGroupName, eventsWrittenToPravega);
        validateEventReads(reads, eventsWrittenToPravega);

        // Define the scale operation.
        Runnable scale = () -> {
            Stream stream = new StreamImpl(SCOPE_NAME, streamName);
            try {
                StreamSegments segments = controller.getCurrentSegments(SCOPE_NAME, streamName).get();
                List<Long> ids = segments.getSegments()
                        .stream()
                        .map(Segment::getSegmentId)
                        .collect(Collectors.toList());
                controller.startScale(stream, ids, Map.of(0.0, 0.5, 0.5, 1.0)).get();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        };

        // Reset reads.
        eventReadCount.set(0);
        // Try to scale the segment after two `send` calls.
        AtomicInteger sendCount = new AtomicInteger(0);
        Supplier<Boolean> predicate = () -> sendCount.getAndIncrement() == CLOSE_WRITE_COUNT;
        AtomicReference<Boolean> latch = new AtomicReference<>(true);
        try (ConnectionExporter connectionFactory = new ConnectionExporter(ClientConfig.builder().build(), latch, scale, predicate);
             ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE_NAME, controller, connectionFactory);
             ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(SCOPE_NAME, controller, clientFactory)) {
            // Next set of writes.
            data = generateEventData(NUM_WRITERS, events * generation.getAndIncrement(), events, LARGE_EVENT_SIZE);
            merge(eventsWrittenToPravega, data);
            // Start writing events to the stream.

            val writers = createEventWriters(streamName, NUM_WRITERS, clientFactory,  data);
            Futures.allOf(writers).get();

            // Wait for the scale event.
            TestUtils.await(() -> !latch.get(), 200, 2000);
            // Create a ReaderGroup.
            createReaderGroup(readerGroupName, readerGroupManager, streamName);
            // Create Readers.
            val readers = createEventReaders(NUM_READERS, clientFactory, readerGroupName, eventsReadFromPravega);
            stopReadFlag.set(true);

            Futures.allOf(readers).get();

            log.info("Deleting ReaderGroup: {}", readerGroupName);
            readerGroupManager.deleteReaderGroup(readerGroupName);
        }

        StreamSegments segments = controller.getCurrentSegments(SCOPE_NAME, streamName).get();
        // Make sure that the scale event has happened.
        Assert.assertEquals("Expected 2 StreamSegments.", 2, segments.getSegments().size());

        // This time there are successor segments, so the data should have been accepted.
        validateEventReads(eventsReadFromPravega, eventsWrittenToPravega);
    }

    private ConcurrentLinkedQueue<ByteBuffer> readWriteCycle(String streamName, String readerGroupName, Map<Integer, List<ByteBuffer>> writes) throws ExecutionException, InterruptedException {
        // Each read-cycle should clear these objects.
        stopReadFlag.set(false);
        // Reads should be clear, but not writes.
        eventReadCount.set(0);
        // The reads should return all events written, and not just the to be written events (writes).
        ConcurrentLinkedQueue<ByteBuffer> reads = new ConcurrentLinkedQueue<>();

        try (ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder().build());
             ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE_NAME, controller, connectionFactory);
             ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(SCOPE_NAME, controller, clientFactory)) {
            // Start writing events to the stream.
            val writers = createEventWriters(streamName, writes.size(), clientFactory, writes);
            // Create a ReaderGroup.
            createReaderGroup(readerGroupName, readerGroupManager, streamName);
            // Create Readers.
            val readers = createEventReaders(NUM_READERS, clientFactory, readerGroupName, reads);
            Futures.allOf(writers).get();
            stopReadFlag.set(true);
            Futures.allOf(readers).get();
            log.info("Deleting ReaderGroup: {}", readerGroupName);
            readerGroupManager.deleteReaderGroup(readerGroupName);
        }

        return reads;
    }

    private CompletableFuture<Void> startNewWriter(final String routingKey,
                                                   final String streamName,
                                                   final AtomicLong writeCount,
                                                   final List<ByteBuffer> data,
                                                   final EventStreamClientFactory clientFactory) {
        return CompletableFuture.runAsync(() -> {
            @Cleanup
            final EventStreamWriter<ByteBuffer> writer = clientFactory.createEventWriter(streamName,
                    new ByteBufferSerializer(),
                    EventWriterConfig.builder().enableLargeEvents(true).build());
            for (ByteBuffer buf : data) {
                log.debug("Writing LargeEvent: [{}/{}]", buf.get(0), buf.get(1));
                writer.writeEvent(routingKey, buf).thenRun(() -> writeCount.incrementAndGet());
            }
            log.info("Closing writer {}", writer);
            writer.close();
        }, writerPool);
    }

    private StreamConfiguration getStreamConfiguration(int readers) {
        ScalingPolicy scalingPolicy = ScalingPolicy.fixed(readers);
        return StreamConfiguration.builder()
                .scalingPolicy(scalingPolicy)
            .retentionPolicy(RetentionPolicy.bySizeBytes(Long.MAX_VALUE))
            .build();
    }

    private void createScopeStream(String scope, String stream, StreamConfiguration config) {
        ClientConfig clientConfig = ClientConfig.builder().build();
        try (ConnectionPool cp = new ConnectionPoolImpl(clientConfig, new SocketConnectionFactoryImpl(clientConfig));
             StreamManager streamManager = new StreamManagerImpl(controller, cp)) {
            //create a scope
            Boolean createScopeStatus = streamManager.createScope(scope);
            log.info("Create Scope status {}.", createScopeStatus);
            //create a stream
            Boolean createStreamStatus = streamManager.createStream(scope, stream, config);
            log.info("Create Stream status {}.", createStreamStatus);
        }
    }

    // Offset is the number to start the event id at, i.e. the number of events already generated (so we don't generate
    // conflicting events).
    private Map<Integer, List<ByteBuffer>> generateEventData(int writers, int offset, int events, int eventSize) {
        Map<Integer, List<ByteBuffer>> data = new HashMap<>();
        for (int i = 1; i <= writers; i++) {
            List<ByteBuffer> buffs = new ArrayList<>();
            for (int j = offset; j < events + offset; j++) {
                byte[] bytes = RandomUtils.nextBytes(eventSize);
                // Make the first byte the writerId for logging purposes.
                bytes[0] = (byte) i;
                // Make the second byte the j'th event written by that writer.
                bytes[1] = (byte) j;
                ByteBuffer buf = ByteBuffer.wrap(bytes);
                buffs.add(buf);
            }
            data.put(i, buffs);
        }
        return data;
    }

    private List<CompletableFuture<Void>> createEventWriters(String streamName, int writers, ClientFactoryImpl factory, Map<Integer, List<ByteBuffer>> data) {
        log.info("Creating {} Writers.", writers);
        List<CompletableFuture<Void>> writerList = new ArrayList<>();
        for (val entry : data.entrySet()) {
            log.info("Starting Writer {}", entry.getKey());
            String routingKey = String.format("LargeEventWriter-%d", entry.getKey());
            writerList.add(startNewWriter(routingKey, streamName, eventWriteCount, entry.getValue(), factory));
        }
        return writerList;
    }

    private void createReaderGroup(String group, ReaderGroupManager manager, String streamName) {
        log.info("Creating ReaderGroup : {}", group);
        manager.createReaderGroup(group, ReaderGroupConfig.builder().stream(Stream.of(SCOPE_NAME, streamName)).build());
        log.info("ReaderGroup Name : {} ", manager.getReaderGroup(group).getGroupName());
        log.info("ReaderGroup Scope : {}", manager.getReaderGroup(group).getScope());
    }

    private List<CompletableFuture<Void>> createEventReaders(int readers, ClientFactoryImpl factory, String readerGroupName, Queue<ByteBuffer> reads) {
        log.info("Creating {} Readers.", readers);
        List<CompletableFuture<Void>> readerList = new ArrayList<>();
        for (int i = 1; i <= readers; i++) {
            String readerId = String.format("LargeEventReader-%d", i);
            readerList.add(startNewReader(readerId,
                    factory,
                    readerGroupName,
                    reads,
                    eventWriteCount,
                    eventReadCount,
                    stopReadFlag));
        }
        return readerList;
    }

    public void validateCleanUp(String streamName) throws ExecutionException, InterruptedException {
        // Seal the stream
        CompletableFuture<Boolean> sealStreamStatus = controller.sealStream(SCOPE_NAME, streamName);
        log.info("Sealing stream {}", streamName);
        assertTrue(sealStreamStatus.get());

        // Delete the stream.
        CompletableFuture<Boolean> deleteStreamStatus = controller.deleteStream(SCOPE_NAME, streamName);
        log.info("Deleting stream '{}'", streamName);
        assertTrue(deleteStreamStatus.get());
        // Delete the scope.
        CompletableFuture<Boolean> deleteScopeStatus = controller.deleteScope(SCOPE_NAME);
        log.info("Deleting scope '{}'", SCOPE_NAME);
        assertTrue(deleteScopeStatus.get());
    }

    private CompletableFuture<Void> startNewReader(final String readerId, final EventStreamClientFactory clientFactory, final String
            readerGroupName, final Queue<ByteBuffer> readResult, final AtomicLong writeCount, final
                                                   AtomicLong readCount, final AtomicBoolean exitFlag) {
        return CompletableFuture.runAsync(() -> {
            @Cleanup
            final EventStreamReader<ByteBuffer> reader = clientFactory.createReader(readerId,
                    readerGroupName,
                    new ByteBufferSerializer(),
                    ReaderConfig.builder().build());
            log.info("Starting Reader: {}", readerId);
            log.info("Read Count: {}, Write Count: {}", readCount.get(), writeCount.get());
            while (!(exitFlag.get() && readCount.get() == writeCount.get())) {
                final ByteBuffer event = reader.readNextEvent(SECONDS.toMillis(2)).getEvent();
                // Update if event read is not null.
                if (event != null) {
                    readResult.add(event);
                    readCount.incrementAndGet();
                }
            }
            log.info("Closing Reader : {}", reader);
            reader.close();
        }, readerPool);
    }

    void validateEventReads(Queue<ByteBuffer> eventsRead, Map<Integer, List<ByteBuffer>> eventsWritten) {
        int writesSize = 0;
        for (val entry : eventsWritten.entrySet()) {
            writesSize += entry.getValue().size();
        }
        // Expect each writer's first event read to be zero.
        Map<Integer, Integer> writerEventNumber = new HashMap<>();
        for (ByteBuffer read : eventsRead) {
            // Get first byte to determine which writer the event came from.
            int writerId = read.get(0);
            assertNotNull(String.format("Unexpected writerId (%d) read from event.", writerId), eventsWritten.get(writerId));
            List<ByteBuffer> events = eventsWritten.get(writerId);
            // The expected event number.
            int writerEvent = writerEventNumber.getOrDefault(writerId, 0);
            ByteBuffer write = events.get(writerEvent);
            log.debug("Expected Event: [{}/{}], Received Event: [{}/{}]", read.get(0), read.get(1), write.get(0), write.get(1));
            // Make sure the event ordering is valid.
            assertEquals("Received out of order write events.", writerEvent, write.get(1));
            // Increment the next expected event number.
            writerEventNumber.put(writerId, writerEvent + 1);
            // Validate ordering of bytes>
            int bytesRead = read.array().length;
            int bytesWritten = write.array().length;
            assertEquals(String.format("Mismatch of bytes read (%d) vs. bytes written (%d).", bytesRead, bytesWritten), bytesRead, bytesWritten);
            for (int j = 0; j < bytesRead; j++) {
                byte rb = read.get(j);
                byte wb = write.get(j);
                // Avoid cost of String creation on matching bytes.
                if (rb != wb) {
                    Assert.fail(String.format("Byte mismatch at index %d (read: %d, written: %d).", j, rb, wb));
                }
            }
        }
        assertEquals("Mismatched number of events written vs. read.", writesSize, eventsRead.size());
        log.info("Read/Write validation completed: {} events read matched {} events written.", eventsRead.size(), writesSize);
    }

    void merge(Map<Integer, List<ByteBuffer>> sink, Map<Integer, List<ByteBuffer>> source) {
        for (val entry : source.entrySet()) {
            if (!sink.containsKey(entry.getKey())) {
                sink.put(entry.getKey(), new ArrayList<>());
            }
            sink.get(entry.getKey()).addAll(entry.getValue());
        }
    }


    private static class ConnectionExporter extends SocketConnectionFactoryImpl {
        @Getter
        public ConnectionSendIntercept connection;
        private AtomicReference<Boolean> latch;
        private Runnable callback;
        private Supplier<Boolean> predicate;

        // The latch ensures that the callback will only be ran once during the lifetime of this object.

        ConnectionExporter(ClientConfig config, AtomicReference<Boolean> latch, Runnable callback, Supplier<Boolean> predicate) {
            super(config);
            this.latch = latch;
            this.callback = callback;
            this.predicate = predicate;
        }

        ConnectionExporter(ClientConfig config, AtomicReference<Boolean> latch) {
            this(config, latch, null, null);
        }

        @Override
        public CompletableFuture<ClientConnection> establishConnection(PravegaNodeUri endpoint, ReplyProcessor rp) {
            return super.establishConnection(endpoint, rp).thenApply(connection -> {
                if (callback == null) {
                    ClientConnection c = connection;
                    callback = () -> c.close();
                }
                return new ConnectionSendIntercept(connection, latch, callback, predicate);
            });
        }

        @Override
        public void close() {
            super.close();
            if (this.connection != null) {
                this.connection.close();
            }
        }

    }

    private static class ConnectionSendIntercept implements ClientConnection {

        private ClientConnection connection;
        private AtomicReference<Boolean> close;
        private Runnable callback;
        private Supplier<Boolean> predicate;

        ConnectionSendIntercept(ClientConnection connection, AtomicReference<Boolean> close, Runnable callback, Supplier<Boolean> predicate) {
            this.connection = connection;
            this.callback = callback;
            this.close = close;
            this.predicate = predicate;
        }

        @Override
        public void send(WireCommand cmd) throws ConnectionFailedException {
            if (this.close.get() && cmd instanceof WireCommands.ConditionalBlockEnd) {
                if (predicate != null && this.predicate.get() && this.close.compareAndSet(true, false)) {
                    if (callback != null) {
                        callback.run();
                    }
                }
                connection.send(cmd);
            } else {
                connection.send(cmd);
            }
        }

        @Override
        public void send(Append append) throws ConnectionFailedException {
            connection.send(append);
        }

        @Override
        public void sendAsync(List<Append> appends, CompletedCallback callback) {
            connection.sendAsync(appends, callback);
        }

        @Override
        public void close() {
            connection.close();
        }

        @Override
        public PravegaNodeUri getLocation() {
            return null;
        }
    }
}

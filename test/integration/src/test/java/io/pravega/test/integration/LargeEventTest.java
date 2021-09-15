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
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.test.common.LeakDetectorTestSuite;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Slf4j
public class LargeEventTest extends LeakDetectorTestSuite {

    private AtomicLong eventReadCount;
    private AtomicLong eventWriteCount;
    private AtomicBoolean stopReadFlag;
    private static final int NUM_WRITERS = 2;
    private static final int NUM_READERS = 1;
    private ServiceBuilder serviceBuilder;
    private TestingServer zkTestServer = null;
    private PravegaConnectionListener server = null;
    private ControllerWrapper controllerWrapper = null;
    private Controller controller = null;
    private ScheduledExecutorService writerPool;
    private ScheduledExecutorService readerPool;
    private ConcurrentLinkedQueue<ByteBuffer> eventsReadFromPravega;
    private ConcurrentHashMap<Integer, ByteBuffer> eventsWrittenToPravega;
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int controllerPort = TestUtils.getAvailableListenPort();
    TableStore tableStore;
    StreamSegmentStore store;

    private static final String STREAM_NAME = "stream";

    private static final String SCOPE_NAME = "scope";

    @Before
    public void setup() throws Exception {
        resetReadWriteObjects();
        String serviceHost = "localhost";
        int containerCount = 4;

        // 1. Start ZK
        this.zkTestServer = new TestingServerStarter().start();
        // 2. Start Pravega SegmentStore service.
        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        store = serviceBuilder.createStreamSegmentService();
        tableStore = serviceBuilder.createTableStoreService();
        // Start up server.
        this.server = new PravegaConnectionListener(false, servicePort, store, tableStore, serviceBuilder.getLowPriorityExecutor());
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


    private StreamConfiguration getStreamConfiguraton(int readers) {
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

    private void resetReadWriteObjects() {
        eventsReadFromPravega = new ConcurrentLinkedQueue<>();
        eventsWrittenToPravega = new ConcurrentHashMap<>();
        eventReadCount = new AtomicLong(); // used by readers to maintain a count of events.
        eventWriteCount = new AtomicLong();
        stopReadFlag = new AtomicBoolean(false);
    }

    private void generateWriteEventData(int writers, int eventSize) {
        for (int i = 0; i < writers; i++) {
            byte[] bytes = RandomUtils.nextBytes(eventSize);
            // Make the first byte the writerId for logging purposes.
            bytes[0] = (byte) i;
            ByteBuffer buf = ByteBuffer.wrap(bytes);
            eventsWrittenToPravega.put(i, buf);
        }
    }

    private List<CompletableFuture<Void>> createEventWriters(String streamName, int writers, ClientFactoryImpl factory) {
        log.info("Creating {} Writers.", writers);
        List<CompletableFuture<Void>> writerList = new ArrayList<>();
        for (int i = 0; i < writers; i++) {
            log.info("Starting Writer {}", i);
            String routingKey = String.format("LargeEventWriter-%d", i);
            writerList.add(startNewWriter(routingKey, streamName, eventWriteCount, eventsWrittenToPravega.get(i), factory));
        }
        return writerList;
    }

    // For now, limit writers to one.
    private List<CompletableFuture<Void>> createReconnectingEventWriters(String streamName, int writers, ClientFactoryImpl factory, ConnectionExporter exporter) {
        log.info("Creating {} Writers.", writers);
        List<CompletableFuture<Void>> writerList = new ArrayList<>();
        for (int i = 0; i < writers; i++) {
            log.info("Starting Writer {}", i);
            String routingKey = String.format("LargeEventWriter-%d", i);
            int writerId = i;
            Runnable restart = () -> {
                log.info("Closing writer {} ClientConnection.", writerId);
                exporter.getConnection().close();
            };
            writerList.add(startNewWriterPreflushAction(routingKey, streamName, eventWriteCount, eventsWrittenToPravega.get(i), factory, restart));
        }
        return writerList;
    }

    private void createReaderGroup(String group, ReaderGroupManager manager, String streamName) {
        log.info("Creating ReaderGroup : {}", group);
        manager.createReaderGroup(group, ReaderGroupConfig.builder().stream(Stream.of(SCOPE_NAME, streamName)).build());
        log.info("ReaderGroup Name : {} ", manager.getReaderGroup(group).getGroupName());
        log.info("ReaderGroup Scope : {}", manager.getReaderGroup(group).getScope());
    }

    private List<CompletableFuture<Void>> createEventReaders(int readers, ClientFactoryImpl factory, String readerGroupName) {
        log.info("Creating {} Readers.", readers);
        List<CompletableFuture<Void>> readerList = new ArrayList<>();
        for (int i = 0; i < readers; i++) {
            String readerId = String.format("LargeEventReader-%d", i);
            readerList.add(startNewReader(readerId,
                    factory,
                    readerGroupName,
                    eventsReadFromPravega,
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
        assertTrue(sealStreamStatus.get());

        //delete the stream
        CompletableFuture<Boolean> deleteStreamStatus = controller.deleteStream(SCOPE_NAME, streamName);
        log.info("Deleting stream '{}'", streamName);
        assertTrue(deleteStreamStatus.get());
        //delete the  SCOPE_NAME
        CompletableFuture<Boolean> deleteScopeStatus = controller.deleteScope(SCOPE_NAME);
        log.info("Deleting scope '{}'", SCOPE_NAME);
        assertTrue(deleteScopeStatus.get());
    }

    @Test
    public void readWriteTest() throws ExecutionException, InterruptedException {
        String readerGroupName = "testLargeEventReaderGroup";
        String streamName = "ReadWrite";
        StreamConfiguration config = getStreamConfiguraton(NUM_READERS);
        createScopeStream(SCOPE_NAME, streamName, config);
        generateWriteEventData(NUM_WRITERS, Serializer.MAX_EVENT_SIZE * 5);

        try (ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder().build());
             ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE_NAME, controller, connectionFactory);
             ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(SCOPE_NAME, controller, clientFactory)) {
            // Start writing events to the stream.
            val writers = createEventWriters(streamName, NUM_WRITERS, clientFactory);
            // Create a ReaderGroup.
            createReaderGroup(readerGroupName, readerGroupManager, streamName);
            // Create Readers.
            val readers = createEventReaders(NUM_READERS, clientFactory, readerGroupName);
            Futures.allOf(writers).get();
            ExecutorServiceHelpers.shutdown(writerPool);
            stopReadFlag.set(true);
            Futures.allOf(readers).get();
            ExecutorServiceHelpers.shutdown(readerPool);

            log.info("Deleting ReaderGroup: {}", readerGroupName);
            readerGroupManager.deleteReaderGroup(readerGroupName);
        }

        validateCleanUp(streamName);
        validateEventReads(eventsReadFromPravega, eventsWrittenToPravega);
    }

    @Test
    public void testReadWriteWithSegmentStoreRestart() throws ExecutionException, InterruptedException {
    String readerGroupName = "testLargeEventFailoverReaderGroup";
        String streamName = "SegmentStoreRestart";
        StreamConfiguration config = getStreamConfiguraton(NUM_READERS);
        createScopeStream(SCOPE_NAME, streamName, config);
        generateWriteEventData(NUM_WRITERS, Serializer.MAX_EVENT_SIZE * 5);

        try (ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder().build());
             ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE_NAME, controller, connectionFactory);
             ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(SCOPE_NAME, controller, clientFactory)) {
            // Start writing events to the stream.
            val writers = createEventWriters(streamName, NUM_WRITERS, clientFactory);
            // Create a ReaderGroup.
            createReaderGroup(readerGroupName, readerGroupManager, streamName);
            // Create Readers.
            val readers = createEventReaders(NUM_READERS, clientFactory, readerGroupName);
            Futures.allOf(writers).get();
            ExecutorServiceHelpers.shutdown(writerPool);
            stopReadFlag.set(true);

            // Reset the server, in effect clearing the AppendProcessor and PravegaRequestProcessor.
            this.server = new PravegaConnectionListener(false, servicePort, store, tableStore, serviceBuilder.getLowPriorityExecutor());
            this.server.startListening();

            Futures.allOf(readers).get();
            ExecutorServiceHelpers.shutdown(readerPool);

            log.info("Deleting ReaderGroup: {}", readerGroupName);
            readerGroupManager.deleteReaderGroup(readerGroupName);
        }

        validateCleanUp(streamName);
        validateEventReads(eventsReadFromPravega, eventsWrittenToPravega);
    }

    @Test
    public void testReadWriteWithConnectionReconnect() throws ExecutionException, InterruptedException {
        int numWriters = 1;
        int numReaders = 1;
        String readerGroupName = "testLargeEventReconnectReaderGroup";
        String streamName = "ConnectionReconnect";
        StreamConfiguration config = getStreamConfiguraton(numReaders);

        createScopeStream(SCOPE_NAME, streamName, config);
        generateWriteEventData(numWriters, Serializer.MAX_EVENT_SIZE * 5);

        try (ConnectionExporter connectionFactory = new ConnectionExporter(ClientConfig.builder().build());
             ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE_NAME, controller, connectionFactory);
             ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(SCOPE_NAME, controller, clientFactory)) {
            // Start writing events to the stream.
            val writers = createReconnectingEventWriters(streamName, numWriters, clientFactory, connectionFactory);
            // Create a ReaderGroup.
            createReaderGroup(readerGroupName, readerGroupManager, streamName);
            // Create Readers.
            val readers = createEventReaders(numReaders, clientFactory, readerGroupName);
            Futures.allOf(writers).get();
            ExecutorServiceHelpers.shutdown(writerPool);
            stopReadFlag.set(true);

            Futures.allOf(readers).get();
            ExecutorServiceHelpers.shutdown(readerPool);

            log.info("Deleting ReaderGroup: {}", readerGroupName);
            readerGroupManager.deleteReaderGroup(readerGroupName);
        }

        validateCleanUp(streamName);
        validateEventReads(eventsReadFromPravega, eventsWrittenToPravega);
    }

    @Test
    public void testReadWriteStreamSeal() throws ExecutionException, InterruptedException {
        String readerGroupName = "testLargeEventStreamSealReaderGroup";
        String streamName = "StreamSeal";
        StreamConfiguration config = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.byDataRate(500, 2, 1))
                .retentionPolicy(RetentionPolicy.bySizeBytes(Long.MAX_VALUE))
                .build();

        createScopeStream(SCOPE_NAME, streamName, config);
        generateWriteEventData(NUM_WRITERS, Serializer.MAX_EVENT_SIZE * 5);

        try (ConnectionExporter connectionFactory = new ConnectionExporter(ClientConfig.builder().build());
             ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE_NAME, controller, connectionFactory);
             ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(SCOPE_NAME, controller, clientFactory)) {
            // Start writing events to the stream.
            val writers = createEventWriters(streamName, NUM_WRITERS, clientFactory);
            // Create a ReaderGroup.
            createReaderGroup(readerGroupName, readerGroupManager, streamName);
            // Create Readers.
            val readers = createEventReaders(NUM_READERS, clientFactory, readerGroupName);
            Futures.allOf(writers).get();
            ExecutorServiceHelpers.shutdown(writerPool);
            stopReadFlag.set(true);

            Futures.allOf(readers).get();
            ExecutorServiceHelpers.shutdown(readerPool);

            log.info("Deleting ReaderGroup: {}", readerGroupName);
            readerGroupManager.deleteReaderGroup(readerGroupName);
        }

        validateCleanUp(streamName);
        validateEventReads(eventsReadFromPravega, eventsWrittenToPravega);
    }

    private CompletableFuture<Void> startNewWriter(final String routingKey,
                                                   final String streamName,
                                                   final AtomicLong writeCount,
                                                   final ByteBuffer data,
                                                   final EventStreamClientFactory clientFactory) {
        return startNewWriterPreflushAction(routingKey, streamName, writeCount, data, clientFactory, null);
    }

    // For now, limit to just one even per writer.
    private CompletableFuture<Void> startNewWriterPreflushAction(final String routingKey,
                                                   final String streamName,
                                                   final AtomicLong writeCount,
                                                   final ByteBuffer data,
                                                   final EventStreamClientFactory clientFactory,
                                                         Runnable action) {
        return CompletableFuture.runAsync(() -> {
            @Cleanup
            final EventStreamWriter<ByteBuffer> writer = clientFactory.createEventWriter(streamName,
                    new ByteBufferSerializer(),
                    EventWriterConfig.builder().build());

            log.debug("Writing Large Event : {}", routingKey);
            writeCount.incrementAndGet();
            writer.writeEvent(routingKey, data);
            // Allow one to supply an action during a write call, such as a SegmentStore restart.
            if (action != null) {
                action.run();
            }
            writer.flush();
            log.info("Closing writer {}", writer);
            writer.close();

        }, writerPool);
    }

    private CompletableFuture<Void> startNewReader(final String readerId, final EventStreamClientFactory clientFactory, final String
            readerGroupName, final ConcurrentLinkedQueue<ByteBuffer> readResult, final AtomicLong writeCount, final
                                                   AtomicLong readCount, final  AtomicBoolean exitFlag) {
        return CompletableFuture.runAsync(() -> {
            @Cleanup
            final EventStreamReader<ByteBuffer> reader = clientFactory.createReader(readerId,
                    readerGroupName,
                    new ByteBufferSerializer(),
                    ReaderConfig.builder().build());
            log.info("Starting Reader: {}", readerId);
            while (!(exitFlag.get() && readCount.get() == writeCount.get())) {
                final ByteBuffer event = reader.readNextEvent(SECONDS.toMillis(2)).getEvent();
                if (event != null) {
                    // This first byte should be the writerId.
                    log.info("Reading Event [{}], {} total bytes.", event.get(0), event.array().length);
                    // Update if event read is not null.
                    readResult.add(event);
                    readCount.incrementAndGet();
                }
            }
            log.info("Closing Reader : {}", reader);
            reader.close();
        }, readerPool);
    }

    void validateEventReads(ConcurrentLinkedQueue<ByteBuffer> eventsRead, ConcurrentHashMap<Integer, ByteBuffer> eventsWritten) {
        assertEquals("Mismatched number of events read vs. written.", eventsRead.size(), eventsWritten.size());
        for (ByteBuffer read : eventsRead) {
            // Get first byte to determine which writer the event came from.
            int writerId = read.get(0);
            assertNotNull(String.format("Unexpected writerId (%d) read from event.", writerId), eventsWritten.get(writerId));
            ByteBuffer write = eventsWritten.get(writerId);
            // Validate ordering of bytes>
            int bytesRead = read.array().length;
            int bytesWritten = write.array().length;
            log.debug("Validating LargeEvent '{}'.", writerId);
            assertEquals(String.format("Mismatch of bytes read (%d) vs. bytes written (%d).", bytesRead, bytesWritten), bytesRead, bytesWritten);
            for (int i = 0; i < bytesRead; i++) {
                byte rb = read.get(i);
                byte wb = write.get(i);
                // Avoid cost of String creation on matching bytes.
                if (rb != wb) {
                    Assert.fail(String.format("Byte mismatch at index %d (read: %b, written: %b).", i, rb, wb));
                }
            }
            log.debug("LargeEvent '{}' validated.", writerId);
        }
    }

    private static class ConnectionExporter extends SocketConnectionFactoryImpl {

        @Getter
        public ClientConnection connection;

        ConnectionExporter(ClientConfig config) {
            super(config);
        }

        @Override
        public CompletableFuture<ClientConnection> establishConnection(PravegaNodeUri endpoint, ReplyProcessor rp) {
            CompletableFuture<ClientConnection> connection = super.establishConnection(endpoint, rp);
            connection.thenAccept(conn -> this.connection = conn);
            return connection;
        }

        @Override
        public void close() {
            super.close();
            this.connection.close();
        }

    }

}

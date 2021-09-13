package io.pravega.test.integration;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.admin.impl.StreamManagerImpl;
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
import io.pravega.test.common.LeakDetectorTestSuite;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
public class LargeAppendTest extends LeakDetectorTestSuite {

    private AtomicLong eventReadCount;
    private AtomicLong eventWriteCount;
    private AtomicBoolean stopReadFlag;
    private static final int NUM_WRITERS = 1;
    private static final int NUM_READERS = 1;
    private ServiceBuilder serviceBuilder;
    private TestingServer zkTestServer = null;
    private PravegaConnectionListener server = null;
    private ControllerWrapper controllerWrapper = null;
    private Controller controller = null;
    private ScheduledExecutorService writerPool;
    private ScheduledExecutorService readerPool;
    private ConcurrentLinkedQueue<ByteBuffer> eventsReadFromPravega;
    private ArrayList<ByteBuffer> eventData = new ArrayList<>();

    private static final String STREAM_NAME = "stream";

    private static final String SCOPE_NAME = "scope";

    @Before
    public void setup() throws Exception {

        final int controllerPort = TestUtils.getAvailableListenPort();
        final String serviceHost = "localhost";
        final int servicePort = TestUtils.getAvailableListenPort();
        final int containerCount = 4;

        // 1. Start ZK
        this.zkTestServer = new TestingServerStarter().start();

        // 2. Start Pravega SegmentStore service.
        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();

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

    @Test
    public void readWriteTest() throws ExecutionException, InterruptedException {

        String readerGroupName = "testLargeEventReaderGroup";

        //20  readers -> 20 stream segments ( to have max read parallelism)
        ScalingPolicy scalingPolicy = ScalingPolicy.fixed(20);
        StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(scalingPolicy).build();

        eventsReadFromPravega = new ConcurrentLinkedQueue<>();
        eventReadCount = new AtomicLong(); // used by readers to maintain a count of events.
        eventWriteCount = new AtomicLong();
        stopReadFlag = new AtomicBoolean(false);

        ClientConfig clientConfig = ClientConfig.builder().build();
        try (ConnectionPool cp = new ConnectionPoolImpl(clientConfig, new SocketConnectionFactoryImpl(clientConfig));
             StreamManager streamManager = new StreamManagerImpl(controller, cp)) {
            //create a scope
            Boolean createScopeStatus = streamManager.createScope(SCOPE_NAME);
            log.info("Create scope status {}", createScopeStatus);
            //create a stream
            Boolean createStreamStatus = streamManager.createStream(SCOPE_NAME, STREAM_NAME, config);
            log.info("Create stream status {}", createStreamStatus);
        }

        int eventSize = Serializer.MAX_EVENT_SIZE * 2;
        for (int i = 0; i < NUM_WRITERS; i++) {
            byte[] bytes = RandomUtils.nextBytes(eventSize);
            // Make the first byte the writerId for logging purposes.
            bytes[0] = (byte) i;
            ByteBuffer buf = ByteBuffer.wrap(bytes);
            eventData.add(buf);
        }

        try (ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder().build());
             ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE_NAME, controller, connectionFactory);
             ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(SCOPE_NAME, controller, clientFactory)) {

            //start writing events to the stream
            log.info("Creating {} Writers.", NUM_WRITERS);
            List<CompletableFuture<Void>> writerList = new ArrayList<>();
            for (int i = 0; i < NUM_WRITERS; i++) {
                log.info("Starting Writer {}", i);
                String routingKey = String.format("LargeEventWriter-%d", i);
                writerList.add(startNewWriter(routingKey, eventWriteCount, eventData.get(i), clientFactory));
            }

            //create a reader group
            log.info("Creating ReaderGroup : {}", readerGroupName);

            readerGroupManager.createReaderGroup(readerGroupName, ReaderGroupConfig.builder().stream(Stream.of(SCOPE_NAME, STREAM_NAME)).build());
            log.info("ReaderGroup Name : {} ", readerGroupManager.getReaderGroup(readerGroupName).getGroupName());
            log.info("ReaderGroup Scope : {}", readerGroupManager.getReaderGroup(readerGroupName).getScope());

            //create readers
            log.info("Creating {} Readers.", NUM_READERS);
            List<CompletableFuture<Void>> readerList = new ArrayList<>();
            for (int i = 0; i < NUM_READERS; i++) {
                readerList.add(startNewReader(String.format("LargeEventReader-%d", i),
                        clientFactory,
                        readerGroupName,
                        eventsReadFromPravega,
                        eventWriteCount,
                        eventReadCount,
                        stopReadFlag));
            }

            Futures.allOf(writerList).get();
            ExecutorServiceHelpers.shutdown(writerPool);

            stopReadFlag.set(true);
            Futures.allOf(readerList).get();
            ExecutorServiceHelpers.shutdown(readerPool);

            log.info("Deleting ReaderGroup: {}", readerGroupName);
            readerGroupManager.deleteReaderGroup(readerGroupName);
        }
        // ------------------------------------------------------------------------------------------------------------
        //ByteBuffer payload = ByteBuffer.allocate((int) messageSize);

        //long heapSize = Runtime.getRuntime().maxMemory();
        //long totalBytes = heapSize / 8;

        //// Writes many large events.
        ////for (int i = 0; i < totalBytes / messageSize; i++) {
        //for (int i = 0; i < 1; i++) {
        //    CompletableFuture<Void> ack = producer.writeEvent(payload);
        //    ack.thenRun(() -> {
        //        System.out.println("\033[31m RED");
        //        System.out.println("Large event written.");
        //    }).get();
        //}

        //Future<Void> ack = producer.writeEvent(testString);
        //ack.get(5, TimeUnit.SECONDS);

    }
    private CompletableFuture<Void> startNewWriter(final String routingKey,
                                                   final AtomicLong writeCount,
                                                   final ByteBuffer data,
                                                   final EventStreamClientFactory clientFactory) {
        return CompletableFuture.runAsync(() -> {
            @Cleanup
            final EventStreamWriter<ByteBuffer> writer = clientFactory.createEventWriter(STREAM_NAME,
                    new ByteBufferSerializer(),
                    EventWriterConfig.builder().build());

            log.debug("Writing Large Event : {}", routingKey);
            writeCount.incrementAndGet();
            writer.writeEvent(routingKey, data);
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
            log.debug("ExitFlag: {}, ReadCount: {}, WriteCount: {}", exitFlag.get(), readCount.get(), writeCount.get());
            while (!(exitFlag.get() && readCount.get() == writeCount.get())) {
                log.debug("Attempting to read next event.");
                final ByteBuffer event = reader.readNextEvent(SECONDS.toMillis(2)).getEvent();
                // This first byte should be the writerId.
                log.debug("Reading event [{}:{} ...]", event.get(0), event.get(1));
                if (event != null) {
                    //update if event read is not null.
                    readResult.add(event);
                    readCount.incrementAndGet();
                }
            }
            log.info("Closing Reader : {}", reader);
            reader.close();
        }, readerPool);
    }

    /**
     * Large events mixed with normal events (maintanis ordering)
     *
     *
     */
    @Test
    public void testLargeAppend() {

    }

    @Test
    public void testLargeAppendConnectionDropped() {

    }

}

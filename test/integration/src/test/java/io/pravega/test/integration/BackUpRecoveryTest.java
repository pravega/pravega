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
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.admin.KeyValueTableInfo;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.state.Revision;
import io.pravega.client.state.RevisionedStreamClient;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Position;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.TimeWindow;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.watermark.WatermarkSerializer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.io.FileHelpers;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.shared.NameUtils;
import io.pravega.shared.watermarks.Watermark;
import io.pravega.storage.filesystem.FileSystemSimpleStorageFactory;
import io.pravega.storage.filesystem.FileSystemStorageConfig;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import io.pravega.test.integration.utils.LocalServiceStarter;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.pravega.common.concurrent.Futures.loop;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Integration test to verify data recovery.
 * Recovery scenario: when data written to Pravega is already flushed to the long term storage.
 * Tests replicate different environments for data recovery.
 */
@Slf4j
public class BackUpRecoveryTest extends ThreadPooledTestSuite {
    private static final Duration TIMEOUT = Duration.ofMillis(100 * 10000);
    private static final Duration READ_TIMEOUT = Duration.ofMillis(1000);
    private static final Duration TRANSACTION_TIMEOUT = Duration.ofMillis(10000);

    /**
     * Write {@link #TOTAL_NUM_EVENTS} events to verify recovery.
     */
    private static final int TOTAL_NUM_EVENTS = 300;

    private static final Random RANDOM = new Random(1234);

    /**
     * Scope and streams to read and write events.
     */
    private static final String SCOPE = "testRecoveryScope";
    private static final String STREAM1 = "testRecoveryStream" + RANDOM.nextInt(Integer.MAX_VALUE);
    private static final String EVENT = "12345";
    private final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(1);
    private final StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(scalingPolicy).build();

    private final AtomicLong timer = new AtomicLong();

    private FileSystemSimpleStorageFactory storageFactory;

    private BookKeeperLogFactory dataLogFactory;
    private File baseDir = null;

    @Before
    public void setUp() throws Exception {
        this.baseDir = Files.createTempDirectory("test_backupRecovery").toFile().getAbsoluteFile();
        val storageConfig = FileSystemStorageConfig
                .builder()
                .with(FileSystemStorageConfig.ROOT, this.baseDir.getAbsolutePath())
                .build();

        this.storageFactory = new FileSystemSimpleStorageFactory(ChunkedSegmentStorageConfig.DEFAULT_CONFIG, storageConfig, executorService());
        log.info("Created a long term storage.");
    }

    @After
    public void tearDown() throws Exception {
        if (this.dataLogFactory != null) {
            this.dataLogFactory.close();
        }
        timer.set(0);
        FileHelpers.deleteFileOrDirectory(this.baseDir);
    }

    @Override
    protected int getThreadPoolSize() {
        return 100;
    }

    /**
     * Creates a client to read and write events.
     */
    private static class ClientRunner implements AutoCloseable {
        private final ConnectionFactory connectionFactory;
        private final ClientFactoryImpl clientFactory;
        private final ReaderGroupManager readerGroupManager;

        ClientRunner(LocalServiceStarter.ControllerRunner controllerRunner) {
            this.connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder()
                    .controllerURI(controllerRunner.getControllerURI()).build());
            this.clientFactory = new ClientFactoryImpl(SCOPE, controllerRunner.getController(), connectionFactory);
            this.readerGroupManager = new ReaderGroupManagerImpl(SCOPE, controllerRunner.getController(), clientFactory);
        }

        @Override
        public void close() {
            this.readerGroupManager.close();
            this.clientFactory.close();
            this.connectionFactory.close();
        }
    }

    /**
     * Tests the data recovery scenario with just one segment container.
     *  What test does, step by step:
     *  1.  Starts Pravega locally with just one segment container.
     *  2.  Writes {@link #TOTAL_NUM_EVENTS} events.
     *  3.  Shuts down the controller
     *  4.  Waits for DurableLog to be entirely flushed to the long term storage.
     *  5.  Shuts down segment store and bookeeper/zookeeper.
     *  6.  Starts new instance of bk, zk, store and controller.
     *  7. Reads all events to verify that auto recovery is done and reads are successful.
     * @throws Exception    In case of an exception occurred while execution.
     */
    @Test(timeout = 90000)
    public void testDurableDataLogRecoverySingleContainer() throws Exception {
        testRecovery(1, 1, false);
    }

    /**
     * Tests the data recovery scenario with multiple segment containers.
     *  What test does, step by step:
     *  1.  Starts Pravega locally with just one segment container.
     *  2.  Writes {@link #TOTAL_NUM_EVENTS} events.
     *  3.  Shuts down the controller
     *  4.  Waits for DurableLog to be entirely flushed to the long term storage.
     *  5.  Shuts down segment store and bookeeper/zookeeper.
     *  6.  Starts new instance of bk, zk, store and controller.
     *  7. Reads all events.
     * @throws Exception    In case of an exception occurred while execution.
     */
    @Test(timeout = 180000)
    public void testDurableDataLogFailRecoveryMultipleContainers() throws Exception {
        testRecovery(4, 3, false);
    }

    /**
     * Tests the data recovery scenario with transactional writer. Events are written using a transactional writer.
     *  What test does, step by step:
     *  1.  Starts Pravega locally with just 4 segment containers.
     *  2.  Writes {@link #TOTAL_NUM_EVENTS} events in the form of transactions.
     *  3.  Shuts down the controller
     *  4.  Waits for DurableLog to be entirely flushed to the long term storage.
     *  5.  Shuts down segment store and bookeeper/zookeeper.
     *  6.  Starts new instance of bk, zk, store and controller.
     *  7. Reads all events.
     * @throws Exception    In case of an exception occurred while execution.
     */
    @Test(timeout = 180000)
    public void testDurableDataLogFailRecoveryTransactionalWriter() throws Exception {
        testRecovery(4, 1, true);
    }


    /**
     * Performs a data recovery test(with all segments flushed to the long-term storage beforehand) using the given parameters.
     * @param containerCount    The number of containers to be used in the pravega instance and the number of debug segment
     *                          containers that will be started.
     * @param withTransaction   A boolean to indicate weather to write events in the form of transactions or not.
     * @throws Exception        In case of an exception occurred while execution.
     */
    private void testRecovery(int containerCount, int bookieCount, boolean withTransaction) throws Exception {
        int instanceId = 0;
        // Creating a long term storage only once here.

        // Start a new BK & ZK, segment store and controller
        @Cleanup
        LocalServiceStarter.PravegaRunner pravegaRunner = new LocalServiceStarter.PravegaRunner(bookieCount, containerCount);
        pravegaRunner.startBookKeeperRunner(instanceId++);
        val bkConfig = BookKeeperConfig.builder()
                .with(BookKeeperConfig.ZK_ADDRESS, "localhost:" + pravegaRunner.getBookKeeperRunner().getBkPort())
                .with(BookKeeperConfig.BK_LEDGER_PATH, pravegaRunner.getBookKeeperRunner().getLedgerPath())
                .with(BookKeeperConfig.ZK_METADATA_PATH, pravegaRunner.getBookKeeperRunner().getLogMetaNamespace())
                .with(BookKeeperConfig.BK_ENSEMBLE_SIZE, 1)
                .with(BookKeeperConfig.BK_WRITE_QUORUM_SIZE, 1)
                .with(BookKeeperConfig.BK_ACK_QUORUM_SIZE, 1)
                .build();
        this.dataLogFactory = new BookKeeperLogFactory(bkConfig, pravegaRunner.getBookKeeperRunner().getZkClient().get(), this.executorService());
        pravegaRunner.startControllerAndSegmentStore(this.storageFactory, this.dataLogFactory);

        // Create a stream for writing data
        createScopeStream(pravegaRunner.getControllerRunner().getController(), SCOPE, STREAM1, config);

        log.info("Created stream '{}'.", STREAM1);

        // Create a client to write events.
        try (val clientRunner = new ClientRunner(pravegaRunner.getControllerRunner())) {
            // Write events.
            writeEventsToStream(clientRunner.clientFactory, withTransaction);
        }

        log.info("Shutting down controller");
        pravegaRunner.shutDownControllerRunner(); // Shut down the controller
        // Flush DurableLog to Long Term Storage
        log.info("Flush to storage");
        flushToStorage(pravegaRunner.getSegmentStoreRunner().getServiceBuilder());

        pravegaRunner.shutDownSegmentStoreRunner(); // Shutdown SegmentStore
        pravegaRunner.shutDownBookKeeperRunner(); // Shutdown BookKeeper & ZooKeeper
        pravegaRunner.close();
        log.info("SegmentStore, BookKeeper & ZooKeeper shutdown");

        Thread.sleep(1000);
        // Start a new segment store and controller
        LocalServiceStarter.PravegaRunner pravegaRunner1 = new LocalServiceStarter.PravegaRunner(bookieCount, containerCount);
        pravegaRunner1.startBookKeeperRunner(instanceId++);
        log.info("Started bk and zk. ");
        val bkConfig1 = BookKeeperConfig.builder()
                .with(BookKeeperConfig.ZK_ADDRESS, "localhost:" + pravegaRunner1.getBookKeeperRunner().getBkPort())
                .with(BookKeeperConfig.BK_LEDGER_PATH, pravegaRunner1.getBookKeeperRunner().getLedgerPath())
                .with(BookKeeperConfig.ZK_METADATA_PATH, pravegaRunner1.getBookKeeperRunner().getLogMetaNamespace())
                .with(BookKeeperConfig.BK_ENSEMBLE_SIZE, 1)
                .with(BookKeeperConfig.BK_WRITE_QUORUM_SIZE, 1)
                .with(BookKeeperConfig.BK_ACK_QUORUM_SIZE, 1)
                .build();
        this.dataLogFactory = new BookKeeperLogFactory(bkConfig1, pravegaRunner1.getBookKeeperRunner().getZkClient().get(), this.executorService());
        log.info("Starting segmentstore and controller ");
        pravegaRunner1.startControllerAndSegmentStore(this.storageFactory, this.dataLogFactory);
        log.info("Started segment store and controller again.");

        log.info("Starting client to read Events ");
        // Create the client with new controller.
        try (val clientRunner = new ClientRunner(pravegaRunner1.getControllerRunner())) {
            // Try reading all events again to verify that the recovery was successful.
            readEventsFromStream(clientRunner.clientFactory, clientRunner.readerGroupManager);
            log.info("Read all events again to verify that segments were recovered.");
        }
    }

    @Test(timeout = 180000)
    public void testRecoverWithKvt() throws Exception {
        int instanceId = 0;
        int containerCount = 4;
        int bookieCount = 3;

        // Start a new BK & ZK, segment store and controller
        @Cleanup
        LocalServiceStarter.PravegaRunner pravegaRunner = new LocalServiceStarter.PravegaRunner(bookieCount, containerCount);
        pravegaRunner.startBookKeeperRunner(instanceId++);
        val bkConfig = BookKeeperConfig.builder()
                .with(BookKeeperConfig.ZK_ADDRESS, "localhost:" + pravegaRunner.getBookKeeperRunner().getBkPort())
                .with(BookKeeperConfig.BK_LEDGER_PATH, pravegaRunner.getBookKeeperRunner().getLedgerPath())
                .with(BookKeeperConfig.ZK_METADATA_PATH, pravegaRunner.getBookKeeperRunner().getLogMetaNamespace())
                .with(BookKeeperConfig.BK_ENSEMBLE_SIZE, 1)
                .with(BookKeeperConfig.BK_WRITE_QUORUM_SIZE, 1)
                .with(BookKeeperConfig.BK_ACK_QUORUM_SIZE, 1)
                .build();
        this.dataLogFactory = new BookKeeperLogFactory(bkConfig, pravegaRunner.getBookKeeperRunner().getZkClient().get(), this.executorService());
        pravegaRunner.startControllerAndSegmentStore(this.storageFactory, this.dataLogFactory);

        // Create a stream for writing data
        createScopeStream(pravegaRunner.getControllerRunner().getController(), SCOPE, STREAM1, config);

        log.info("Created stream '{}'.", STREAM1);

        val defaultConfig = KeyValueTableConfiguration.builder()
                .partitionCount(5)
                .primaryKeyLength(Long.BYTES)
                .secondaryKeyLength(Integer.BYTES)
                .build();

        val kvt1 = newKeyValueTableName();
        boolean created = pravegaRunner.getControllerRunner().getController().createKeyValueTable(kvt1.getScope(), kvt1.getKeyValueTableName(), defaultConfig).join();
        Assert.assertTrue(created);
        val segments = pravegaRunner.getControllerRunner().getController().getCurrentSegmentsForKeyValueTable(kvt1.getScope(), kvt1.getKeyValueTableName()).join();
        Assert.assertEquals(defaultConfig.getPartitionCount(), segments.getSegments().size());

        log.info("Shutting down controller");
        pravegaRunner.shutDownControllerRunner(); // Shut down the controller
        // Flush DurableLog to Long Term Storage
        log.info("Flush to storage");
        flushToStorage(pravegaRunner.getSegmentStoreRunner().getServiceBuilder());

        pravegaRunner.shutDownSegmentStoreRunner(); // Shutdown SegmentStore
        pravegaRunner.shutDownBookKeeperRunner(); // Shutdown BookKeeper & ZooKeeper
        pravegaRunner.close();
        log.info("SegmentStore, BookKeeper & ZooKeeper shutdown");

        Thread.sleep(1000);
        // Start a new segment store and controller
        LocalServiceStarter.PravegaRunner pravegaRunner1 = new LocalServiceStarter.PravegaRunner(bookieCount, containerCount);
        pravegaRunner1.startBookKeeperRunner(instanceId++);
        log.info("Started bk and zk. ");
        val bkConfig1 = BookKeeperConfig.builder()
                .with(BookKeeperConfig.ZK_ADDRESS, "localhost:" + pravegaRunner1.getBookKeeperRunner().getBkPort())
                .with(BookKeeperConfig.BK_LEDGER_PATH, pravegaRunner1.getBookKeeperRunner().getLedgerPath())
                .with(BookKeeperConfig.ZK_METADATA_PATH, pravegaRunner1.getBookKeeperRunner().getLogMetaNamespace())
                .with(BookKeeperConfig.BK_ENSEMBLE_SIZE, 1)
                .with(BookKeeperConfig.BK_WRITE_QUORUM_SIZE, 1)
                .with(BookKeeperConfig.BK_ACK_QUORUM_SIZE, 1)
                .build();
        this.dataLogFactory = new BookKeeperLogFactory(bkConfig1, pravegaRunner1.getBookKeeperRunner().getZkClient().get(), this.executorService());
        log.info("Starting segmentstore and controller ");
        pravegaRunner1.startControllerAndSegmentStore(this.storageFactory, this.dataLogFactory);
        log.info("Started segment store and controller again.");

        log.info("Verify segments for kvt");
        val segmentsAfterRecovery = pravegaRunner1.getControllerRunner().getController().getCurrentSegmentsForKeyValueTable(kvt1.getScope(), kvt1.getKeyValueTableName()).join();
        Assert.assertEquals(defaultConfig.getPartitionCount(), segmentsAfterRecovery.getSegments().size());
    }

    private KeyValueTableInfo newKeyValueTableName() {
        return new KeyValueTableInfo(SCOPE, String.format("KVT-%d", System.nanoTime()));
    }

    private void flushToStorage(ServiceBuilder serviceBuilder) throws Exception {
        ServiceBuilder.ComponentSetup componentSetup = new ServiceBuilder.ComponentSetup(serviceBuilder);
        ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int containerId = 0; containerId < componentSetup.getContainerRegistry().getContainerCount(); containerId++) {
            futures.add(componentSetup.getContainerRegistry().getContainer(containerId).flushToStorage(TIMEOUT));
        }
        Futures.allOf(futures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    // Writes events to the streams with/without transactions.
    private void writeEventsToStream(ClientFactoryImpl clientFactory, boolean withTransaction)
            throws TxnFailedException {
        if (withTransaction) {
            log.info("Writing transactional events on to stream: {}", STREAM1);
            writeTransactionalEvents(STREAM1, clientFactory); // write events
        } else {
            log.info("Writing events on to stream: {}", STREAM1);
            writeEvents(STREAM1, clientFactory); // write events
        }
    }

    // Reads all events from the streams.
    private void readEventsFromStream(ClientFactoryImpl clientFactory, ReaderGroupManager readerGroupManager) {
        readAllEvents(STREAM1, clientFactory, readerGroupManager, "RG" + RANDOM.nextInt(Integer.MAX_VALUE),
                "R" + RANDOM.nextInt(Integer.MAX_VALUE));
    }

    /**
     * Tests the data recovery scenario with readers stalling while reading. Readers read some events and then they are
     * stopped.
     *  What test does, step by step:
     *  1. Starts Pravega locally with just 4 segment containers.
     *  2. Writes {@link #TOTAL_NUM_EVENTS} events.
     *  3. Shuts down the controller.
     *  3. Waits for all segments created to be flushed to the long term storage.
     *  4. Let a reader read N number of events.
     *  5. Shuts down the segment store and bookeeper/zookeeper.
     *  6. Starts new instance of bk, zk, segment store and controller.
     *  7. Let the reader read rest of the 10-N number of events.
     * @throws Exception    In case of an exception occurred while execution.
     */
    @Test(timeout = 180000)
    public void testDurableDataLogRecoveryReadersPaused() throws Exception {
        int instanceId = 0;
        int bookieCount = 1;
        int containerCount = 4;
        int eventsReadCount = RANDOM.nextInt(TOTAL_NUM_EVENTS);
        String testReader = "readerDRIntegrationTest";
        String testReaderGroup = "readerGroupDRIntegrationTest";

        // Start a new BK & ZK, segment store and controller
        @Cleanup
        LocalServiceStarter.PravegaRunner pravegaRunner = new LocalServiceStarter.PravegaRunner(bookieCount, containerCount);
        pravegaRunner.startBookKeeperRunner(instanceId++);
        val bkConfig = BookKeeperConfig.builder()
                .with(BookKeeperConfig.ZK_ADDRESS, "localhost:" + pravegaRunner.getBookKeeperRunner().getBkPort())
                .with(BookKeeperConfig.BK_LEDGER_PATH, pravegaRunner.getBookKeeperRunner().getLedgerPath())
                .with(BookKeeperConfig.ZK_METADATA_PATH, pravegaRunner.getBookKeeperRunner().getLogMetaNamespace())
                .with(BookKeeperConfig.BK_ENSEMBLE_SIZE, 1)
                .with(BookKeeperConfig.BK_WRITE_QUORUM_SIZE, 1)
                .with(BookKeeperConfig.BK_ACK_QUORUM_SIZE, 1)
                .build();
        this.dataLogFactory = new BookKeeperLogFactory(bkConfig, pravegaRunner.getBookKeeperRunner().getZkClient().get(), this.executorService());
        pravegaRunner.startControllerAndSegmentStore(this.storageFactory, this.dataLogFactory);

        log.info("Started pravega ");
        // Create a stream for writing data
        createScopeStream(pravegaRunner.getControllerRunner().getController(), SCOPE, STREAM1, config);
        log.info("Created stream '{}'.", STREAM1);

        // Create a client to write events.
        try (val clientRunner = new ClientRunner(pravegaRunner.getControllerRunner())) {
            // Write events.
            writeEventsToStream(clientRunner.clientFactory, true);

            // Create a reader for reading from the stream.
            EventStreamReader<String> reader = createReader(clientRunner.clientFactory, clientRunner.readerGroupManager,
                    SCOPE, STREAM1, testReaderGroup, testReader);

            // Let reader read N number of events and mark its position.
            Position p = readNEvents(reader, eventsReadCount);

            ReaderGroup readerGroup = clientRunner.readerGroupManager.getReaderGroup(testReaderGroup);

            readerGroup.readerOffline(testReader, p);
        }

        pravegaRunner.shutDownControllerRunner(); // Shut down the controller

        // Flush DurableLog to Long Term Storage
        flushToStorage(pravegaRunner.getSegmentStoreRunner().getServiceBuilder());

        pravegaRunner.shutDownSegmentStoreRunner(); // Shutdown SegmentStore
        pravegaRunner.shutDownBookKeeperRunner(); // Shutdown BookKeeper & ZooKeeper
        log.info("SegmentStore, BookKeeper & ZooKeeper shutdown");

        // start a new BookKeeper and ZooKeeper.
        pravegaRunner.startBookKeeperRunner(instanceId++);
        val bkConfig1 = BookKeeperConfig.builder()
                .with(BookKeeperConfig.ZK_ADDRESS, "localhost:" + pravegaRunner.getBookKeeperRunner().getBkPort())
                .with(BookKeeperConfig.BK_LEDGER_PATH, pravegaRunner.getBookKeeperRunner().getLedgerPath())
                .with(BookKeeperConfig.ZK_METADATA_PATH, pravegaRunner.getBookKeeperRunner().getLogMetaNamespace())
                .with(BookKeeperConfig.BK_ENSEMBLE_SIZE, 1)
                .with(BookKeeperConfig.BK_WRITE_QUORUM_SIZE, 1)
                .with(BookKeeperConfig.BK_ACK_QUORUM_SIZE, 1)
                .build();
        this.dataLogFactory = new BookKeeperLogFactory(bkConfig1, pravegaRunner.getBookKeeperRunner().getZkClient().get(), this.executorService());
        log.info("Started a new BookKeeper and ZooKeeper.");

        // Start a new segment store and controller
        pravegaRunner.startControllerAndSegmentStore(this.storageFactory, this.dataLogFactory);
        log.info("Started segment store and controller again.");

        // Create the client with new controller.
        try (val clientRunner = new ClientRunner(pravegaRunner.getControllerRunner())) {

            // Get reader group.
            ReaderGroup readerGroup = clientRunner.readerGroupManager.getReaderGroup(testReaderGroup);
            assertNotNull(readerGroup);

            EventStreamReader<String> reader = clientRunner.clientFactory.createReader(testReader, testReaderGroup,
                    new UTF8StringSerializer(), ReaderConfig.builder().build());

            // Read the remaining number of events.
            readNEvents(reader, TOTAL_NUM_EVENTS - eventsReadCount);

            // Reading next event should return null.
            assertNull(reader.readNextEvent(READ_TIMEOUT.toMillis()).getEvent());
            reader.close();
        }
    }


    /**
     * Tests the data recovery scenario with watermarking events.
     *  What test does, step by step:
     *  1. Starts Pravega locally with just 4 segment containers.
     *  2. Writes {@link #TOTAL_NUM_EVENTS} events to a segment with watermarks.
     *  3. Shuts down the controller.
     *  3. Waits for all segments created to be flushed to the long term storage.
     *  4. Shuts down the segment store and bookeeper/zookeeper.
     *  8. Starts new instance of bk, zk, store and controller.
     *  9. Read all events and verify that all events are below the bounds.
     * @throws Exception    In case of an exception occurred while execution.
     */
    @Test(timeout = 180000)
    public void testDurableDataLogRecoveryWatermarking() throws Exception {
        int instanceId = 0;
        int bookieCount = 1;
        int containerCount = 4;
        String readerGroup = "rgTx";

        // Start a new BK & ZK, segment store and controller
        @Cleanup
        LocalServiceStarter.PravegaRunner pravegaRunner = new LocalServiceStarter.PravegaRunner(bookieCount, containerCount);
        pravegaRunner.startBookKeeperRunner(instanceId++);
        val bkConfig = BookKeeperConfig.builder()
                .with(BookKeeperConfig.ZK_ADDRESS, "localhost:" + pravegaRunner.getBookKeeperRunner().getBkPort())
                .with(BookKeeperConfig.BK_LEDGER_PATH, pravegaRunner.getBookKeeperRunner().getLedgerPath())
                .with(BookKeeperConfig.ZK_METADATA_PATH, pravegaRunner.getBookKeeperRunner().getLogMetaNamespace())
                .with(BookKeeperConfig.BK_ENSEMBLE_SIZE, 1)
                .with(BookKeeperConfig.BK_WRITE_QUORUM_SIZE, 1)
                .with(BookKeeperConfig.BK_ACK_QUORUM_SIZE, 1)
                .build();
        this.dataLogFactory = new BookKeeperLogFactory(bkConfig, pravegaRunner.getBookKeeperRunner().getZkClient().get(), this.executorService());
        pravegaRunner.startControllerAndSegmentStore(this.storageFactory, this.dataLogFactory);

        // Create a stream for writing data
        createScopeStream(pravegaRunner.getControllerRunner().getController(), SCOPE, STREAM1, config);

        // Create a client to write events.
        @Cleanup
        ClientRunner clientRunner = new ClientRunner(pravegaRunner.getControllerRunner());

        // Create a writer
        @Cleanup
        TransactionalEventStreamWriter<Long> writer = clientRunner.clientFactory
                .createTransactionalEventWriter("writer1", STREAM1, new JavaSerializer<>(),
                        EventWriterConfig.builder().transactionTimeoutTime(TRANSACTION_TIMEOUT.toMillis()).build());

        AtomicBoolean stopFlag = new AtomicBoolean(false);
        // write events
        CompletableFuture<Void> writerFuture = writeTxEvents(writer, stopFlag);

        // scale the stream several times so that we get complex positions
        StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(5)).build();
        Stream streamObj = Stream.of(SCOPE, STREAM1);
        scale(pravegaRunner.getControllerRunner().getController(), streamObj, config);

        // get watermarks
        LinkedBlockingQueue<Watermark> watermarks = getWatermarks(pravegaRunner, stopFlag, writerFuture);

        pravegaRunner.getControllerRunner().close(); // Shut down the controller

        // Flush DurableLog to Long Term Storage
        flushToStorage(pravegaRunner.getSegmentStoreRunner().getServiceBuilder());

        pravegaRunner.shutDownSegmentStoreRunner(); // Shutdown SegmentStore
        pravegaRunner.shutDownBookKeeperRunner(); // Shutdown BookKeeper & ZooKeeper
        log.info("SegmentStore, BookKeeper & ZooKeeper shutdown");

        // start a new BookKeeper and ZooKeeper.
        pravegaRunner.startBookKeeperRunner(instanceId++);
        val bkConfig1 = BookKeeperConfig.builder()
                .with(BookKeeperConfig.ZK_ADDRESS, "localhost:" + pravegaRunner.getBookKeeperRunner().getBkPort())
                .with(BookKeeperConfig.BK_LEDGER_PATH, pravegaRunner.getBookKeeperRunner().getLedgerPath())
                .with(BookKeeperConfig.ZK_METADATA_PATH, pravegaRunner.getBookKeeperRunner().getLogMetaNamespace())
                .with(BookKeeperConfig.BK_ENSEMBLE_SIZE, 1)
                .with(BookKeeperConfig.BK_WRITE_QUORUM_SIZE, 1)
                .with(BookKeeperConfig.BK_ACK_QUORUM_SIZE, 1)
                .build();
        this.dataLogFactory = new BookKeeperLogFactory(bkConfig1, pravegaRunner.getBookKeeperRunner().getZkClient().get(), this.executorService());
        log.info("Started a new BookKeeper and ZooKeeper.");

        // Start a new segment store and controller
        pravegaRunner.startControllerAndSegmentStore(this.storageFactory, this.dataLogFactory);
        log.info("Started segment store and controller again.");

        // Create the client with new controller.
        @Cleanup
        ClientRunner newClientRunner = new ClientRunner(pravegaRunner.getControllerRunner());

        // read events and verify
        readVerifyEventsWithWatermarks(readerGroup, newClientRunner, streamObj, watermarks);
    }

    /**
     * Creates reader and verifies watermarking by verifying the time bounds for events.
     */
    private void readVerifyEventsWithWatermarks(String readerGroup, ClientRunner clientRunner, Stream streamObj,
                                                LinkedBlockingQueue<Watermark> watermarks) throws InterruptedException {
        List<Map<Stream, StreamCut>> streamCuts = getStreamCutsFromWaterMarks(streamObj, watermarks);
        // read from stream cut of first watermark
        clientRunner.readerGroupManager.createReaderGroup(readerGroup, ReaderGroupConfig.builder().stream(streamObj)
                .startingStreamCuts(streamCuts.get(0))
                .endingStreamCuts(streamCuts.get(1))
                .build());
        @Cleanup
        final EventStreamReader<Long> reader = clientRunner.clientFactory.createReader("myreaderTx", readerGroup,
                new JavaSerializer<>(), ReaderConfig.builder().build());

        EventRead<Long> event = reader.readNextEvent(READ_TIMEOUT.toMillis());
        TimeWindow currentTimeWindow = reader.getCurrentTimeWindow(streamObj);
        while (event.getEvent() != null && currentTimeWindow.getLowerTimeBound() == null && currentTimeWindow.getUpperTimeBound() == null) {
            event = reader.readNextEvent(READ_TIMEOUT.toMillis());
            currentTimeWindow = reader.getCurrentTimeWindow(streamObj);
        }

        assertNotNull(currentTimeWindow.getUpperTimeBound());

        currentTimeWindow = verifyEventsWithTimeBounds(streamObj, reader, event, currentTimeWindow);

        assertNotNull(currentTimeWindow.getLowerTimeBound());
    }

    /**
     * Gets watermarks used while writing the events
     */
    private LinkedBlockingQueue<Watermark> getWatermarks(LocalServiceStarter.PravegaRunner pravegaRunner, AtomicBoolean stopFlag,
                                                         CompletableFuture<Void> writerFuture) throws Exception {
        @Cleanup
        SynchronizerClientFactory syncClientFactory = SynchronizerClientFactory.withScope(SCOPE,
                ClientConfig.builder().controllerURI(pravegaRunner.getControllerRunner().getControllerURI()).build());

        String markStream = NameUtils.getMarkStreamForStream(STREAM1);
        RevisionedStreamClient<Watermark> watermarkReader = syncClientFactory.createRevisionedStreamClient(markStream,
                new WatermarkSerializer(), SynchronizerConfig.builder().build());
        LinkedBlockingQueue<Watermark> watermarks = new LinkedBlockingQueue<>();
        CompletableFuture<Void> fetchWaterMarksFuture = fetchWatermarks(watermarkReader, watermarks, stopFlag);
        AssertExtensions.assertEventuallyEquals(true, () -> watermarks.size() >= 2, 100000);
        stopFlag.set(true);
        fetchWaterMarksFuture.join();
        writerFuture.join();
        return watermarks;
    }

    private List<Map<Stream, StreamCut>> getStreamCutsFromWaterMarks(Stream streamObj, LinkedBlockingQueue<Watermark> watermarks)
            throws InterruptedException {
        Watermark watermark0 = watermarks.take();
        Watermark watermark1 = watermarks.take();
        assertTrue(watermark0.getLowerTimeBound() <= watermark0.getUpperTimeBound());
        assertTrue(watermark1.getLowerTimeBound() <= watermark1.getUpperTimeBound());
        assertTrue(watermark0.getLowerTimeBound() < watermark1.getLowerTimeBound());

        Map<Segment, Long> positionMap0 = watermark0.getStreamCut().entrySet().stream().collect(
                Collectors.toMap(x -> new Segment(SCOPE, STREAM1, x.getKey().getSegmentId()), Map.Entry::getValue));
        Map<Segment, Long> positionMap1 = watermark1.getStreamCut().entrySet().stream().collect(
                Collectors.toMap(x -> new Segment(SCOPE, STREAM1, x.getKey().getSegmentId()), Map.Entry::getValue));

        StreamCut streamCutFirst = new StreamCutImpl(streamObj, positionMap0);
        StreamCut streamCutSecond = new StreamCutImpl(streamObj, positionMap1);
        Map<Stream, StreamCut> firstMarkStreamCut = Collections.singletonMap(streamObj, streamCutFirst);
        Map<Stream, StreamCut> secondMarkStreamCut = Collections.singletonMap(streamObj, streamCutSecond);

        return Arrays.asList(firstMarkStreamCut, secondMarkStreamCut);
    }

    private TimeWindow verifyEventsWithTimeBounds(Stream streamObj, EventStreamReader<Long> reader, EventRead<Long> event, TimeWindow currentTimeWindow) {
        // read all events and verify that all events are below the bounds
        while (event.getEvent() != null) {
            Long time = event.getEvent();
            log.info("timewindow = {} event = {}", currentTimeWindow, time);
            assertTrue(currentTimeWindow.getLowerTimeBound() == null || time >= currentTimeWindow.getLowerTimeBound());
            assertTrue(currentTimeWindow.getUpperTimeBound() == null || time <= currentTimeWindow.getUpperTimeBound());

            TimeWindow nextTimeWindow = reader.getCurrentTimeWindow(streamObj);
            assertTrue(currentTimeWindow.getLowerTimeBound() == null || nextTimeWindow.getLowerTimeBound() >= currentTimeWindow.getLowerTimeBound());
            assertTrue(currentTimeWindow.getUpperTimeBound() == null || nextTimeWindow.getUpperTimeBound() >= currentTimeWindow.getUpperTimeBound());
            currentTimeWindow = nextTimeWindow;

            event = reader.readNextEvent(READ_TIMEOUT.toMillis());
            if (event.isCheckpoint()) {
                event = reader.readNextEvent(READ_TIMEOUT.toMillis());
            }
        }
        return currentTimeWindow;
    }

    /**
     * Adds water marks to the watermarks queue.
     */
    private CompletableFuture<Void> fetchWatermarks(RevisionedStreamClient<Watermark> watermarkReader, LinkedBlockingQueue<Watermark> watermarks,
                                                    AtomicBoolean stop) {
        AtomicReference<Revision> revision = new AtomicReference<>(watermarkReader.fetchOldestRevision());
        return Futures.loop(() -> !stop.get(), () -> Futures.delayedTask(() -> {
            if (stop.get()) {
                return null;
            }
            Iterator<Map.Entry<Revision, Watermark>> marks = watermarkReader.readFrom(revision.get());
            if (marks.hasNext()) {
                Map.Entry<Revision, Watermark> next = marks.next();
                log.info("watermark = {}", next.getValue());
                watermarks.add(next.getValue());
                revision.set(next.getKey());
            }
            return null;
        }, Duration.ofSeconds(1), executorService()), executorService());
    }

    private CompletableFuture<Void> writeTxEvents(TransactionalEventStreamWriter<Long> writer, AtomicBoolean stopFlag) {
        AtomicInteger count = new AtomicInteger(0);
        return loop(() -> !stopFlag.get(), () -> Futures.delayedFuture(() -> {
            if (stopFlag.get()) {
                return CompletableFuture.completedFuture(null);
            }
            AtomicLong currentTime = new AtomicLong();
            Transaction<Long> txn = writer.beginTxn();
            return CompletableFuture.runAsync(() -> {
                try {
                    for (int i = 0; i < TOTAL_NUM_EVENTS; i++) {
                        currentTime.set(timer.incrementAndGet());
                        txn.writeEvent(count.toString(), currentTime.get());
                    }
                    txn.commit(currentTime.get());
                } catch (TxnFailedException e) {
                    throw new CompletionException(e);
                }
            });
        }, 1000L, executorService()), executorService());
    }

    private void scale(Controller controller, Stream streamObj, StreamConfiguration configuration) {
        // perform several scales
        int numOfSegments = configuration.getScalingPolicy().getMinNumSegments();
        double delta = 1.0 / numOfSegments;
        for (long segmentNumber = 0; segmentNumber < numOfSegments - 1; segmentNumber++) {
            double rangeLow = segmentNumber * delta;
            double rangeHigh = (segmentNumber + 1) * delta;
            double rangeMid = (rangeHigh + rangeLow) / 2;

            Map<Double, Double> map = new HashMap<>();
            map.put(rangeLow, rangeMid);
            map.put(rangeMid, rangeHigh);
            controller.scaleStream(streamObj, Collections.singletonList(segmentNumber), map, executorService()).getFuture().join();
        }
    }

    private EventStreamReader<String> createReader(ClientFactoryImpl clientFactory,
                                                   ReaderGroupManager readerGroupManager, String scope, String stream,
                                                   String readerGroupName, String readerName) {
        readerGroupManager.createReaderGroup(readerGroupName,
                ReaderGroupConfig
                        .builder()
                        .stream(Stream.of(scope, stream))
                        .automaticCheckpointIntervalMillis(2000)
                        .build());

        return clientFactory.createReader(readerName,
                readerGroupName,
                new UTF8StringSerializer(),
                ReaderConfig.builder().build());
    }

    // Reads the given number of events using the given reader and returns its position
    private Position readNEvents(EventStreamReader<String> reader, int num) {
        Position position = null;
        EventRead<String> eventRead = null;
        for (int q = 0; q < num; q++ ) {
            eventRead = reader.readNextEvent(READ_TIMEOUT.toMillis());
            Assert.assertEquals("Event written and read back don't match", EVENT, eventRead.getEvent());
        }
        position = eventRead.getPosition();
        return position;
    }

    // Writes the required number of events to the given stream without using transactions.
    private void writeEvents(String streamName, ClientFactoryImpl clientFactory) {
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
                new UTF8StringSerializer(),
                EventWriterConfig.builder().build());
        for (int i = 0; i < TOTAL_NUM_EVENTS;) {
            writer.writeEvent("", EVENT);
            i++;
        }
        writer.flush();
    }

    // Writes the required number of events to the given stream with using transactions.
    private void writeTransactionalEvents(String streamName, ClientFactoryImpl clientFactory) throws TxnFailedException {
        EventWriterConfig writerConfig = EventWriterConfig.builder().transactionTimeoutTime(TRANSACTION_TIMEOUT.toMillis()).build();
        @Cleanup
        TransactionalEventStreamWriter<String> txnWriter = clientFactory.createTransactionalEventWriter(streamName, new UTF8StringSerializer(),
                writerConfig);

        Transaction<String> transaction = txnWriter.beginTxn();
        for (int i = 0; i < TOTAL_NUM_EVENTS; i++) {
            transaction.writeEvent("0", EVENT);
        }
        transaction.commit();
    }

    // Reads the required number of events from the stream.
    private void readAllEvents(String streamName, ClientFactoryImpl clientFactory, ReaderGroupManager readerGroupManager,
                               String readerGroupName, String readerName) {
        readerGroupManager.createReaderGroup(readerGroupName,
                ReaderGroupConfig
                        .builder()
                        .stream(Stream.of(SCOPE, streamName))
                        .build());

        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader(readerName,
                readerGroupName,
                new UTF8StringSerializer(),
                ReaderConfig.builder().build());

        for (int q = 0; q < TOTAL_NUM_EVENTS; q++) {
            String eventRead = reader.readNextEvent(READ_TIMEOUT.toMillis()).getEvent();
            Assert.assertEquals("Event written and read back don't match", EVENT, eventRead);
        }
    }

    private static void createScopeStream(Controller controller, String scopeName, String streamName, StreamConfiguration streamConfig) {
        ClientConfig clientConfig = ClientConfig.builder().build();
        @Cleanup
        ConnectionPool cp = new ConnectionPoolImpl(clientConfig, new SocketConnectionFactoryImpl(clientConfig));
        @SuppressWarnings("resource") //Don't close the controller.
        StreamManager streamManager = new StreamManagerImpl(controller, cp);
        //create scope
        Boolean createScopeStatus = streamManager.createScope(scopeName);
        Assert.assertTrue(createScopeStatus);
        log.debug("Create scope status {}", createScopeStatus);
        //create stream
        Boolean createStreamStatus = streamManager.createStream(scopeName, streamName, streamConfig);
        Assert.assertTrue(createStreamStatus);
        log.debug("Create stream status {}", createStreamStatus);
    }
}

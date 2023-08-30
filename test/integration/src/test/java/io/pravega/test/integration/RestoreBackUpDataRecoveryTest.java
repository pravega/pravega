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
import io.pravega.client.watermark.WatermarkSerializer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.concurrent.Services;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.containers.ContainerRecoveryUtils;
import io.pravega.segmentstore.server.OperationLogFactory;
import io.pravega.segmentstore.server.containers.ContainerConfig;
import io.pravega.segmentstore.server.containers.DebugStreamSegmentContainer;
import io.pravega.segmentstore.server.containers.DebugStreamSegmentContainerTests;
import io.pravega.segmentstore.server.host.handler.IndexAppendProcessor;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.logs.DurableLogConfig;
import io.pravega.segmentstore.server.logs.DurableLogFactory;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.segmentstore.server.writer.WriterConfig;
import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperServiceRunner;
import io.pravega.segmentstore.storage.mocks.InMemoryDurableDataLogFactory;
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import io.pravega.segmentstore.storage.rolling.RollingStorage;
import io.pravega.shared.NameUtils;
import io.pravega.shared.watermarks.Watermark;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.ThreadPooledTestSuite;
import io.pravega.test.integration.utils.ControllerWrapper;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
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
public class RestoreBackUpDataRecoveryTest extends ThreadPooledTestSuite {
    private static final Duration TIMEOUT = Duration.ofMillis(100 * 1000);
    private static final Duration READ_TIMEOUT = Duration.ofMillis(1000);
    private static final Duration TRANSACTION_TIMEOUT = Duration.ofMillis(10000);

    /**
     * Write {@link #TOTAL_NUM_EVENTS} events to verify recovery.
     */
    private static final int TOTAL_NUM_EVENTS = 300;

    private static final String APPEND_FORMAT = "Segment_%s_Append_%d";
    private static final long DEFAULT_ROLLING_SIZE = (int) (APPEND_FORMAT.length() * 1.5);

    private static final Random RANDOM = new Random(1234);

    /**
     * Scope and streams to read and write events.
     */
    private static final String SCOPE = "testMetricsScope";
    private static final String STREAM1 = "testMetricsStream" + RANDOM.nextInt(Integer.MAX_VALUE);
    private static final String EVENT = "12345";
    private static final ContainerConfig DEFAULT_CONFIG = ContainerConfig
            .builder()
            .with(ContainerConfig.SEGMENT_METADATA_EXPIRATION_SECONDS, 10 * 60)
            .with(ContainerConfig.DATA_INTEGRITY_CHECKS_ENABLED, true)
            .build();
    // Configurations for DebugSegmentContainer
    private static final ContainerConfig CONTAINER_CONFIG = ContainerConfig
            .builder()
            .with(ContainerConfig.SEGMENT_METADATA_EXPIRATION_SECONDS, (int) DEFAULT_CONFIG.getSegmentMetadataExpiration().getSeconds())
            .with(ContainerConfig.DATA_INTEGRITY_CHECKS_ENABLED, true)
            .build();

    // DL config that can be used to simulate no DurableLog truncations.
    private static final DurableLogConfig NO_TRUNCATIONS_DURABLE_LOG_CONFIG = DurableLogConfig
            .builder()
            .with(DurableLogConfig.CHECKPOINT_MIN_COMMIT_COUNT, 10000)
            .with(DurableLogConfig.CHECKPOINT_COMMIT_COUNT, 50000)
            .with(DurableLogConfig.CHECKPOINT_TOTAL_COMMIT_LENGTH, 1024 * 1024 * 1024L)
            .build();

    private final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(1);
    private final StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(scalingPolicy).build();

    private final AtomicLong timer = new AtomicLong();

    private InMemoryStorageFactory storageFactory;
    private InMemoryDurableDataLogFactory dataLogFactory;

    @After
    public void tearDown() throws Exception {
        if (this.storageFactory != null) {
            this.storageFactory.close();
        }
        if (this.dataLogFactory != null) {
            this.dataLogFactory.close();
        }
        timer.set(0);
    }

    @Override
    protected int getThreadPoolSize() {
        return 100;
    }

    /**
     * Sets up a new BookKeeper & ZooKeeper.
     */
    private static class BookKeeperRunner implements AutoCloseable {
        private final int bkPort;
        private final BookKeeperServiceRunner bookKeeperServiceRunner;
        private final AtomicReference<BookKeeperConfig> bkConfig = new AtomicReference<>();
        private final AtomicReference<CuratorFramework> zkClient = new AtomicReference<>();
        private final AtomicReference<BookKeeperServiceRunner> bkService = new AtomicReference<>();

        BookKeeperRunner(int instanceId, int bookieCount) throws Exception {
            bkPort = TestUtils.getAvailableListenPort();
            val bookiePorts = new ArrayList<Integer>();
            for (int i = 0; i < bookieCount; i++) {
                bookiePorts.add(TestUtils.getAvailableListenPort());
            }
            this.bookKeeperServiceRunner = BookKeeperServiceRunner.builder()
                    .startZk(true)
                    .zkPort(bkPort)
                    .ledgersPath("/pravega/bookkeeper/ledgers")
                    .bookiePorts(bookiePorts)
                    .build();
            try {
                this.bookKeeperServiceRunner.startAll();
            } catch (Exception e) {
                log.error("Exception occurred while starting bookKeeper service.", e);
                this.close();
                throw e;
            }
            bkService.set(this.bookKeeperServiceRunner);

            // Create a ZKClient with a unique namespace.
            String baseNamespace = "pravega/" + instanceId + "_" + Long.toHexString(System.nanoTime());
            this.zkClient.set(CuratorFrameworkFactory
                    .builder()
                    .connectString("localhost:" + bkPort)
                    .namespace(baseNamespace)
                    .retryPolicy(new ExponentialBackoffRetry(1000, 5))
                    .build());

            this.zkClient.get().start();

            String logMetaNamespace = "segmentstore/containers" + instanceId;
            this.bkConfig.set(BookKeeperConfig
                    .builder()
                    .with(BookKeeperConfig.ZK_ADDRESS, "localhost:" + bkPort)
                    .with(BookKeeperConfig.ZK_METADATA_PATH, logMetaNamespace)
                    .with(BookKeeperConfig.BK_LEDGER_PATH, "/pravega/bookkeeper/ledgers")
                    .with(BookKeeperConfig.BK_ENSEMBLE_SIZE, bookieCount)
                    .with(BookKeeperConfig.BK_WRITE_QUORUM_SIZE, bookieCount)
                    .with(BookKeeperConfig.BK_ACK_QUORUM_SIZE, bookieCount)
                    .with(BookKeeperConfig.BK_WRITE_TIMEOUT, 1000)
                    .build());
        }

        @Override
        public void close() throws Exception {
            val process = this.bkService.getAndSet(null);
            if (process != null) {
                process.close();
            }

            val bk = this.bookKeeperServiceRunner;
            if (bk != null) {
                bk.close();
            }

            val zkClient = this.zkClient.getAndSet(null);
            if (zkClient != null) {
                zkClient.close();
            }
        }
    }

    /**
     * Creates a segment store.
     */
    private static class SegmentStoreRunner implements AutoCloseable {
        private final int servicePort = TestUtils.getAvailableListenPort();
        private final ServiceBuilder serviceBuilder;
        private final PravegaConnectionListener server;
        private final StreamSegmentStore streamSegmentStore;
        private final TableStore tableStore;

        SegmentStoreRunner(StorageFactory storageFactory, InMemoryDurableDataLogFactory dataLogFactory, int containerCount)
                throws DurableDataLogException {
            ServiceBuilderConfig.Builder configBuilder = ServiceBuilderConfig
                    .builder()
                    .include(ServiceConfig.builder()
                            .with(ServiceConfig.CONTAINER_COUNT, containerCount)
                            .with(WriterConfig.MIN_READ_TIMEOUT_MILLIS, 100L)
                            .with(WriterConfig.MAX_READ_TIMEOUT_MILLIS, 250L));
            if (storageFactory != null) {
                if (dataLogFactory != null) {
                    this.serviceBuilder = ServiceBuilder.newInMemoryBuilder(configBuilder.build())
                            .withStorageFactory(setup -> storageFactory)
                            .withDataLogFactory(setup -> dataLogFactory);
                } else {
                    this.serviceBuilder = ServiceBuilder.newInMemoryBuilder(configBuilder.build())
                            .withStorageFactory(setup -> storageFactory);
                }
            } else {
                this.serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
            }
            this.serviceBuilder.initialize();
            this.streamSegmentStore = this.serviceBuilder.createStreamSegmentService();
            this.tableStore = this.serviceBuilder.createTableStoreService();
            this.server = new PravegaConnectionListener(false, servicePort, this.streamSegmentStore, this.tableStore,
                    this.serviceBuilder.getLowPriorityExecutor(), new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), streamSegmentStore));
            this.server.startListening();
        }

        @Override
        public void close() {
            this.server.close();
            this.serviceBuilder.close();
        }
    }

    /**
     * Creates a controller instance and runs it.
     */
    private static class ControllerRunner implements AutoCloseable {
        private final int controllerPort = TestUtils.getAvailableListenPort();
        private final String serviceHost = "localhost";
        private final ControllerWrapper controllerWrapper;
        private final Controller controller;
        private final URI controllerURI = URI.create("tcp://" + serviceHost + ":" + controllerPort);

        ControllerRunner(int bkPort, int servicePort, int containerCount) {
            this.controllerWrapper = new ControllerWrapper("localhost:" + bkPort, false,
                    controllerPort, serviceHost, servicePort, containerCount);
            this.controllerWrapper.awaitRunning();
            this.controller = controllerWrapper.getController();
        }

        @Override
        public void close() throws Exception {
            this.controller.close();
            this.controllerWrapper.close();
        }
    }

    /**
     * Creates a client to read and write events.
     */
    private static class ClientRunner implements AutoCloseable {
        private final ConnectionFactory connectionFactory;
        private final ClientFactoryImpl clientFactory;
        private final ReaderGroupManager readerGroupManager;

        ClientRunner(ControllerRunner controllerRunner) {
            this.connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder()
                    .controllerURI(controllerRunner.controllerURI).build());
            this.clientFactory = new ClientFactoryImpl(SCOPE, controllerRunner.controller, connectionFactory);
            this.readerGroupManager = new ReaderGroupManagerImpl(SCOPE, controllerRunner.controller, clientFactory);
        }

        @Override
        public void close() {
            this.readerGroupManager.close();
            this.clientFactory.close();
            this.connectionFactory.close();
        }
    }

    /**
     * Creates a Pravega instance.
     */
    private static class PravegaRunner implements AutoCloseable {
        private final int containerCount;
        private BookKeeperRunner bookKeeperRunner;
        private SegmentStoreRunner segmentStoreRunner;
        private ControllerRunner controllerRunner;

        PravegaRunner(int instanceId, int bookieCount, int containerCount, StorageFactory storageFactory) throws Exception {
            this.containerCount = containerCount;
            this.bookKeeperRunner = new BookKeeperRunner(instanceId, bookieCount);
            restartControllerAndSegmentStore(storageFactory, null);
        }

        public void restartControllerAndSegmentStore(StorageFactory storageFactory, InMemoryDurableDataLogFactory dataLogFactory)
                throws DurableDataLogException {
            this.segmentStoreRunner = new SegmentStoreRunner(storageFactory, dataLogFactory, this.containerCount);
            this.controllerRunner = new ControllerRunner(this.bookKeeperRunner.bkPort, this.segmentStoreRunner.servicePort, containerCount);
        }

        @Override
        public void close() throws Exception {
            this.controllerRunner.close();
            this.segmentStoreRunner.close();
            this.bookKeeperRunner.close();
        }
    }

    /**
     * Tests the data recovery scenario with just one segment container. Segments recovery is attained using just one
     * debug segment container.
     *  What test does, step by step:
     *  1.  Starts Pravega locally with just one segment container.
     *  2.  Writes {@link #TOTAL_NUM_EVENTS} events.
     *  3.  Waits for DurableLog to be entirely flushed to the long term storage.
     *  4.  Shuts down the controller, segment store and bookeeper/zookeeper.
     *  5.  Creates back up of container metadata segment and its attribute segment before deleting them from the Long Term Storage .
     *  6.  Starts one debug segment container using a new bookeeper/zookeeper and the Long Term Storage.
     *  7.  Re-creates the container metadata segment in DurableLog and let's it to be flushed to the Long Term Storage.
     *  8.  Updates core attributes of segments in the new container metadata segment by using details from the back up of old container metadata segment.
     *  9.  Starts segment store and controller.
     *  10. Reads all events.
     * @throws Exception    In case of an exception occurred while execution.
     */
    @Test(timeout = 90000)
    public void testDurableDataLogFailRecoverySingleContainer() throws Exception {
        testRecovery(1, 1, false);
    }

    /**
     * Tests the data recovery scenario with multiple segment containers. Segments recovery is attained using multiple
     * debug segment containers as well.
     *  What test does, step by step:
     *  1.  Starts Pravega locally with just 4 segment containers.
     *  2.  Writes {@link #TOTAL_NUM_EVENTS} events.
     *  3.  Waits for DurableLog to be entirely flushed to the long term storage.
     *  4.  Shuts down the controller, segment store and bookeeper/zookeeper.
     *  5.  Creates back up of container metadata segment and its attribute segment before deleting them from the Long Term Storage .
     *  6.  Starts 4 debug segment containers using a new bookeeper/zookeeper and the Long Term Storage.
     *  7.  Re-creates the container metadata segments in DurableLog and lets them to be flushed to the Long Term Storage.
     *  8.  Updates core attributes of segments in the new container metadata segment by using details from the back up of old container metadata segment.
     *  9.  Starts segment store and controller.
     *  10. Reads all events.
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
     *  3.  Waits for DurableLog to be entirely flushed to the long term storage.
     *  4.  Shuts down the controller, segment store and bookeeper/zookeeper.
     *  5.  Creates back up of container metadata segment and its attribute segment before deleting them from the Long Term Storage .
     *  6.  Starts 4 debug segment containers using a new bookeeper/zookeeper and the Long Term Storage.
     *  7.  Re-creates the container metadata segments in DurableLog and lets them to be flushed to the Long Term Storage.
     *  8.  Updates core attributes of segments in the new container metadata segment by using details from the back up of old container metadata segment.
     *  9.  Starts segment store and controller.
     *  10. Reads all events.
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
        this.storageFactory = new InMemoryStorageFactory(executorService());
        log.info("Created a long term storage.");

        // Start a new BK & ZK, segment store and controller
        @Cleanup
        PravegaRunner pravegaRunner = new PravegaRunner(instanceId++, bookieCount, containerCount, this.storageFactory);

        // Create a stream for writing data
        createScopeStream(pravegaRunner.controllerRunner.controller, SCOPE, STREAM1);
        log.info("Created stream '{}'.", STREAM1);

        // Create a client to write events.
        try (val clientRunner = new ClientRunner(pravegaRunner.controllerRunner)) {
            // Write events.
            writeEventsToStream(clientRunner.clientFactory, withTransaction);
        }

        pravegaRunner.controllerRunner.close(); // Shut down the controller

        // Flush DurableLog to Long Term Storage
        flushToStorage(pravegaRunner.segmentStoreRunner.serviceBuilder);

        pravegaRunner.segmentStoreRunner.close(); // Shutdown SegmentStore
        pravegaRunner.bookKeeperRunner.close(); // Shutdown BookKeeper & ZooKeeper
        log.info("SegmentStore, BookKeeper & ZooKeeper shutdown");

        // Get the long term storage from the running pravega instance
        @Cleanup
        Storage storage = new AsyncStorageWrapper(new RollingStorage(this.storageFactory.createSyncStorage(),
                new SegmentRollingPolicy(DEFAULT_ROLLING_SIZE)), executorService());

        Map<Integer, String> backUpMetadataSegments = ContainerRecoveryUtils.createBackUpMetadataSegments(storage, containerCount,
                executorService(), TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // start a new BookKeeper and ZooKeeper.
        pravegaRunner.bookKeeperRunner = new BookKeeperRunner(instanceId++, bookieCount);
        createBookKeeperLogFactory();
        log.info("Started a new BookKeeper and ZooKeeper.");

        // Recover segments
        runRecovery(containerCount, storage, backUpMetadataSegments);

        // Start a new segment store and controller
        pravegaRunner.restartControllerAndSegmentStore(this.storageFactory, this.dataLogFactory);
        log.info("Started segment store and controller again.");

        // Create the client with new controller.
        try (val clientRunner = new ClientRunner(pravegaRunner.controllerRunner)) {
            // Try reading all events again to verify that the recovery was successful.
            readEventsFromStream(clientRunner.clientFactory, clientRunner.readerGroupManager);
            log.info("Read all events again to verify that segments were recovered.");
        }
    }

    private void flushToStorage(ServiceBuilder serviceBuilder) throws Exception {
        ServiceBuilder.ComponentSetup componentSetup = new ServiceBuilder.ComponentSetup(serviceBuilder);
        ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int containerId = 0; containerId < componentSetup.getContainerRegistry().getContainerCount(); containerId++) {
            futures.add(componentSetup.getContainerRegistry().getContainer(containerId).flushToStorage(TIMEOUT));
        }
        Futures.allOf(futures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void createBookKeeperLogFactory() throws DurableDataLogException {
        this.dataLogFactory = new InMemoryDurableDataLogFactory(executorService());
        this.dataLogFactory.initialize();
    }

    // Creates debug segment container instances, puts them in a map and returns it.
    private Map<Integer, DebugStreamSegmentContainer> startDebugSegmentContainers(DebugStreamSegmentContainerTests.TestContext
                                                                                          context, int containerCount,
                                                                                  InMemoryDurableDataLogFactory dataLogFactory,
                                                                                  StorageFactory storageFactory) throws Exception {
        // Start a debug segment container corresponding to the given container Id and put it in the Hashmap with the Id.
        Map<Integer, DebugStreamSegmentContainer> debugStreamSegmentContainerMap = new HashMap<>();
        OperationLogFactory localDurableLogFactory = new DurableLogFactory(NO_TRUNCATIONS_DURABLE_LOG_CONFIG, dataLogFactory, executorService());

        // Create a debug segment container instances using a
        for (int containerId = 0; containerId < containerCount; containerId++) {
            DebugStreamSegmentContainerTests.MetadataCleanupContainer debugStreamSegmentContainer = new
                    DebugStreamSegmentContainerTests.MetadataCleanupContainer(containerId, CONTAINER_CONFIG, localDurableLogFactory,
                    context.readIndexFactory, context.attributeIndexFactory, context.writerFactory, storageFactory,
                    context.getDefaultExtensions(), executorService());

            Services.startAsync(debugStreamSegmentContainer, executorService()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            debugStreamSegmentContainerMap.put(containerId, debugStreamSegmentContainer);
        }
        return debugStreamSegmentContainerMap;
    }

    // Closes the debug segment container instances in the given map after waiting for the metadata segment to be flushed to
    // the given storage.
    private void stopDebugSegmentContainers(int containerCount, Map<Integer, DebugStreamSegmentContainer> debugStreamSegmentContainerMap) {
        for (int containerId = 0; containerId < containerCount; containerId++) {
            debugStreamSegmentContainerMap.get(containerId).close();
        }
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
     * stopped. Durable data log is erased and restored. It's validated that readers are able to read rest of the unread
     * events.
     *  What test does, step by step:
     *  1. Starts Pravega locally with just 4 segment containers.
     *  2. Writes {@link #TOTAL_NUM_EVENTS} events.
     *  3. Waits for all segments created to be flushed to the long term storage.
     *  4. Let a reader read N number of events.
     *  5. Shuts down the controller, segment store and bookeeper/zookeeper.
     *  6. Creates back up of container metadata segment and its attribute segment before deleting them from the Long Term Storage .
     *  7. Starts 4 debug segment containers using a new bookeeper/zookeeper and the Long Term Storage.
     *  8. Re-creates the container metadata segments in DurableLog and lets them to be flushed to the Long Term Storage.
     *  9. Updates core attributes of segments in the new container metadata segment by using details from the back up of old container metadata segment.
     *  10. Starts segment store and controller.
     *  11. Let the reader read rest of the 10-N number of events.
     * @throws Exception    In case of an exception occurred while execution.
     */
    @Test(timeout = 180000)
    public void testDurableDataLogFailRecoveryReadersPaused() throws Exception {
        int instanceId = 0;
        int bookieCount = 1;
        int containerCount = 4;
        int eventsReadCount = RANDOM.nextInt(TOTAL_NUM_EVENTS);
        String testReader = "readerDRIntegrationTest";
        String testReaderGroup = "readerGroupDRIntegrationTest";

        // Creating a long term storage only once here.
        this.storageFactory = new InMemoryStorageFactory(executorService());
        log.info("Created a long term storage.");

        // Start a new BK & ZK, segment store and controller
        @Cleanup
        PravegaRunner pravegaRunner = new PravegaRunner(instanceId++, bookieCount, containerCount, this.storageFactory);

        // Create a stream for writing data
        createScopeStream(pravegaRunner.controllerRunner.controller, SCOPE, STREAM1);
        log.info("Created stream '{}'", STREAM1);

        // Create a client to write events.
        try (val clientRunner = new ClientRunner(pravegaRunner.controllerRunner)) {
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

        pravegaRunner.controllerRunner.close(); // Shut down the controller

        // Flush DurableLog to Long Term Storage
        flushToStorage(pravegaRunner.segmentStoreRunner.serviceBuilder);

        pravegaRunner.segmentStoreRunner.close(); // Shutdown SegmentStore
        pravegaRunner.bookKeeperRunner.close(); // Shutdown BookKeeper & ZooKeeper
        log.info("SegmentStore, BookKeeper & ZooKeeper shutdown");

        // Get the long term storage from the running pravega instance
        @Cleanup
        Storage storage = new AsyncStorageWrapper(new RollingStorage(this.storageFactory.createSyncStorage(),
                new SegmentRollingPolicy(DEFAULT_ROLLING_SIZE)), executorService());

        Map<Integer, String> backUpMetadataSegments = ContainerRecoveryUtils.createBackUpMetadataSegments(storage, containerCount,
                executorService(), TIMEOUT).join();

        // start a new BookKeeper and ZooKeeper.
        pravegaRunner.bookKeeperRunner = new BookKeeperRunner(instanceId++, bookieCount);
        createBookKeeperLogFactory();
        log.info("Started a new BookKeeper and ZooKeeper.");

        // Recover segments
        runRecovery(containerCount, storage, backUpMetadataSegments);

        // Start a new segment store and controller
        pravegaRunner.restartControllerAndSegmentStore(this.storageFactory, this.dataLogFactory);
        log.info("Started segment store and controller again.");

        // Create the client with new controller.
        try (val clientRunner = new ClientRunner(pravegaRunner.controllerRunner)) {

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

    private void runRecovery(int containerCount, Storage storage, Map<Integer, String> backUpMetadataSegments) throws Exception {
        // Create the environment for DebugSegmentContainer.
        @Cleanup
        DebugStreamSegmentContainerTests.TestContext context = DebugStreamSegmentContainerTests.createContext(executorService());

        // create debug segment container instances using new new dataLog and old storage.
        Map<Integer, DebugStreamSegmentContainer> debugStreamSegmentContainerMap = startDebugSegmentContainers(context,
                containerCount, this.dataLogFactory, this.storageFactory);

        // List segments from storage and recover them using debug segment container instance.
        ContainerRecoveryUtils.recoverAllSegments(storage, debugStreamSegmentContainerMap, executorService(), TIMEOUT);

        // Update core attributes from the backUp Metadata segments
        ContainerRecoveryUtils.updateCoreAttributes(backUpMetadataSegments, debugStreamSegmentContainerMap, executorService(), TIMEOUT);

        // Waits for metadata segments to be flushed to Long Term Storage and then stops the debug segment containers
        stopDebugSegmentContainers(containerCount, debugStreamSegmentContainerMap);
        log.info("Segments have been recovered.");
    }

    /**
     * Tests the data recovery scenario with watermarking events.
     *  What test does, step by step:
     *  1. Starts Pravega locally with just 4 segment containers.
     *  2. Writes {@link #TOTAL_NUM_EVENTS} events to a segment with watermarks.
     *  3. Waits for all segments created to be flushed to the long term storage.
     *  4. Shuts down the controller, segment store and bookeeper/zookeeper.
     *  5. Creates back up of container metadata segment and its attribute segment before deleting them from the Long Term Storage .
     *  6. Starts 4 debug segment containers using a new bookeeper/zookeeper and the Long Term Storage.
     *  7. Re-creates the container metadata segments in DurableLog and lets them to be flushed to the Long Term Storage.
     *  8. Starts segment store and controller.
     *  9. Read all events and verify that all events are below the bounds.
     * @throws Exception    In case of an exception occurred while execution.
     */
    @Test(timeout = 180000)
    public void testDurableDataLogFailRecoveryWatermarking() throws Exception {
        int instanceId = 0;
        int bookieCount = 1;
        int containerCount = 4;
        String readerGroup = "rgTx";

        // Creating a long term storage only once here.
        this.storageFactory = new InMemoryStorageFactory(executorService());
        log.info("Created a long term storage.");

        // Start a new BK & ZK, segment store and controller
        @Cleanup
        PravegaRunner pravegaRunner = new PravegaRunner(instanceId++, bookieCount, containerCount, this.storageFactory);

        // Create a scope and a stream
        createScopeStream(pravegaRunner.controllerRunner.controller, SCOPE, STREAM1);

        // Create a client to write events.
        @Cleanup
        ClientRunner clientRunner = new ClientRunner(pravegaRunner.controllerRunner);

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
        scale(pravegaRunner.controllerRunner.controller, streamObj, config);

        // get watermarks
        LinkedBlockingQueue<Watermark> watermarks = getWatermarks(pravegaRunner, stopFlag, writerFuture);

        pravegaRunner.controllerRunner.close(); // Shut down the controller

        // Flush DurableLog to Long Term Storage
        flushToStorage(pravegaRunner.segmentStoreRunner.serviceBuilder);

        pravegaRunner.segmentStoreRunner.close(); // Shutdown SegmentStore
        pravegaRunner.bookKeeperRunner.close(); // Shutdown BookKeeper & ZooKeeper
        log.info("SegmentStore, BookKeeper & ZooKeeper shutdown");

        // Get the long term storage from the running pravega instance
        @Cleanup
        Storage storage = new AsyncStorageWrapper(new RollingStorage(this.storageFactory.createSyncStorage(),
                new SegmentRollingPolicy(DEFAULT_ROLLING_SIZE)), executorService());

        Map<Integer, String> backUpMetadataSegments = ContainerRecoveryUtils.createBackUpMetadataSegments(storage, containerCount,
                executorService(), TIMEOUT).join();

        // start a new BookKeeper and ZooKeeper.
        pravegaRunner.bookKeeperRunner = new BookKeeperRunner(instanceId++, bookieCount);
        createBookKeeperLogFactory();
        log.info("Started a new BookKeeper and ZooKeeper.");

        // Recover segments
        runRecovery(containerCount, storage, backUpMetadataSegments);

        // Start a new segment store and controller
        pravegaRunner.restartControllerAndSegmentStore(this.storageFactory, this.dataLogFactory);
        log.info("Started segment store and controller again.");

        // Create the client with new controller.
        @Cleanup
        ClientRunner newClientRunner = new ClientRunner(pravegaRunner.controllerRunner);

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
    private LinkedBlockingQueue<Watermark> getWatermarks(PravegaRunner pravegaRunner, AtomicBoolean stopFlag,
                                                         CompletableFuture<Void> writerFuture) throws Exception {
        @Cleanup
        SynchronizerClientFactory syncClientFactory = SynchronizerClientFactory.withScope(SCOPE,
                ClientConfig.builder().controllerURI(pravegaRunner.controllerRunner.controllerURI).build());

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
        for (int q = 0; q < num;) {
            eventRead = reader.readNextEvent(READ_TIMEOUT.toMillis());
            Assert.assertEquals("Event written and read back don't match", EVENT, eventRead.getEvent());
            q++;
        }
        position = eventRead.getPosition();
        return position;
    }

    // Creates the given scope and stream using the given controller instance.
    private void createScopeStream(Controller controller, String scopeName, String streamName) {
        ClientConfig clientConfig = ClientConfig.builder().build();
        try (ConnectionPool cp = new ConnectionPoolImpl(clientConfig, new SocketConnectionFactoryImpl(clientConfig));
             StreamManager streamManager = new StreamManagerImpl(controller, cp)) {
            //create scope
            Boolean createScopeStatus = streamManager.createScope(scopeName);
            log.info("Create scope status {}", createScopeStatus);
            //create stream
            Boolean createStreamStatus = streamManager.createStream(scopeName, streamName, config);
            log.info("Create stream status {}", createStreamStatus);
        }
    }

    // Writes the required number of events to the given stream without using transactions.
    private void writeEvents(String streamName, ClientFactoryImpl clientFactory) {
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
                new UTF8StringSerializer(),
                EventWriterConfig.builder().build());
        for (int i = 0; i < TOTAL_NUM_EVENTS;) {
            writer.writeEvent("", EVENT);
            i++;
        }
        writer.flush();
        writer.close();
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
        txnWriter.close();
    }

    // Reads the required number of events from the stream.
    private void readAllEvents(String streamName, ClientFactoryImpl clientFactory, ReaderGroupManager readerGroupManager,
                               String readerGroupName, String readerName) {
        readerGroupManager.createReaderGroup(readerGroupName,
                ReaderGroupConfig
                        .builder()
                        .stream(Stream.of(SCOPE, streamName))
                        .build());

        EventStreamReader<String> reader = clientFactory.createReader(readerName,
                readerGroupName,
                new UTF8StringSerializer(),
                ReaderConfig.builder().build());

        for (int q = 0; q < TOTAL_NUM_EVENTS;) {
            String eventRead = reader.readNextEvent(READ_TIMEOUT.toMillis()).getEvent();
            Assert.assertEquals("Event written and read back don't match", EVENT, eventRead);
            q++;
        }
        reader.close();
    }
}

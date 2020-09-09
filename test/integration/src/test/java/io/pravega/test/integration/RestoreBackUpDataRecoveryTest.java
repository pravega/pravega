/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
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
import io.pravega.common.Exceptions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.concurrent.Services;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.SegmentStoreWithSegmentTracker;
import io.pravega.segmentstore.server.containers.ContainerRecoveryUtils;
import io.pravega.segmentstore.server.OperationLogFactory;
import io.pravega.segmentstore.server.containers.ContainerConfig;
import io.pravega.segmentstore.server.containers.DebugStreamSegmentContainer;
import io.pravega.segmentstore.server.containers.DebugStreamSegmentContainerTests;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.logs.DurableLogConfig;
import io.pravega.segmentstore.server.logs.DurableLogFactory;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperServiceRunner;
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import io.pravega.segmentstore.storage.rolling.RollingStorage;
import io.pravega.shared.NameUtils;
import io.pravega.shared.watermarks.Watermark;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.ThreadPooledTestSuite;
import io.pravega.test.integration.demo.ControllerWrapper;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.SECONDS;
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
    private static final Duration READ_TIMEOUT = Duration.ofMillis(500);
    private static final Duration TRANSACTION_TIMEOUT = Duration.ofMillis(10000);

    /**
     * Write 300 events to different segments.
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
    private static final String STREAM2 = "testMetricsStream" + RANDOM.nextInt(Integer.MAX_VALUE);
    private static final String EVENT = "12345";
    private static final int NUM_EVENTS = 100;
    private static final ContainerConfig DEFAULT_CONFIG = ContainerConfig
            .builder()
            .with(ContainerConfig.SEGMENT_METADATA_EXPIRATION_SECONDS, 10 * 60)
            .build();
    // Configurations for DebugSegmentContainer
    private static final ContainerConfig CONTAINER_CONFIG = ContainerConfig
            .builder()
            .with(ContainerConfig.SEGMENT_METADATA_EXPIRATION_SECONDS, (int) DEFAULT_CONFIG.getSegmentMetadataExpiration().getSeconds())
            .with(ContainerConfig.MAX_ACTIVE_SEGMENT_COUNT, 100)
            .build();
    private static final DurableLogConfig DURABLE_LOG_CONFIG = DurableLogConfig
            .builder()
            .with(DurableLogConfig.CHECKPOINT_MIN_COMMIT_COUNT, 1)
            .with(DurableLogConfig.CHECKPOINT_COMMIT_COUNT, 10)
            .with(DurableLogConfig.CHECKPOINT_TOTAL_COMMIT_LENGTH, 10L * 1024 * 1024)
            .build();

    private final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(1);
    private final StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(scalingPolicy).build();

    private BookKeeperRunner bookKeeperRunner = null;
    private StorageFactory storageFactory;
    private BookKeeperLogFactory dataLogFactory;
    private AtomicLong timer = new AtomicLong();

    @After
    public void tearDown() throws Exception {
        if (this.dataLogFactory != null) {
            this.dataLogFactory.close();
        }

        if (this.bookKeeperRunner != null) {
            this.bookKeeperRunner.close();
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
    private static class SegmentStoreRunner {
        private final int servicePort = TestUtils.getAvailableListenPort();
        private final ServiceBuilder serviceBuilder;
        private final SegmentStoreWithSegmentTracker segmentsTracker;
        private final PravegaConnectionListener server;

        SegmentStoreRunner(StorageFactory storageFactory, BookKeeperLogFactory dataLogFactory, int containerCount)
                throws DurableDataLogException {
            ServiceBuilderConfig.Builder configBuilder = ServiceBuilderConfig
                    .builder()
                    .include(ServiceConfig.builder()
                            .with(ServiceConfig.CONTAINER_COUNT, containerCount));
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
            this.segmentsTracker = new SegmentStoreWithSegmentTracker(serviceBuilder.createStreamSegmentService(),
                    serviceBuilder.createTableStoreService());
            this.server = new PravegaConnectionListener(false, servicePort, this.segmentsTracker, this.segmentsTracker,
                    this.serviceBuilder.getLowPriorityExecutor());
            this.server.startListening();
        }

        public void close() {
            this.server.close();
            this.serviceBuilder.close();
        }
    }

    /**
     * Creates a controller instance and runs it.
     */
    private static class ControllerRunner {
        private final int controllerPort = TestUtils.getAvailableListenPort();
        private final String serviceHost = "localhost";
        private final ControllerWrapper controllerWrapper;
        private final Controller controller;
        private final URI controllerURI = URI.create("tcp://" + serviceHost + ":" + controllerPort);

        ControllerRunner(int bkPort, int servicePort, int containerCount) throws InterruptedException {
            this.controllerWrapper = new ControllerWrapper("localhost:" + bkPort, false,
                    controllerPort, serviceHost, servicePort, containerCount);
            this.controllerWrapper.awaitRunning();
            this.controller = controllerWrapper.getController();
        }

        public void close() throws Exception {
            this.controller.close();
            this.controllerWrapper.close();
        }
    }

    /**

     * Creates a client to read and write events.
     */
    private static class ClientRunner {
        private final ConnectionFactory connectionFactory;
        private final ClientFactoryImpl clientFactory;
        private final ReaderGroupManager readerGroupManager;

        ClientRunner(ControllerRunner controllerRunner) {
            this.connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder()
                    .controllerURI(controllerRunner.controllerURI).build());
            this.clientFactory = new ClientFactoryImpl(SCOPE, controllerRunner.controller, connectionFactory);
            this.readerGroupManager = new ReaderGroupManagerImpl(SCOPE, controllerRunner.controller, clientFactory);
        }

        public void close() {
            this.readerGroupManager.close();
            this.clientFactory.close();
            this.connectionFactory.close();
        }
    }

    /**
     * Tests the data recovery scenario with just one segment container. Segments recovery is attained using just one
     * debug segment container.
     *  What test does, step by step:
     *  1. Starts Pravega locally with just one segment container.
     *  2. Writes 300 events to two different segments.
     *  3. Waits for all segments created to be flushed to the long term storage.
     *  4. Shuts down the controller, segment store and bookeeper/zookeeper.
     *  5. Deletes container metadata segment and its attribute segment from the old LTS.
     *  5. Starts just one debug segment container using a new bookeeper/zookeeper and the old LTS.
     *  6. Re-creates the container metadata segment in Tier1 and let's it flushed to the LTS.
     *  7. Starts segment store and controller.
     *  8. Reads all 600 events again.
     * @throws Exception    In case of an exception occurred while execution.
     */
    @Test(timeout = 180000)
    public void testDurableDataLogFailRecoverySingleContainer() throws Exception {
        testRecovery(1, 1, false);
    }

    /**
     * Tests the data recovery scenario with multiple segment containers. Segments recovery is attained using multiple
     * debug segment containers as well.
     *  What test does, step by step:
     *  1. Starts Pravega locally with just 4 segment containers.
     *  2. Writes 300 events to two different segments.
     *  3. Waits for all segments created to be flushed to the long term storage.
     *  4. Shuts down the controller, segment store and bookeeper/zookeeper.
     *  5. Deletes container metadata segment and its attribute segment from the old LTS.
     *  5. Starts 4 debug segment containers using a new bookeeper/zookeeper and the old LTS.
     *  6. Re-creates the container metadata segment in Tier1 and let's it flushed to the LTS.
     *  7. Starts segment store and controller.
     *  8. Reads all 600 events again.
     * @throws Exception    In case of an exception occurred while execution.
     */
    @Test(timeout = 180000)
    public void testDurableDataLogFailRecoveryMultipleContainers() throws Exception {
        testRecovery(4, 3, false);
    }

    /**
     * Tests the data recovery scenario with transactional writer. Events are written using a transactional writer.
     *  What test does, step by step:
     *  1. Starts Pravega locally with just 4 segment containers.
     *  2. Writes 300 events in the form of transactions to two different segments.
     *  3. Waits for all segments created to be flushed to the long term storage.
     *  4. Shuts down the controller, segment store and bookeeper/zookeeper.
     *  5. Deletes container metadata segment and its attribute segment from the old LTS.
     *  5. Starts 4 debug segment containers using a new bookeeper/zookeeper and the old LTS.
     *  6. Re-creates the container metadata segment in Tier1 and let's it flushed to the LTS.
     *  7. Starts segment store and controller.
     *  8. Reads all 600 events again.
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
        this.bookKeeperRunner = new BookKeeperRunner(instanceId++, bookieCount);
        @Cleanup
        SegmentStoreRunner segmentStoreRunner = new SegmentStoreRunner(this.storageFactory, null, containerCount);
        @Cleanup
        ControllerRunner controllerRunner = new ControllerRunner(this.bookKeeperRunner.bkPort, segmentStoreRunner.servicePort,
                containerCount);

        // Create two streams for writing data onto two different segments
        createScopeStream(controllerRunner.controller, SCOPE, STREAM1);
        createScopeStream(controllerRunner.controller, SCOPE, STREAM2);
        log.info("Created two streams.");

        // Create a client to read and write events.
        @Cleanup
        ClientRunner clientRunner = new ClientRunner(controllerRunner);
        // Write events to the streams.
        writeEventsToStreams(clientRunner.clientFactory, withTransaction);

        // Verify events have been written by reading from the streams.
        readEventsFromStreams(clientRunner.clientFactory, clientRunner.readerGroupManager);
        log.info("Verified that events were written, by reading them.");

        clientRunner.close(); // Close the client.

        controllerRunner.close(); // Shut down the controller

        // Get names of all the segments created.
        ConcurrentHashMap<String, Boolean> allSegments = segmentStoreRunner.segmentsTracker.getSegments();
        log.info("No. of segments created = {}", allSegments.size());

        // Get the long term storage from the running pravega instance
        @Cleanup
        Storage storage = new AsyncStorageWrapper(new RollingStorage(this.storageFactory.createSyncStorage(),
                new SegmentRollingPolicy(DEFAULT_ROLLING_SIZE)), executorService());

        // wait for all segments to be flushed to the long term storage.
        waitForSegmentsInStorage(allSegments.keySet(), segmentStoreRunner.segmentsTracker, storage)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        segmentStoreRunner.close(); // Shutdown SegmentStore
        log.info("Segment Store Shutdown");

        this.bookKeeperRunner.close(); // Shutdown BookKeeper & ZooKeeper
        log.info("BookKeeper & ZooKeeper shutdown");

        // start a new BookKeeper and ZooKeeper.
        this.bookKeeperRunner = new BookKeeperRunner(instanceId++, bookieCount);
        this.dataLogFactory = new BookKeeperLogFactory(this.bookKeeperRunner.bkConfig.get(), this.bookKeeperRunner.zkClient.get(),
                executorService());
        this.dataLogFactory.initialize();
        log.info("Started a new BookKeeper and ZooKeeper.");

        // Recover segments
        runRecovery(containerCount, storage);

        // Start a new segment store and controller
        segmentStoreRunner = new SegmentStoreRunner(this.storageFactory, this.dataLogFactory, containerCount);
        controllerRunner = new ControllerRunner(this.bookKeeperRunner.bkPort, segmentStoreRunner.servicePort, containerCount);
        log.info("Started segment store and controller again.");

        // Create the client with new controller.
        clientRunner = new ClientRunner(controllerRunner);

        // Try creating the same segments again with the new controller
        createScopeStream(controllerRunner.controller, SCOPE, STREAM1);
        createScopeStream(controllerRunner.controller, SCOPE, STREAM2);

        // Try reading all events again to verify that the recovery was successful.
        readEventsFromStreams(clientRunner.clientFactory, clientRunner.readerGroupManager);
        log.info("Read all events again to verify that segments were recovered.");
    }

    // Creates debug segment container instances, puts them in a map and returns it.
    private Map<Integer, DebugStreamSegmentContainer> startDebugSegmentContainers(DebugStreamSegmentContainerTests.TestContext
                                                                                          context, int containerCount,
                                                                                  BookKeeperLogFactory dataLogFactory,
                                                                                  StorageFactory storageFactory) throws Exception {
        // Start a debug segment container corresponding to the given container Id and put it in the Hashmap with the Id.
        Map<Integer, DebugStreamSegmentContainer> debugStreamSegmentContainerMap = new HashMap<>();
        OperationLogFactory localDurableLogFactory = new DurableLogFactory(DURABLE_LOG_CONFIG, dataLogFactory, executorService());

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
    private void stopDebugSegmentContainersPostFlush(int containerCount, Map<Integer, DebugStreamSegmentContainer> debugStreamSegmentContainerMap,
                                                     Storage storage) throws Exception {
        for (int containerId = 0; containerId < containerCount; containerId++) {
            String metadataSegmentName = NameUtils.getMetadataSegmentName(containerId);
            waitForSegmentsInStorage(Collections.singleton(metadataSegmentName), debugStreamSegmentContainerMap.get(containerId),
                    storage)
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            log.info("Long term storage has been update with a new container metadata segment.");

            // Stop the debug segment container
            Services.stopAsync(debugStreamSegmentContainerMap.get(containerId), executorService()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            debugStreamSegmentContainerMap.get(containerId).close();
        }
    }

    // Writes events to the streams with/without transactions.
    private void writeEventsToStreams(ClientFactoryImpl clientFactory, boolean withTransaction)
            throws TxnFailedException {
        if (withTransaction) {
            log.info("Writing transactional events on to stream: {}", STREAM1);
            writeTransactionalEvents(STREAM1, clientFactory); // write 300 events on one segment
            log.info("Writing transactional events on to stream: {}", STREAM2);
            writeTransactionalEvents(STREAM2, clientFactory); // write 300 events on other segment
        } else {
            log.info("Writing events on to stream: {}", STREAM1);
            writeEvents(STREAM1, clientFactory); // write 300 events on one segment
            log.info("Writing events on to stream: {}", STREAM2);
            writeEvents(STREAM2, clientFactory); // write 300 events on other segment
        }
    }

    // Reads all events from the streams.
    private void readEventsFromStreams(ClientFactoryImpl clientFactory, ReaderGroupManager readerGroupManager) {
        readAllEvents(STREAM1, clientFactory, readerGroupManager, "RG" + RANDOM.nextInt(Integer.MAX_VALUE),
                "R" + RANDOM.nextInt(Integer.MAX_VALUE));
        readAllEvents(STREAM2, clientFactory, readerGroupManager, "RG" + RANDOM.nextInt(Integer.MAX_VALUE),
                "R" + RANDOM.nextInt(Integer.MAX_VALUE));
    }

    /**
     * Tests the data recovery scenario with readers stalling while reading. Readers read some events and then they are
     * stopped. Durable data log is erased and restored. It's validated that readers are able to read rest of the unread
     * events.
     *  What test does, step by step:
     *  1. Starts Pravega locally with just 4 segment containers.
     *  2. Writes 300 events to two different segments.
     *  3. Waits for all segments created to be flushed to the long term storage.
     *  4. Let readers read N number of events.
     *  5. Shuts down the controller, segment store and bookeeper/zookeeper.
     *  6. Deletes container metadata segment and its attribute segment from the old LTS.
     *  7. Starts 4 debug segment containers using a new bookeeper/zookeeper and the old LTS.
     *  8. Re-creates the container metadata segment in Tier1 and let's it flushed to the LTS.
     *  9. Starts segment store and controller.
     *  10. Let readers read rest of the 300-N number of events.
     * @throws Exception    In case of an exception occurred while execution.
     */
    @Test(timeout = 180000)
    public void testDurableDataLogFailRecoveryReadersStall() throws Exception {
        int instanceId = 0;
        int bookieCount = 1;
        int containerCount = 4;
        String testReader1 = "readerDRIntegrationTest1";
        String testReader2 = "readerDRIntegrationTest2";
        String testReaderGroup1 = "readerGroupDRIntegrationTest1";
        String testReaderGroup2 = "readerGroupDRIntegrationTest2";

        // Creating a long term storage only once here.
        this.storageFactory = new InMemoryStorageFactory(executorService());
        log.info("Created a long term storage.");

        // Start a new BK & ZK, segment store and controller
        this.bookKeeperRunner = new BookKeeperRunner(instanceId++, bookieCount);
        @Cleanup
        SegmentStoreRunner segmentStoreRunner = new SegmentStoreRunner(this.storageFactory, null, containerCount);
        @Cleanup
        ControllerRunner controllerRunner = new ControllerRunner(this.bookKeeperRunner.bkPort, segmentStoreRunner.servicePort,
                containerCount);

        // Create two streams for writing data onto two different segments
        createScopeStream(controllerRunner.controller, SCOPE, STREAM1);
        createScopeStream(controllerRunner.controller, SCOPE, STREAM2);
        log.info("Created two streams.");

        // Create a client to read and write events.
        @Cleanup
        ClientRunner clientRunner = new ClientRunner(controllerRunner);

        // Write events to the streams.
        writeEventsToStreams(clientRunner.clientFactory, true);

        // Create two readers for reading both the streams.
        EventStreamReader<String> reader1 = createReader(clientRunner.clientFactory, clientRunner.readerGroupManager,
                SCOPE, STREAM1, testReaderGroup1, testReader1);
        EventStreamReader<String> reader2 = createReader(clientRunner.clientFactory, clientRunner.readerGroupManager,
                SCOPE, STREAM2, testReaderGroup2, testReader2);

        // Let readers read N number of events and mark their positions.
        Position p1 = readNEvents(reader1, NUM_EVENTS);
        Position p2 = readNEvents(reader2, NUM_EVENTS);

        ReaderGroup readerGroup1 = clientRunner.readerGroupManager.getReaderGroup(testReaderGroup1);
        ReaderGroup readerGroup2 = clientRunner.readerGroupManager.getReaderGroup(testReaderGroup2);

        readerGroup1.readerOffline(testReader1, p1);
        readerGroup2.readerOffline(testReader2, p2);

        clientRunner.close();

        controllerRunner.close(); // Shut down the controller

        // Get names of all the segments created.
        ConcurrentHashMap<String, Boolean> allSegments = segmentStoreRunner.segmentsTracker.getSegments();
        log.info("No. of segments created = {}", allSegments.size());

        // Get the long term storage from the running pravega instance
        @Cleanup
        Storage storage = new AsyncStorageWrapper(new RollingStorage(this.storageFactory.createSyncStorage(),
                new SegmentRollingPolicy(DEFAULT_ROLLING_SIZE)), executorService());

        // wait for all segments to be flushed to the long term storage.
        waitForSegmentsInStorage(allSegments.keySet(), segmentStoreRunner.segmentsTracker, storage)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        segmentStoreRunner.close(); // Shutdown SegmentStore
        log.info("Segment Store Shutdown");

        this.bookKeeperRunner.close(); // Shutdown BookKeeper & ZooKeeper
        log.info("BookKeeper & ZooKeeper shutdown");

        // start a new BookKeeper and ZooKeeper.
        this.bookKeeperRunner = new BookKeeperRunner(instanceId++, bookieCount);
        this.dataLogFactory = new BookKeeperLogFactory(this.bookKeeperRunner.bkConfig.get(), this.bookKeeperRunner.zkClient.get(),
                executorService());
        this.dataLogFactory.initialize();
        log.info("Started a new BookKeeper and ZooKeeper.");

        // Recover segments
        runRecovery(containerCount, storage);

        // Start a new segment store and controller
        segmentStoreRunner = new SegmentStoreRunner(this.storageFactory, this.dataLogFactory, containerCount);
        controllerRunner = new ControllerRunner(this.bookKeeperRunner.bkPort, segmentStoreRunner.servicePort, containerCount);
        log.info("Started segment store and controller again.");

        // Create the client with new controller.
        clientRunner = new ClientRunner(controllerRunner);

        // Try creating the same segments again with the new controller
        createScopeStream(controllerRunner.controller, SCOPE, STREAM1);
        createScopeStream(controllerRunner.controller, SCOPE, STREAM2);

        // Get reader group.
        readerGroup1 = clientRunner.readerGroupManager.getReaderGroup(testReaderGroup1);
        readerGroup2 = clientRunner.readerGroupManager.getReaderGroup(testReaderGroup2);
        assertNotNull(readerGroup1);
        assertNotNull(readerGroup2);

        reader1 = clientRunner.clientFactory.createReader(testReader1, testReaderGroup1, new UTF8StringSerializer(),
                ReaderConfig.builder().build());
        reader2 = clientRunner.clientFactory.createReader(testReader2, testReaderGroup2, new UTF8StringSerializer(),
                ReaderConfig.builder().build());

        // Read rest of the events.
        readNEvents(reader1, TOTAL_NUM_EVENTS - NUM_EVENTS);
        readNEvents(reader2, TOTAL_NUM_EVENTS - NUM_EVENTS);

        // Reading next event should return null.
        assertNull(reader1.readNextEvent(5000).getEvent());
        assertNull(reader2.readNextEvent(5000).getEvent());
        reader1.close();
        reader2.close();
    }

    private void runRecovery(int containerCount, Storage storage) throws Exception {
        // Create the environment for DebugSegmentContainer.
        @Cleanup
        DebugStreamSegmentContainerTests.TestContext context = DebugStreamSegmentContainerTests.createContext(executorService());

        // create debug segment container instances using new new dataLog and old storage.
        Map<Integer, DebugStreamSegmentContainer> debugStreamSegmentContainerMap = startDebugSegmentContainers(context,
                containerCount, this.dataLogFactory, this.storageFactory);

        // Delete container metadata segment and attributes index segment corresponding to the container Id from the long term storage
        for (int containerId = 0; containerId < containerCount; containerId++) {
            ContainerRecoveryUtils.deleteMetadataAndAttributeSegments(storage, containerId).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }

        // List segments from storage and recover them using debug segment container instance.
        ContainerRecoveryUtils.recoverAllSegments(storage, debugStreamSegmentContainerMap, executorService());

        // Waits for metadata segments to be flushed to LTS and then stops the debug segment containers
        stopDebugSegmentContainersPostFlush(containerCount, debugStreamSegmentContainerMap, storage);
        log.info("Segments have been recovered.");

        this.dataLogFactory.close();
    }

    /**
     * Tests the data recovery scenario with watermarking events.
     *  What test does, step by step:
     *  1. Starts Pravega locally with just 4 segment containers.
     *  2. Writes 300 events to two different segments with watermarks.
     *  3. Waits for all segments created to be flushed to the long term storage.
     *  4. Shuts down the controller, segment store and bookeeper/zookeeper.
     *  5. Deletes container metadata segment and its attribute segment from the old LTS.
     *  6. Starts 4 debug segment containers using a new bookeeper/zookeeper and the old LTS.
     *  7. Re-creates the container metadata segment in Tier1 and let's it flushed to the LTS.
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
        this.bookKeeperRunner = new BookKeeperRunner(instanceId++, bookieCount);
        @Cleanup
        SegmentStoreRunner segmentStoreRunner = new SegmentStoreRunner(this.storageFactory, null, containerCount);
        @Cleanup
        ControllerRunner controllerRunner = new ControllerRunner(this.bookKeeperRunner.bkPort, segmentStoreRunner.servicePort,
                containerCount);

        // Create a scope and a stream
        createScopeStream(controllerRunner.controller, SCOPE, STREAM1);

        // Create a client to read and write events.
        @Cleanup
        ClientRunner clientRunner = new ClientRunner(controllerRunner);

        // Create writes to both streams
        @Cleanup
        TransactionalEventStreamWriter<Long> writer1 = clientRunner.clientFactory
                .createTransactionalEventWriter("writer1", STREAM1, new JavaSerializer<>(),
                        EventWriterConfig.builder().transactionTimeoutTime(10000).build());
        @Cleanup
        TransactionalEventStreamWriter<Long> writer2 = clientRunner.clientFactory
                .createTransactionalEventWriter("writer2", STREAM1, new JavaSerializer<>(),
                        EventWriterConfig.builder().transactionTimeoutTime(10000).build());

        AtomicBoolean stopFlag = new AtomicBoolean(false);
        // write events
        CompletableFuture<Void> writer1Future = writeTxEvents(writer1, stopFlag);
        CompletableFuture<Void> writer2Future = writeTxEvents(writer2, stopFlag);

        // scale the stream several times so that we get complex positions
        StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(5)).build();
        Stream streamObj = Stream.of(SCOPE, STREAM1);
        scale(controllerRunner.controller, streamObj, config);

        // get watermarks
        @Cleanup
        SynchronizerClientFactory syncClientFactory = SynchronizerClientFactory.withScope(SCOPE,
                ClientConfig.builder().controllerURI(controllerRunner.controllerURI).build());

        String markStream = NameUtils.getMarkStreamForStream(STREAM1);
        RevisionedStreamClient<Watermark> watermarkReader = syncClientFactory.createRevisionedStreamClient(markStream,
                new WatermarkSerializer(), SynchronizerConfig.builder().build());
        LinkedBlockingQueue<Watermark> watermarks = new LinkedBlockingQueue<>();
        fetchWatermarks(watermarkReader, watermarks, stopFlag);
        AssertExtensions.assertEventuallyEquals(true, () -> watermarks.size() >= 2, 100000);
        stopFlag.set(true);
        writer1Future.join();
        writer2Future.join();

        controllerRunner.close(); // Shut down the controller

        // Get names of all the segments created.
        ConcurrentHashMap<String, Boolean> allSegments = segmentStoreRunner.segmentsTracker.getSegments();
        log.info("No. of segments created = {}", allSegments.size());

        // Get the long term storage from the running pravega instance
        @Cleanup
        Storage storage = new AsyncStorageWrapper(new RollingStorage(this.storageFactory.createSyncStorage(),
                new SegmentRollingPolicy(DEFAULT_ROLLING_SIZE)), executorService());

        // wait for all segments to be flushed to the long term storage.
        waitForSegmentsInStorage(allSegments.keySet(), segmentStoreRunner.segmentsTracker, storage)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        segmentStoreRunner.close(); // Shutdown SegmentStore
        log.info("Segment Store Shutdown");

        this.bookKeeperRunner.close(); // Shutdown BookKeeper & ZooKeeper
        log.info("BookKeeper & ZooKeeper shutdown");

        // start a new BookKeeper and ZooKeeper.
        this.bookKeeperRunner = new BookKeeperRunner(instanceId++, bookieCount);
        this.dataLogFactory = new BookKeeperLogFactory(this.bookKeeperRunner.bkConfig.get(), this.bookKeeperRunner.zkClient.get(),
                executorService());
        this.dataLogFactory.initialize();
        log.info("Started a new BookKeeper and ZooKeeper.");

        // Recover segments
        runRecovery(containerCount, storage);

        // Start a new segment store and controller
        segmentStoreRunner = new SegmentStoreRunner(this.storageFactory, this.dataLogFactory, containerCount);
        controllerRunner = new ControllerRunner(this.bookKeeperRunner.bkPort, segmentStoreRunner.servicePort, containerCount);
        log.info("Started segment store and controller again.");

        // Create the client with new controller.
        clientRunner = new ClientRunner(controllerRunner);

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

        // read from stream cut of first watermark
        clientRunner.readerGroupManager.createReaderGroup(readerGroup, ReaderGroupConfig.builder().stream(streamObj)
                .startingStreamCuts(firstMarkStreamCut)
                .endingStreamCuts(secondMarkStreamCut)
                .build());

        @Cleanup
        final EventStreamReader<Long> reader = clientRunner.clientFactory.createReader("myreaderTx", readerGroup,
                new JavaSerializer<>(), ReaderConfig.builder().build());

        EventRead<Long> event = reader.readNextEvent(10000L);
        TimeWindow currentTimeWindow = reader.getCurrentTimeWindow(streamObj);
        while (event.getEvent() != null && currentTimeWindow.getLowerTimeBound() == null && currentTimeWindow.getUpperTimeBound() == null) {
            event = reader.readNextEvent(10000L);
            currentTimeWindow = reader.getCurrentTimeWindow(streamObj);
        }

        assertNotNull(currentTimeWindow.getUpperTimeBound());

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

            event = reader.readNextEvent(10000L);
            if (event.isCheckpoint()) {
                event = reader.readNextEvent(10000L);
            }
        }

        assertNotNull(currentTimeWindow.getLowerTimeBound());
    }

    private void fetchWatermarks(RevisionedStreamClient<Watermark> watermarkReader, LinkedBlockingQueue<Watermark> watermarks,
                                 AtomicBoolean stop) {
        AtomicReference<Revision> revision = new AtomicReference<>(watermarkReader.fetchOldestRevision());

        Futures.loop(() -> !stop.get(), () -> Futures.delayedTask(() -> {
            Iterator<Map.Entry<Revision, Watermark>> marks = watermarkReader.readFrom(revision.get());
            if (marks.hasNext()) {
                Map.Entry<Revision, Watermark> next = marks.next();
                log.info("watermark = {}", next.getValue());

                watermarks.add(next.getValue());
                revision.set(next.getKey());
            }
            return null;
        }, Duration.ofSeconds(10), executorService()), executorService());
    }

    private CompletableFuture<Void> writeTxEvents(TransactionalEventStreamWriter<Long> writer, AtomicBoolean stopFlag) {
        AtomicInteger count = new AtomicInteger(0);
        return Futures.loop(() -> !stopFlag.get(), () -> Futures.delayedFuture(() -> {
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

    private Position readNEvents(EventStreamReader<String> reader, int num) {
        Position position = null;
        for (int q = 0; q < num;) {
            try {
                EventRead<String> eventRead = reader.readNextEvent(READ_TIMEOUT.toMillis());
                Assert.assertEquals("Event written and read back don't match", EVENT, eventRead.getEvent());
                q++;
                position = eventRead.getPosition();
            } catch (Exception e) {
                Assert.fail("Error occurred while reading the events.");
            }
        }
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
            writer.writeEvent("", EVENT).join();
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
        transaction.flush();
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

    // Waits for the segments to be flushed to the storage using the information from the given DebugStreamSegmentContainer instance.
    private CompletableFuture<Void> waitForSegmentsInStorage(Collection<String> segmentNames, DebugStreamSegmentContainer container,
                                                             Storage storage) {
        ArrayList<CompletableFuture<Void>> segmentsCompletion = new ArrayList<>();
        for (String segmentName : segmentNames) {
            SegmentProperties sp = container.getStreamSegmentInfo(segmentName, TIMEOUT).join();
            log.info("Segment properties = {}", sp);
            segmentsCompletion.add(waitForSegmentInStorage(sp, storage));
        }

        return Futures.allOf(segmentsCompletion);
    }

    // Waits for the segments to be flushed to the storage using the information from the given StramSegmentStore instance.
    private CompletableFuture<Void> waitForSegmentsInStorage(Collection<String> segmentNames, StreamSegmentStore baseStore,
                                                             Storage storage) {
        ArrayList<CompletableFuture<Void>> segmentsCompletion = new ArrayList<>();
        SegmentProperties sp;
        for (String segmentName : segmentNames) {
            try {
                sp = baseStore.getStreamSegmentInfo(segmentName, TIMEOUT).join();
            } catch (Throwable e) {
                if (Exceptions.unwrap(e) instanceof StreamSegmentNotExistsException) {
                    log.info("Segment '{}' doesn't exist.", segmentName);
                    continue;
                } else {
                    throw e;
                }
            }
            log.info("Segment properties = {}", sp);
            segmentsCompletion.add(waitForSegmentInStorage(sp, storage));
        }

        return Futures.allOf(segmentsCompletion);
    }

    private CompletableFuture<Void> waitForSegmentInStorage(SegmentProperties sp, Storage storage) {
        if (sp.getLength() == 0) {
            // Empty segments may or may not exist in Storage, so don't bother complicating ourselves with this.
            return CompletableFuture.completedFuture(null);
        }

        // We want to make sure that both the main segment and its attribute segment have been sync-ed to Storage. In case
        // of the attribute segment, the only thing we can easily do is verify that it has been sealed when the main segment
        // it is associated with has also been sealed.
        String attributeSegmentName = NameUtils.getAttributeSegmentName(sp.getName());
        TimeoutTimer timer = new TimeoutTimer(TIMEOUT);
        AtomicBoolean tryAgain = new AtomicBoolean(true);
        return Futures.loop(
                tryAgain::get,
                () -> {
                    val segInfo = getStorageSegmentInfo(sp.getName(), timer, storage);
                    val attrInfo = getStorageSegmentInfo(attributeSegmentName, timer, storage);
                    return CompletableFuture.allOf(segInfo, attrInfo)
                            .thenCompose(v -> {
                                SegmentProperties storageProps = segInfo.join();
                                SegmentProperties attrProps = attrInfo.join();
                                if (sp.isSealed()) {
                                    tryAgain.set(!storageProps.isSealed() || !(attrProps.isSealed() || attrProps.isDeleted()));
                                } else {
                                    tryAgain.set(sp.getLength() != storageProps.getLength());
                                }

                                if (tryAgain.get() && !timer.hasRemaining()) {
                                    return Futures.<Void>failedFuture(new TimeoutException(
                                            String.format("Segment %s did not complete in Storage in the allotted time.", sp.getName())));
                                } else {
                                    return Futures.delayedFuture(Duration.ofMillis(100), executorService());
                                }
                            });
                },
                executorService());
    }

    private CompletableFuture<SegmentProperties> getStorageSegmentInfo(String segmentName, TimeoutTimer timer, Storage storage) {
        return Futures
                .exceptionallyExpecting(storage.getStreamSegmentInfo(segmentName, timer.getRemaining()),
                        ex -> ex instanceof StreamSegmentNotExistsException,
                        StreamSegmentInformation.builder().name(segmentName).deleted(true).build());
    }
}

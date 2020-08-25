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
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.UTF8StringSerializer;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Integration test to verify data recovery.
 * Recovery scenario: when data written to Pravega is already flushed to the long term storage.
 * Tests replicate different environments for data recovery.
 */
@Slf4j
public class RestoreBackUpDataRecoveryTest extends ThreadPooledTestSuite {
    protected static final Duration TIMEOUT = Duration.ofMillis(100 * 1000);

    private static final int CONTAINER_ID = 0;

    /**
     * Write 300 events to different segments.
     */
    private static final long TOTAL_NUM_EVENTS = 300;

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

    private StorageFactory storageFactory;
    private BookKeeperLogFactory dataLogFactory;
    private BookKeeperStarter bookKeeperStarter = null;

    @After
    public void tearDown() throws Exception {
        if (this.dataLogFactory != null) {
            this.dataLogFactory.close();
            this.dataLogFactory = null;
        }

        if (this.bookKeeperStarter != null) {
            this.bookKeeperStarter.close();
            this.bookKeeperStarter = null;
        }
    }

    @Override
    protected int getThreadPoolSize() {
        return 100;
    }

    BookKeeperStarter setUpNewBK(int instanceId) throws Exception {
        return new BookKeeperStarter(instanceId);
    }

    /**
     * Sets up a new BookKeeper & ZooKeeper.
     */
    private static class BookKeeperStarter implements AutoCloseable {
        private final int bookieCount = 1;
        private AtomicReference<BookKeeperConfig> bkConfig = new AtomicReference<>();
        private AtomicReference<CuratorFramework> zkClient = new AtomicReference<>();
        private BookKeeperServiceRunner bookKeeperServiceRunner;
        private AtomicReference<BookKeeperServiceRunner> bkService = new AtomicReference<>();
        private int bkPort;

        BookKeeperStarter(int instanceId) throws Exception {
            bkPort = TestUtils.getAvailableListenPort();
            val bookiePort = new ArrayList<>(Arrays.asList(TestUtils.getAvailableListenPort()));

            this.bookKeeperServiceRunner = BookKeeperServiceRunner.builder()
                    .startZk(true)
                    .zkPort(bkPort)
                    .ledgersPath("/pravega/bookkeeper/ledgers")
                    .bookiePorts(bookiePort)
                    .build();
            this.bookKeeperServiceRunner.startAll();
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
                this.bookKeeperServiceRunner = null;
            }

            val zkClient = this.zkClient.getAndSet(null);
            if (zkClient != null) {
                zkClient.close();
            }
        }
    }

    SegmentStoreStarter startSegmentStore(StorageFactory storageFactory, BookKeeperLogFactory dataLogFactory, int containerCount)
            throws DurableDataLogException {
        return new SegmentStoreStarter(storageFactory, dataLogFactory, containerCount);
    }

    /**
     * Creates a segment store.
     */
    private static class SegmentStoreStarter {
        private final int servicePort = TestUtils.getAvailableListenPort();
        private ServiceBuilder serviceBuilder;
        private SegmentStoreWithSegmentTracker segmentsTracker;
        private PravegaConnectionListener server;

        SegmentStoreStarter(StorageFactory storageFactory, BookKeeperLogFactory dataLogFactory, int containerCount)
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

    ControllerStarter startController(int bkPort, int servicePort, int containerCount) throws InterruptedException {
        return new ControllerStarter(bkPort, servicePort, containerCount);
    }

    /**
     * Creates a controller instance and runs it.
     */
    private static class ControllerStarter {
        private final int controllerPort = TestUtils.getAvailableListenPort();
        private final String serviceHost = "localhost";
        private ControllerWrapper controllerWrapper;
        private Controller controller;
        private URI controllerURI = URI.create("tcp://" + serviceHost + ":" + controllerPort);

        ControllerStarter(int bkPort, int servicePort, int containerCount) throws InterruptedException {
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
     * Tests the data recovery scenario with just one segment container. Segments recovery is also attained using just one
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
        int instanceId = 0;
        int containerCount = 1;
        // Creating a long term storage only once here.
        this.storageFactory = new InMemoryStorageFactory(executorService());
        log.info("Created a long term storage.");

        // Start a new BK & ZK, segment store and controller
        this.bookKeeperStarter = setUpNewBK(instanceId++);
        @Cleanup
        SegmentStoreStarter segmentStoreStarter = startSegmentStore(this.storageFactory, null, containerCount);
        @Cleanup
        ControllerStarter controllerStarter = startController(this.bookKeeperStarter.bkPort, segmentStoreStarter.servicePort,
                containerCount);

        // Create two streams for writing data onto two different segments
        createScopeStream(controllerStarter.controller, SCOPE, STREAM1);
        createScopeStream(controllerStarter.controller, SCOPE, STREAM2);
        log.info("Created two streams.");

        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder()
                .controllerURI(controllerStarter.controllerURI).build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE, controllerStarter.controller, connectionFactory);
        @Cleanup
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(SCOPE, controllerStarter.controller, clientFactory);

        log.info("Writing events on to stream: {}", STREAM1);
        writeEvents(STREAM1, clientFactory); // write 300 events on one segment
        log.info("Writing events on to stream: {}", STREAM2);
        writeEvents(STREAM2, clientFactory); // write 300 events on other segment

        // Verify events write by reading them.
        readAllEvents(STREAM1, clientFactory, readerGroupManager, "RG" + RANDOM.nextInt(Integer.MAX_VALUE),
                "R" + RANDOM.nextInt(Integer.MAX_VALUE));
        readAllEvents(STREAM2, clientFactory, readerGroupManager, "RG" + RANDOM.nextInt(Integer.MAX_VALUE),
                "R" + RANDOM.nextInt(Integer.MAX_VALUE));
        log.info("Verified that events were written, by reading them.");

        readerGroupManager.close();
        clientFactory.close();

        controllerStarter.close(); // Shut down the controller

        // Get names of all the segments created.
        ConcurrentHashMap<String, Boolean> allSegments = segmentStoreStarter.segmentsTracker.getSegments();
        log.info("No. of segments created = {}", allSegments.size());

        // Get the long term storage from the running pravega instance
        @Cleanup
        Storage storage = new AsyncStorageWrapper(new RollingStorage(this.storageFactory.createSyncStorage(),
                new SegmentRollingPolicy(DEFAULT_ROLLING_SIZE)), executorService());

        // wait for all segments to be flushed to the long term storage.
        waitForSegmentsInStorage(allSegments.keySet(), segmentStoreStarter.segmentsTracker, storage)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        segmentStoreStarter.close(); // Shutdown SegmentStore
        log.info("Segment Store Shutdown");

        this.bookKeeperStarter.close(); // Shutdown BookKeeper & ZooKeeper
        this.bookKeeperStarter = null;
        log.info("BookKeeper & ZooKeeper shutdown");

        // start a new BookKeeper and ZooKeeper.
        this.bookKeeperStarter = setUpNewBK(instanceId++);
        this.dataLogFactory = new BookKeeperLogFactory(this.bookKeeperStarter.bkConfig.get(), this.bookKeeperStarter.zkClient.get(),
                executorService());
        this.dataLogFactory.initialize();
        log.info("Started a new BookKeeper and ZooKeeper.");

        // Create the environment for DebugSegmentContainer.
        @Cleanup
        DebugStreamSegmentContainerTests.TestContext context = DebugStreamSegmentContainerTests.createContext(executorService());
        // Use dataLogFactory from new BK instance.
        OperationLogFactory localDurableLogFactory = new DurableLogFactory(DURABLE_LOG_CONFIG, this.dataLogFactory,
                executorService());

        // Start a debug segment container corresponding to the given container Id and put it in the Hashmap with the Id.
        Map<Integer, DebugStreamSegmentContainer> debugStreamSegmentContainerMap = new HashMap<>();

        // Create a debug segment container instance using a new dataLog and old storage.
        DebugStreamSegmentContainerTests.MetadataCleanupContainer debugStreamSegmentContainer = new
                DebugStreamSegmentContainerTests.MetadataCleanupContainer(CONTAINER_ID, CONTAINER_CONFIG, localDurableLogFactory,
                context.readIndexFactory, context.attributeIndexFactory, context.writerFactory, this.storageFactory,
                context.getDefaultExtensions(), executorService());

        Services.startAsync(debugStreamSegmentContainer, executorService()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        debugStreamSegmentContainerMap.put(CONTAINER_ID, debugStreamSegmentContainer);

        // Delete container metadata segment and attributes index segment corresponding to the container Id from the long term storage
        ContainerRecoveryUtils.deleteMetadataAndAttributeSegments(storage, CONTAINER_ID).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // List segments from storage and recover them using debug segment container instance.
        ContainerRecoveryUtils.recoverAllSegments(storage, debugStreamSegmentContainerMap, executorService());

        // Wait for metadata segment to be flushed to LTS
        String metadataSegmentName = NameUtils.getMetadataSegmentName(CONTAINER_ID);
        waitForSegmentsInStorage(Collections.singleton(metadataSegmentName), debugStreamSegmentContainer, storage)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        log.info("Long term storage has been update with a new container metadata segment.");

        // Stop the debug segment container
        this.dataLogFactory.close();
        Services.stopAsync(debugStreamSegmentContainerMap.get(CONTAINER_ID), executorService()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        debugStreamSegmentContainerMap.get(CONTAINER_ID).close();
        log.info("Segments have been recovered.");

        // Start a new segment store and controller
        segmentStoreStarter = startSegmentStore(this.storageFactory, this.dataLogFactory, containerCount);
        controllerStarter = startController(this.bookKeeperStarter.bkPort, segmentStoreStarter.servicePort, containerCount);
        log.info("Started segment store and controller again.");

        connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder()
                .controllerURI(controllerStarter.controllerURI).build());
        clientFactory = new ClientFactoryImpl(SCOPE, controllerStarter.controller, connectionFactory);
        readerGroupManager = new ReaderGroupManagerImpl(SCOPE, controllerStarter.controller, clientFactory);

        // Try creating the same segments again with the new controller
        createScopeStream(controllerStarter.controller, SCOPE, STREAM1);
        createScopeStream(controllerStarter.controller, SCOPE, STREAM2);

        // Try reading all events again
        readAllEvents(STREAM1, clientFactory, readerGroupManager, "RG" + RANDOM.nextInt(Integer.MAX_VALUE),
                "R" + RANDOM.nextInt(Integer.MAX_VALUE));
        readAllEvents(STREAM2, clientFactory, readerGroupManager, "RG" + RANDOM.nextInt(Integer.MAX_VALUE),
                "R" + RANDOM.nextInt(Integer.MAX_VALUE));
        log.info("Read all events again to verify that segments were recovered.");
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
        int instanceId = 0;
        int containerCount = 4;
        // Creating a long term storage only once here.
        this.storageFactory = new InMemoryStorageFactory(executorService());
        log.info("Created a long term storage.");

        // Start a new BK & ZK, segment store and controller
        this.bookKeeperStarter = setUpNewBK(instanceId++);
        @Cleanup
        SegmentStoreStarter segmentStoreStarter = startSegmentStore(this.storageFactory, null, containerCount);
        @Cleanup
        ControllerStarter controllerStarter = startController(this.bookKeeperStarter.bkPort, segmentStoreStarter.servicePort,
                containerCount);

        // Create two streams for writing data onto two different segments
        createScopeStream(controllerStarter.controller, SCOPE, STREAM1);
        createScopeStream(controllerStarter.controller, SCOPE, STREAM2);
        log.info("Created two streams.");

        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder()
                .controllerURI(controllerStarter.controllerURI).build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE, controllerStarter.controller, connectionFactory);
        @Cleanup
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(SCOPE, controllerStarter.controller, clientFactory);

        log.info("Writing events on to stream: {}", STREAM1);
        writeEvents(STREAM1, clientFactory); // write 300 events on one segment
        log.info("Writing events on to stream: {}", STREAM2);
        writeEvents(STREAM2, clientFactory); // write 300 events on other segment

        // Verify events write by reading them.
        readAllEvents(STREAM1, clientFactory, readerGroupManager, "RG" + RANDOM.nextInt(Integer.MAX_VALUE),
                "R" + RANDOM.nextInt(Integer.MAX_VALUE));
        readAllEvents(STREAM2, clientFactory, readerGroupManager, "RG" + RANDOM.nextInt(Integer.MAX_VALUE),
                "R" + RANDOM.nextInt(Integer.MAX_VALUE));
        log.info("Verified that events were written, by reading them.");

        readerGroupManager.close();
        clientFactory.close();

        controllerStarter.close(); // Shut down the controller

        // Get names of all the segments created.
        ConcurrentHashMap<String, Boolean> allSegments = segmentStoreStarter.segmentsTracker.getSegments();
        log.info("No. of segments created = {}", allSegments.size());

        // Get the long term storage from the running pravega instance
        @Cleanup
        Storage storage = new AsyncStorageWrapper(new RollingStorage(this.storageFactory.createSyncStorage(),
                new SegmentRollingPolicy(DEFAULT_ROLLING_SIZE)), executorService());

        // wait for all segments to be flushed to the long term storage.
        waitForSegmentsInStorage(allSegments.keySet(), segmentStoreStarter.segmentsTracker, storage)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        segmentStoreStarter.close(); // Shutdown SegmentStore
        log.info("Segment Store Shutdown");

        this.bookKeeperStarter.close(); // Shutdown BookKeeper & ZooKeeper
        this.bookKeeperStarter = null;
        log.info("BookKeeper & ZooKeeper shutdown");

        // start a new BookKeeper and ZooKeeper.
        this.bookKeeperStarter = setUpNewBK(instanceId++);
        this.dataLogFactory = new BookKeeperLogFactory(this.bookKeeperStarter.bkConfig.get(), this.bookKeeperStarter.zkClient.get(),
                executorService());
        this.dataLogFactory.initialize();
        log.info("Started a new BookKeeper and ZooKeeper.");

        // Create the environment for DebugSegmentContainer.
        @Cleanup
        DebugStreamSegmentContainerTests.TestContext context = DebugStreamSegmentContainerTests.createContext(executorService());
        // Use dataLogFactory from new BK instance.
        OperationLogFactory localDurableLogFactory = new DurableLogFactory(DURABLE_LOG_CONFIG, this.dataLogFactory,
                executorService());

        // Start a debug segment container corresponding to the given container Id and put it in the Hashmap with the Id.
        Map<Integer, DebugStreamSegmentContainer> debugStreamSegmentContainerMap = new HashMap<>();

        // Create a debug segment container instances using a new dataLog and old storage.
        for (int containerId = 0; containerId < containerCount; containerId++) {
            DebugStreamSegmentContainerTests.MetadataCleanupContainer debugStreamSegmentContainer = new
                    DebugStreamSegmentContainerTests.MetadataCleanupContainer(containerId, CONTAINER_CONFIG, localDurableLogFactory,
                    context.readIndexFactory, context.attributeIndexFactory, context.writerFactory, this.storageFactory,
                    context.getDefaultExtensions(), executorService());

            Services.startAsync(debugStreamSegmentContainer, executorService()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            debugStreamSegmentContainerMap.put(containerId, debugStreamSegmentContainer);

            // Delete container metadata segment and attributes index segment corresponding to the container Id from the long term storage
            ContainerRecoveryUtils.deleteMetadataAndAttributeSegments(storage, containerId).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }

        // List segments from storage and recover them using debug segment container instance.
        ContainerRecoveryUtils.recoverAllSegments(storage, debugStreamSegmentContainerMap, executorService());

        for (int containerId = 0; containerId < containerCount; containerId++) {
            // Wait for metadata segment to be flushed to LTS
            String metadataSegmentName = NameUtils.getMetadataSegmentName(containerId);
            waitForSegmentsInStorage(Collections.singleton(metadataSegmentName), debugStreamSegmentContainerMap.get(containerId),
                    storage)
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            log.info("Long term storage has been update with a new container metadata segment.");

            // Stop the debug segment container
            Services.stopAsync(debugStreamSegmentContainerMap.get(containerId), executorService()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            debugStreamSegmentContainerMap.get(containerId).close();
        }
        log.info("Segments have been recovered.");

        this.dataLogFactory.close();
        // Start a new segment store and controller
        segmentStoreStarter = startSegmentStore(this.storageFactory, this.dataLogFactory, containerCount);
        controllerStarter = startController(this.bookKeeperStarter.bkPort, segmentStoreStarter.servicePort, containerCount);
        log.info("Started segment store and controller again.");

        connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder()
                .controllerURI(controllerStarter.controllerURI).build());
        clientFactory = new ClientFactoryImpl(SCOPE, controllerStarter.controller, connectionFactory);
        readerGroupManager = new ReaderGroupManagerImpl(SCOPE, controllerStarter.controller, clientFactory);

        // Try creating the same segments again with the new controller
        createScopeStream(controllerStarter.controller, SCOPE, STREAM1);
        createScopeStream(controllerStarter.controller, SCOPE, STREAM2);

        // Try reading all events again
        readAllEvents(STREAM1, clientFactory, readerGroupManager, "RG" + RANDOM.nextInt(Integer.MAX_VALUE),
                "R" + RANDOM.nextInt(Integer.MAX_VALUE));
        readAllEvents(STREAM2, clientFactory, readerGroupManager, "RG" + RANDOM.nextInt(Integer.MAX_VALUE),
                "R" + RANDOM.nextInt(Integer.MAX_VALUE));
        log.info("Read all events again to verify that segments were recovered.");
    }

    /**
     * Tests the data recovery scenario with transactional writer. Events are written using a transactional writer.
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
    public void testDurableDataLogFailRecoveryTransactionalWriter() throws Exception {
        int instanceId = 0;
        int containerCount = 4;

        // Creating a long term storage only once here.
        this.storageFactory = new InMemoryStorageFactory(executorService());
        log.info("Created a long term storage.");

        // Start a new BK & ZK, segment store and controller
        this.bookKeeperStarter = setUpNewBK(instanceId++);
        @Cleanup
        SegmentStoreStarter segmentStoreStarter = startSegmentStore(this.storageFactory, null, containerCount);
        @Cleanup
        ControllerStarter controllerStarter = startController(this.bookKeeperStarter.bkPort, segmentStoreStarter.servicePort,
                containerCount);

        // Create two streams for writing data onto two different segments
        createScopeStream(controllerStarter.controller, SCOPE, STREAM1);
        createScopeStream(controllerStarter.controller, SCOPE, STREAM2);
        log.info("Created two streams.");

        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder()
                .controllerURI(controllerStarter.controllerURI).build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE, controllerStarter.controller, connectionFactory);
        @Cleanup
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(SCOPE, controllerStarter.controller, clientFactory);

        log.info("Writing events on to stream: {}", STREAM1);
        writeTransactionalEvents(STREAM1, clientFactory); // write 300 events on one segment
        log.info("Writing events on to stream: {}", STREAM2);
        writeTransactionalEvents(STREAM2, clientFactory); // write 300 events on other segment

        // Verify events write by reading them.
        readAllEvents(STREAM1, clientFactory, readerGroupManager, "RG" + RANDOM.nextInt(Integer.MAX_VALUE),
                "R" + RANDOM.nextInt(Integer.MAX_VALUE));
        readAllEvents(STREAM2, clientFactory, readerGroupManager, "RG" + RANDOM.nextInt(Integer.MAX_VALUE),
                "R" + RANDOM.nextInt(Integer.MAX_VALUE));
        log.info("Verified that events were written, by reading them.");

        readerGroupManager.close();
        clientFactory.close();

        controllerStarter.close(); // Shut down the controller

        // Get names of all the segments created.
        ConcurrentHashMap<String, Boolean> allSegments = segmentStoreStarter.segmentsTracker.getSegments();
        log.info("No. of segments created = {}", allSegments.size());

        // Get the long term storage from the running pravega instance
        @Cleanup
        Storage storage = new AsyncStorageWrapper(new RollingStorage(this.storageFactory.createSyncStorage(),
                new SegmentRollingPolicy(DEFAULT_ROLLING_SIZE)), executorService());

        // wait for all segments to be flushed to the long term storage.
        waitForSegmentsInStorage(allSegments.keySet(), segmentStoreStarter.segmentsTracker, storage)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        segmentStoreStarter.close(); // Shutdown SegmentStore
        log.info("Segment Store Shutdown");

        this.bookKeeperStarter.close(); // Shutdown BookKeeper & ZooKeeper
        this.bookKeeperStarter = null;
        log.info("BookKeeper & ZooKeeper shutdown");

        // start a new BookKeeper and ZooKeeper.
        this.bookKeeperStarter = setUpNewBK(instanceId++);
        this.dataLogFactory = new BookKeeperLogFactory(this.bookKeeperStarter.bkConfig.get(), this.bookKeeperStarter.zkClient.get(),
                executorService());
        this.dataLogFactory.initialize();
        log.info("Started a new BookKeeper and ZooKeeper.");

        // Create the environment for DebugSegmentContainer.
        @Cleanup
        DebugStreamSegmentContainerTests.TestContext context = DebugStreamSegmentContainerTests.createContext(executorService());
        // Use dataLogFactory from new BK instance.
        OperationLogFactory localDurableLogFactory = new DurableLogFactory(DURABLE_LOG_CONFIG, this.dataLogFactory,
                executorService());

        // Start a debug segment container corresponding to the given container Id and put it in the Hashmap with the Id.
        Map<Integer, DebugStreamSegmentContainer> debugStreamSegmentContainerMap = new HashMap<>();

        // Create a debug segment container instances using a new dataLog and old storage.
        for (int containerId = 0; containerId < containerCount; containerId++) {
            DebugStreamSegmentContainerTests.MetadataCleanupContainer debugStreamSegmentContainer = new
                    DebugStreamSegmentContainerTests.MetadataCleanupContainer(containerId, CONTAINER_CONFIG, localDurableLogFactory,
                    context.readIndexFactory, context.attributeIndexFactory, context.writerFactory, this.storageFactory,
                    context.getDefaultExtensions(), executorService());

            Services.startAsync(debugStreamSegmentContainer, executorService()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            debugStreamSegmentContainerMap.put(containerId, debugStreamSegmentContainer);

            // Delete container metadata segment and attributes index segment corresponding to the container Id from the long term storage
            ContainerRecoveryUtils.deleteMetadataAndAttributeSegments(storage, containerId).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }

        // List segments from storage and recover them using debug segment container instance.
        ContainerRecoveryUtils.recoverAllSegments(storage, debugStreamSegmentContainerMap, executorService());

        for (int containerId = 0; containerId < containerCount; containerId++) {
            // Wait for metadata segment to be flushed to LTS
            String metadataSegmentName = NameUtils.getMetadataSegmentName(containerId);
            waitForSegmentsInStorage(Collections.singleton(metadataSegmentName), debugStreamSegmentContainerMap.get(containerId),
                    storage)
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            log.info("Long term storage has been update with a new container metadata segment.");

            // Stop the debug segment container
            Services.stopAsync(debugStreamSegmentContainerMap.get(containerId), executorService()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            debugStreamSegmentContainerMap.get(containerId).close();
        }
        log.info("Segments have been recovered.");

        this.dataLogFactory.close();
        // Start a new segment store and controller
        segmentStoreStarter = startSegmentStore(this.storageFactory, this.dataLogFactory, containerCount);
        controllerStarter = startController(this.bookKeeperStarter.bkPort, segmentStoreStarter.servicePort, containerCount);
        log.info("Started segment store and controller again.");

        connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder()
                .controllerURI(controllerStarter.controllerURI).build());
        clientFactory = new ClientFactoryImpl(SCOPE, controllerStarter.controller, connectionFactory);
        readerGroupManager = new ReaderGroupManagerImpl(SCOPE, controllerStarter.controller, clientFactory);

        // Try creating the same segments again with the new controller
        createScopeStream(controllerStarter.controller, SCOPE, STREAM1);
        createScopeStream(controllerStarter.controller, SCOPE, STREAM2);

        // Try reading all events again
        readAllEvents(STREAM1, clientFactory, readerGroupManager, "RG" + RANDOM.nextInt(Integer.MAX_VALUE),
                "R" + RANDOM.nextInt(Integer.MAX_VALUE));
        readAllEvents(STREAM2, clientFactory, readerGroupManager, "RG" + RANDOM.nextInt(Integer.MAX_VALUE),
                "R" + RANDOM.nextInt(Integer.MAX_VALUE));
        log.info("Read all events again to verify that segments were recovered.");
    }

    public void createScopeStream(Controller controller, String scopeName, String streamName) {
        ClientConfig clientConfig = ClientConfig.builder().build();
        try (ConnectionPool cp = new ConnectionPoolImpl(clientConfig, new SocketConnectionFactoryImpl(clientConfig));
             StreamManager streamManager = new StreamManagerImpl(controller, cp)) {
            //create a scope
            Boolean createScopeStatus = streamManager.createScope(scopeName);
            log.info("Create scope status {}", createScopeStatus);
            //create a stream
            Boolean createStreamStatus = streamManager.createStream(scopeName, streamName, config);
            log.info("Create stream status {}", createStreamStatus);
        }
    }

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

    private void writeTransactionalEvents(String streamName, ClientFactoryImpl clientFactory) throws TxnFailedException {
        EventWriterConfig writerConfig = EventWriterConfig.builder().transactionTimeoutTime(10000).build();
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

    private void readAllEvents(String streamName, ClientFactoryImpl clientFactory, ReaderGroupManager readerGroupManager,
                               String readerGroupName, String readerName) {
        readerGroupManager.createReaderGroup(readerGroupName,
                ReaderGroupConfig
                        .builder()
                        .stream(Stream.of(SCOPE, streamName))
                        .automaticCheckpointIntervalMillis(2000)
                        .build());

        EventStreamReader<String> reader = clientFactory.createReader(readerName,
                readerGroupName,
                new UTF8StringSerializer(),
                ReaderConfig.builder().build());

        for (int q = 0; q < TOTAL_NUM_EVENTS;) {
            String eventRead = reader.readNextEvent(SECONDS.toMillis(500)).getEvent();
            Assert.assertEquals("Event written and read back don't match", EVENT, eventRead);
            q++;
        }
        reader.close();
    }

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

    private CompletableFuture<Void> waitForSegmentsInStorage(Collection<String> segmentNames, StreamSegmentStore baseStore,
                                                             Storage storage) {
        ArrayList<CompletableFuture<Void>> segmentsCompletion = new ArrayList<>();
        SegmentProperties sp;
        for (String segmentName : segmentNames) {
            try {
                sp = baseStore.getStreamSegmentInfo(segmentName, TIMEOUT).join();
            } catch (Exception e) {
                log.info("Segment '{}' doesn't exist.", segmentName);
                continue;
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

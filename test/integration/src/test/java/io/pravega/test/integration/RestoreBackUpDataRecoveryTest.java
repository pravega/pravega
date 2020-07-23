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

import com.google.common.base.Preconditions;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.concurrent.Services;
import io.pravega.common.io.FileHelpers;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.CachePolicy;
import io.pravega.segmentstore.server.DataRecoveryTestUtils;
import io.pravega.segmentstore.server.OperationLogFactory;
import io.pravega.segmentstore.server.ReadIndexFactory;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.SegmentContainerExtension;
import io.pravega.segmentstore.server.SegmentsTracker;
import io.pravega.segmentstore.server.WriterFactory;
import io.pravega.segmentstore.server.attributes.AttributeIndexConfig;
import io.pravega.segmentstore.server.attributes.AttributeIndexFactory;
import io.pravega.segmentstore.server.attributes.ContainerAttributeIndexFactoryImpl;
import io.pravega.segmentstore.server.containers.ContainerConfig;
import io.pravega.segmentstore.server.containers.DebugStreamSegmentContainer;
import io.pravega.segmentstore.server.containers.StreamSegmentContainerFactory;
import io.pravega.segmentstore.server.host.delegationtoken.PassingTokenVerifier;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.host.stat.AutoScaleMonitor;
import io.pravega.segmentstore.server.host.stat.AutoScalerConfig;
import io.pravega.segmentstore.server.logs.DurableLogConfig;
import io.pravega.segmentstore.server.logs.DurableLogFactory;
import io.pravega.segmentstore.server.reading.ContainerReadIndexFactory;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.tables.ContainerTableExtension;
import io.pravega.segmentstore.server.tables.ContainerTableExtensionImpl;
import io.pravega.segmentstore.server.writer.StorageWriterFactory;
import io.pravega.segmentstore.server.writer.WriterConfig;
import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.cache.CacheStorage;
import io.pravega.segmentstore.storage.cache.DirectMemoryCache;
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

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Integration test to verify data recovery.
 * Recovery scenario: when data written to Pravega is already flushed to the long term storage.
 */
@Slf4j
public class RestoreBackUpDataRecoveryTest extends ThreadPooledTestSuite {
    protected static final Duration TIMEOUT = Duration.ofMillis(60000 * 1000);

    private static final int CONTAINER_COUNT = 1;
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

    private final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(1);
    private final StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(scalingPolicy).build();

    private File baseDir;
    private StorageFactory storageFactory;
    private BookKeeperLogFactory dataLogFactory;
    private SegmentStoreStarter segmentStoreStarter;
    private BKZK bkzk = null;

    @After
    public void tearDown() throws Exception {
        if (this.dataLogFactory != null) {
            this.dataLogFactory.close();
            this.dataLogFactory = null;
        }

        if (this.segmentStoreStarter != null) {
            this.segmentStoreStarter.close();
            this.segmentStoreStarter = null;
        }

        if (this.bkzk != null) {
            this.bkzk.close();
            this.bkzk = null;
        }

        if (this.baseDir != null) {
            FileHelpers.deleteFileOrDirectory(this.baseDir);
            this.baseDir = null;
        }
    }

    @Override
    protected int getThreadPoolSize() {
        return 100;
    }

    BKZK setUpNewBK(int instanceId) throws Exception {
        return new BKZK(instanceId);
    }

    /**
     * Sets up a new BookKeeper & ZooKeeper.
     */
    private static class BKZK implements AutoCloseable {
        private final int bookieCount = 1;
        private AtomicReference<BookKeeperConfig> bkConfig = new AtomicReference<>();
        private AtomicReference<CuratorFramework> zkClient = new AtomicReference<>();
        private BookKeeperServiceRunner bookKeeperServiceRunner;
        private AtomicReference<BookKeeperServiceRunner> bkService = new AtomicReference<>();
        private int bkPort;

        BKZK(int instanceId) throws Exception {
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

    DebugTool createDebugTool(BookKeeperLogFactory dataLogFactory, StorageFactory storageFactory, ScheduledExecutorService
            scheduledExecutorService) {
        return new DebugTool(dataLogFactory, storageFactory, scheduledExecutorService);
    }

    /**
     * Sets up the environment for creating a DebugSegmentContainer.
     */
    private static class DebugTool implements AutoCloseable {
        private final CacheStorage cacheStorage;
        private final OperationLogFactory operationLogFactory;
        private final ReadIndexFactory readIndexFactory;
        private final AttributeIndexFactory attributeIndexFactory;
        private final WriterFactory writerFactory;
        private final CacheManager cacheManager;
        private final StreamSegmentContainerFactory containerFactory;
        private final BookKeeperLogFactory dataLogFactory;
        private final StorageFactory storageFactory;

        private final DurableLogConfig durableLogConfig = DurableLogConfig
                .builder()
                .with(DurableLogConfig.CHECKPOINT_MIN_COMMIT_COUNT, 1)
                .with(DurableLogConfig.CHECKPOINT_COMMIT_COUNT, 10)
                .with(DurableLogConfig.CHECKPOINT_TOTAL_COMMIT_LENGTH, 10L * 1024 * 1024L)
                .with(DurableLogConfig.START_RETRY_DELAY_MILLIS, 20)
                .build();

        private final ReadIndexConfig readIndexConfig = ReadIndexConfig.builder().with(ReadIndexConfig.STORAGE_READ_ALIGNMENT, 1024).build();
        private final AttributeIndexConfig attributeIndexConfig = AttributeIndexConfig
                .builder()
                .with(AttributeIndexConfig.MAX_INDEX_PAGE_SIZE, 2 * 1024)
                .with(AttributeIndexConfig.ATTRIBUTE_SEGMENT_ROLLING_SIZE, 1000)
                .build();
        private final WriterConfig writerConfig = WriterConfig
                .builder()
                .with(WriterConfig.FLUSH_THRESHOLD_BYTES, 1)
                .with(WriterConfig.FLUSH_THRESHOLD_MILLIS, 25L)
                .with(WriterConfig.MIN_READ_TIMEOUT_MILLIS, 10L)
                .with(WriterConfig.MAX_READ_TIMEOUT_MILLIS, 250L)
                .build();

        DebugTool(BookKeeperLogFactory dataLogFactory, StorageFactory storageFactory, ScheduledExecutorService
                  scheduledExecutorService) {
            Preconditions.checkNotNull(dataLogFactory);
            Preconditions.checkNotNull(storageFactory);
            this.dataLogFactory = dataLogFactory;
            this.storageFactory = storageFactory;
            this.operationLogFactory = new DurableLogFactory(durableLogConfig, this.dataLogFactory, scheduledExecutorService);
            this.cacheStorage = new DirectMemoryCache(Integer.MAX_VALUE / 5);
            this.cacheManager = new CacheManager(CachePolicy.INFINITE, this.cacheStorage, scheduledExecutorService);
            this.readIndexFactory = new ContainerReadIndexFactory(readIndexConfig, this.cacheManager, scheduledExecutorService);
            this.attributeIndexFactory = new ContainerAttributeIndexFactoryImpl(attributeIndexConfig, this.cacheManager, scheduledExecutorService);
            this.writerFactory = new StorageWriterFactory(writerConfig, scheduledExecutorService);
            ContainerConfig containerConfig = ServiceBuilderConfig.getDefaultConfig().getConfig(ContainerConfig::builder);
            this.containerFactory = new StreamSegmentContainerFactory(containerConfig, this.operationLogFactory,
                    this.readIndexFactory, this.attributeIndexFactory, this.writerFactory, this.storageFactory,
                    this::createContainerExtensions, scheduledExecutorService);
        }

        private Map<Class<? extends SegmentContainerExtension>, SegmentContainerExtension> createContainerExtensions(
                SegmentContainer container, ScheduledExecutorService executor) {
            return Collections.singletonMap(ContainerTableExtension.class, new ContainerTableExtensionImpl(container, this.cacheManager, executor));
        }

        @Override
        public void close() {
            this.readIndexFactory.close();
            this.cacheManager.close();
            this.cacheStorage.close();
            this.dataLogFactory.close();
        }
    }

    SegmentStoreStarter startSegmentStore(StorageFactory storageFactory, BookKeeperLogFactory dataLogFactory) throws DurableDataLogException {
        return new SegmentStoreStarter(storageFactory, dataLogFactory);
    }

    /**
     * Creates a segment store server.
     */
    private static class SegmentStoreStarter {
        private final int servicePort = TestUtils.getAvailableListenPort();
        private ServiceBuilder serviceBuilder;
        private SegmentsTracker segmentsTracker;
        private AutoScaleMonitor monitor;
        private PravegaConnectionListener server;

        SegmentStoreStarter(StorageFactory storageFactory, BookKeeperLogFactory dataLogFactory) throws DurableDataLogException {
            if (storageFactory != null) {
                if (dataLogFactory != null) {
                    this.serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig())
                            .withStorageFactory(setup -> storageFactory)
                            .withDataLogFactory(setup -> dataLogFactory);
                } else {
                    this.serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig())
                            .withStorageFactory(setup -> storageFactory);
                }
            } else {
                this.serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
            }
            this.serviceBuilder.initialize();
            this.segmentsTracker = new SegmentsTracker(serviceBuilder.createStreamSegmentService(), serviceBuilder.createTableStoreService());
            this.monitor = new AutoScaleMonitor(this.segmentsTracker, AutoScalerConfig.builder().build());
            this.server = new PravegaConnectionListener(false, false, "localhost", servicePort, this.segmentsTracker,
                    this.segmentsTracker, monitor.getStatsRecorder(), monitor.getTableSegmentStatsRecorder(), new PassingTokenVerifier(),
                    null, null, true, serviceBuilder.getLowPriorityExecutor());
            this.server.startListening();
        }

        public void close() {
            if (this.server != null) {
                this.server.close();
                this.server = null;
            }

            if (this.monitor != null) {
                this.monitor.close();
                this.monitor = null;
            }

            if (this.serviceBuilder != null) {
                this.serviceBuilder.close();
                this.serviceBuilder = null;
            }
        }
    }

    ControllerStarter startController(int bkPort, int servicePort) throws InterruptedException {
        return new ControllerStarter(bkPort, servicePort);
    }

    /**
     * Creates a controller instance and runs it.
     */
    private static class ControllerStarter {
        private final int controllerPort = TestUtils.getAvailableListenPort();
        private final String serviceHost = "localhost";
        private ControllerWrapper controllerWrapper = null;
        private Controller controller = null;

        ControllerStarter(int bkPort, int servicePort) throws InterruptedException {
            this.controllerWrapper = new ControllerWrapper("localhost:" + bkPort, false,
                    controllerPort, serviceHost, servicePort, CONTAINER_COUNT);
            this.controllerWrapper.awaitRunning();
            this.controller = controllerWrapper.getController();
        }

        public void close() throws Exception {
            if (this.controller != null) {
                this.controller.close();
                this.controller = null;
            }

            if (this.controllerWrapper != null) {
                this.controllerWrapper.close();
                this.controllerWrapper = null;
            }
        }
    }

    @Test(timeout = 240000)
    public void testDurableDataLogFail() throws Exception {
        ScheduledExecutorService executorService = executorService();
        int instanceId = 0;
        // Creating a long term storage only once here.
        this.storageFactory = new InMemoryStorageFactory(executorService);
        log.info("Created a long term storage.");

        // Start a new BK & ZK, segment store and controller
        this.bkzk = setUpNewBK(instanceId++);
        this.segmentStoreStarter = startSegmentStore(this.storageFactory, null);
        @Cleanup
        ControllerStarter controllerStarter = startController(this.bkzk.bkPort, this.segmentStoreStarter.servicePort);

        // Create two streams for writing data onto two different segments
        createScopeStream(controllerStarter.controller, SCOPE, STREAM1);
        createScopeStream(controllerStarter.controller, SCOPE, STREAM2);
        log.info("Created two streams.");

        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE, controllerStarter.controller, connectionFactory);
        @Cleanup
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(SCOPE, controllerStarter.controller, clientFactory, connectionFactory);

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
        ConcurrentHashMap<String, Boolean> allSegments = this.segmentStoreStarter.segmentsTracker.getSegments();
        log.info("No. of segments created = {}", allSegments.size());

        // Get the long term storage from the running pravega instance
        @Cleanup
        Storage storage = new AsyncStorageWrapper(new RollingStorage(this.storageFactory.createSyncStorage(),
                new SegmentRollingPolicy(DEFAULT_ROLLING_SIZE)), executorService);

        // wait for all segments to be flushed to the long term storage.
        waitForSegmentsInStorage(allSegments.keySet(), this.segmentStoreStarter.segmentsTracker, storage)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        this.segmentStoreStarter.close(); // Shutdown SegmentStore
        this.segmentStoreStarter = null;
        log.info("Segment Store Shutdown");

        this.bkzk.close(); // Shutdown BookKeeper & ZooKeeper
        this.bkzk = null;
        log.info("BookKeeper & ZooKeeper shutdown");

        // start a new BookKeeper and ZooKeeper.
        this.bkzk = setUpNewBK(instanceId++);
        this.dataLogFactory = new BookKeeperLogFactory(this.bkzk.bkConfig.get(), this.bkzk.zkClient.get(), executorService);
        this.dataLogFactory.initialize();
        log.info("Started a new BookKeeper and ZooKeeper.");

        Map<Integer, DebugStreamSegmentContainer> debugStreamSegmentContainerMap = new HashMap<>();

        // Start debug segment container using dataLogFactory from new BK instance and old long term storageFactory.
        DebugTool debugTool = createDebugTool(this.dataLogFactory, this.storageFactory, executorService);

        // Start a debug segment container with given id and recover all segments belonging to that container
        for (int containerId = 0; containerId < CONTAINER_COUNT; containerId++) {
            DebugStreamSegmentContainer debugStreamSegmentContainer = (DebugStreamSegmentContainer)
                    debugTool.containerFactory.createDebugStreamSegmentContainer(containerId);
            Services.startAsync(debugStreamSegmentContainer, executorService).join();
            debugStreamSegmentContainerMap.put(containerId, debugStreamSegmentContainer);
        }

        DataRecoveryTestUtils.recoverAllSegments(storage, debugStreamSegmentContainerMap, executorService);

        // Re-create all segments which were listed.
        for (int containerId = 0; containerId < CONTAINER_COUNT; containerId++) {
            Services.stopAsync(debugStreamSegmentContainerMap.get(containerId), executorService).join();
            debugStreamSegmentContainerMap.get(containerId).close();
        }
        debugTool.close();
        log.info("Segments have been recovered.");

        // Start a new segment store and controller
        this.segmentStoreStarter = startSegmentStore(this.storageFactory, this.dataLogFactory);
        controllerStarter = startController(this.bkzk.bkPort, this.segmentStoreStarter.servicePort);
        log.info("Started segment store and controller again.");

        connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());
        clientFactory = new ClientFactoryImpl(SCOPE, controllerStarter.controller, connectionFactory);
        readerGroupManager = new ReaderGroupManagerImpl(SCOPE, controllerStarter.controller, clientFactory, connectionFactory);

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
        try (ConnectionFactory cf = new ConnectionFactoryImpl(ClientConfig.builder().build());
             StreamManager streamManager = new StreamManagerImpl(controller, cf)) {
            streamManager.createScope(scopeName);
            streamManager.createStream(scopeName, streamName, config);
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

    private CompletableFuture<Void> waitForSegmentsInStorage(Collection<String> segmentNames, StreamSegmentStore baseStore,
                                                             Storage tier2) {
        ArrayList<CompletableFuture<Void>> segmentsCompletion = new ArrayList<>();
        for (String segmentName : segmentNames) {
            SegmentProperties sp = baseStore.getStreamSegmentInfo(segmentName, TIMEOUT).join();
            log.info("Segment properties = {}", sp);
            segmentsCompletion.add(waitForSegmentInStorage(sp, tier2));
        }

        return Futures.allOf(segmentsCompletion);
    }

    private CompletableFuture<Void> waitForSegmentInStorage(SegmentProperties sp, Storage tier2) {
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
                    val segInfo = getStorageSegmentInfo(sp.getName(), timer, tier2);
                    val attrInfo = getStorageSegmentInfo(attributeSegmentName, timer, tier2);
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

    private CompletableFuture<SegmentProperties> getStorageSegmentInfo(String segmentName, TimeoutTimer timer, Storage tier2) {
        return Futures
                .exceptionallyExpecting(tier2.getStreamSegmentInfo(segmentName, timer.getRemaining()),
                        ex -> ex instanceof StreamSegmentNotExistsException,
                        StreamSegmentInformation.builder().name(segmentName).deleted(true).build());
    }
}

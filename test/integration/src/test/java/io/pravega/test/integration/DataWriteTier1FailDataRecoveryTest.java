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
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.concurrent.Services;
import io.pravega.common.io.FileHelpers;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.StreamSegmentStoreWrapper;
import io.pravega.segmentstore.contracts.tables.TableStoreWrapper;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.CachePolicy;
import io.pravega.segmentstore.server.DataRecoveryTestUtils;
import io.pravega.segmentstore.server.OperationLogFactory;
import io.pravega.segmentstore.server.ReadIndexFactory;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.SegmentContainerExtension;
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
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.cache.CacheStorage;
import io.pravega.segmentstore.storage.cache.DirectMemoryCache;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperServiceRunner;
import io.pravega.segmentstore.storage.mocks.InMemoryDurableDataLogFactory;
import io.pravega.segmentstore.storage.rolling.RollingStorage;
import io.pravega.shared.NameUtils;
import io.pravega.storage.filesystem.FileSystemStorageConfig;
import io.pravega.storage.filesystem.FileSystemStorageFactory;
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
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Integration test to verify data recovery.
 * Recovery scenario: when data written to Pravega is already flushed to the long term storage.
 */
@Slf4j
public class DataWriteTier1FailDataRecoveryTest extends ThreadPooledTestSuite {
    protected static final Duration TIMEOUT = Duration.ofMillis(60000 * 1000);

    private static final int CONTAINER_COUNT = 1;
    private static final int CONTAINER_ID = 0;

    /**
     * Write 300 events to different segments.
     */
    private static final long TOTAL_NUM_EVENTS = 300;

    private static final String APPEND_FORMAT = "Segment_%s_Append_%d";
    private static final long DEFAULT_ROLLING_SIZE = (int) (APPEND_FORMAT.length() * 1.5);

    private static final Random RANDOM = new Random();

    /**
     * Scope and streams to read and write events.
     */
    private static final String SCOPE = "testMetricsScope";
    private static final String STREAM1 = "testMetricsStream" + RANDOM.nextInt(Integer.MAX_VALUE);
    private static final String STREAM2 = "testMetricsStream" + RANDOM.nextInt(Integer.MAX_VALUE);
    private static final String EVENT = "12345";

    private final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(1);
    private final StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(scalingPolicy).build();

    private ScheduledExecutorService executorService = createExecutorService(100);
    private InMemoryDurableDataLogFactory durableDataLogFactory = null;
    private File baseDir = null;
    private FileSystemStorageFactory storageFactory = null;
    private BookKeeperLogFactory dataLogFactory = null;
    private SegmentStoreStarter segmentStoreStarter = null;
    private ControllerStarter controllerStarter = null;
    private BKZK bkzk = null;

    @After
    public void tearDown() throws Exception {
        if (this.durableDataLogFactory != null) {
            this.durableDataLogFactory.close();
            this.durableDataLogFactory = null;
        }

        if (this.dataLogFactory != null) {
            this.dataLogFactory.close();
            this.dataLogFactory = null;
        }

        if (this.controllerStarter != null) {
            this.controllerStarter.close();
            this.controllerStarter = null;
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
    private class BKZK implements AutoCloseable {
        private final int writeCount = 500;
        private final int maxWriteAttempts = 3;
        private final int maxLedgerSize = 200 * Math.max(10, writeCount / 20);
        private final AtomicBoolean secureBk = new AtomicBoolean();
        private final int bookieCount = 1;
        private AtomicReference<BookKeeperConfig> bkConfig = new AtomicReference<>();
        private AtomicReference<CuratorFramework> zkClient = new AtomicReference<>();
        private BookKeeperServiceRunner bookKeeperServiceRunner;
        private AtomicReference<BookKeeperServiceRunner> bkService = new AtomicReference<>();
        private AtomicInteger bkPort = new AtomicInteger();
        private int instanceId;

        BKZK(int instanceId) throws Exception {
            this.instanceId = instanceId;
            secureBk.set(false);
            bkPort.set(TestUtils.getAvailableListenPort());
            val bookiePorts = new ArrayList<Integer>();
            for (int i = 0; i < bookieCount; i++) {
                bookiePorts.add(TestUtils.getAvailableListenPort());
            }

            this.bookKeeperServiceRunner = BookKeeperServiceRunner.builder()
                    .startZk(true)
                    .zkPort(bkPort.get())
                    .ledgersPath("/pravega/bookkeeper/ledgers")
                    .secureBK(isSecure())
                    .secureZK(isSecure())
                    .tlsTrustStore("../segmentstore/config/bookie.truststore.jks")
                    .tLSKeyStore("../segmentstore/config/bookie.keystore.jks")
                    .tLSKeyStorePasswordPath("../segmentstore/config/bookie.keystore.jks.passwd")
                    .bookiePorts(bookiePorts)
                    .build();
            this.bookKeeperServiceRunner.startAll();
            bkService.set(this.bookKeeperServiceRunner);

            // Create a ZKClient with a unique namespace.
            String baseNamespace = "pravega/" + this.instanceId + "_" + Long.toHexString(System.nanoTime());
            this.zkClient.set(CuratorFrameworkFactory
                    .builder()
                    .connectString("localhost:" + bkPort.get())
                    .namespace(baseNamespace)
                    .retryPolicy(new ExponentialBackoffRetry(1000, 5))
                    .build());

            this.zkClient.get().start();

            String logMetaNamespace = "segmentstore/containers" + this.instanceId;
            this.bkConfig.set(BookKeeperConfig
                    .builder()
                    .with(BookKeeperConfig.ZK_ADDRESS, "localhost:" + bkPort.get())
                    .with(BookKeeperConfig.MAX_WRITE_ATTEMPTS, maxWriteAttempts)
                    .with(BookKeeperConfig.BK_LEDGER_MAX_SIZE, maxLedgerSize)
                    .with(BookKeeperConfig.ZK_METADATA_PATH, logMetaNamespace)
                    .with(BookKeeperConfig.BK_LEDGER_PATH, "/pravega/bookkeeper/ledgers")
                    .with(BookKeeperConfig.BK_ENSEMBLE_SIZE, bookieCount)
                    .with(BookKeeperConfig.BK_WRITE_QUORUM_SIZE, bookieCount)
                    .with(BookKeeperConfig.BK_ACK_QUORUM_SIZE, bookieCount)
                    .with(BookKeeperConfig.BK_TLS_ENABLED, isSecure())
                    .with(BookKeeperConfig.BK_WRITE_TIMEOUT, 1000)
                    .build());
        }

        public boolean isSecure() {
            return secureBk.get();
        }

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

    DebugTool createDebugTool(BookKeeperLogFactory dataLogFactory, StorageFactory storageFactory) {
        return new DebugTool(dataLogFactory, storageFactory);
    }

    /**
     * Sets up the environment for creating a DebugSegmentContainer.
     */
    private class DebugTool implements AutoCloseable {
        private CacheStorage cacheStorage;
        private OperationLogFactory operationLogFactory;
        private ReadIndexFactory readIndexFactory;
        private AttributeIndexFactory attributeIndexFactory;
        private WriterFactory writerFactory;
        private CacheManager cacheManager;
        private StreamSegmentContainerFactory containerFactory;
        private BookKeeperLogFactory dataLogFactory = null;
        private StorageFactory storageFactory;

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

        DebugTool(BookKeeperLogFactory dataLogFactory, StorageFactory storageFactory) {
            this.dataLogFactory = dataLogFactory;
            this.storageFactory = storageFactory;
            this.operationLogFactory = new DurableLogFactory(durableLogConfig, this.dataLogFactory, executorService);

            this.cacheStorage = new DirectMemoryCache(Integer.MAX_VALUE);
            this.cacheManager = new CacheManager(CachePolicy.INFINITE, this.cacheStorage, executorService);
            this.readIndexFactory = new ContainerReadIndexFactory(readIndexConfig, this.cacheManager, executorService);
            this.attributeIndexFactory = new ContainerAttributeIndexFactoryImpl(attributeIndexConfig, this.cacheManager, executorService);
            this.writerFactory = new StorageWriterFactory(writerConfig, executorService);

            ContainerConfig containerConfig = ServiceBuilderConfig.getDefaultConfig().getConfig(ContainerConfig::builder);
            this.containerFactory = new StreamSegmentContainerFactory(containerConfig, this.operationLogFactory,
                    this.readIndexFactory, this.attributeIndexFactory, this.writerFactory, this.storageFactory,
                    this::createContainerExtensions, executorService);
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
    private class SegmentStoreStarter {
        private int servicePort = TestUtils.getAvailableListenPort();
        private ServiceBuilder serviceBuilder = null;
        private StreamSegmentStoreWrapper streamSegmentStoreWrapper = null;
        private AutoScaleMonitor monitor = null;
        private TableStoreWrapper tableStoreWrapper = null;
        private PravegaConnectionListener server = null;

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
            this.streamSegmentStoreWrapper = new StreamSegmentStoreWrapper(serviceBuilder.createStreamSegmentService());
            this.monitor = new AutoScaleMonitor(streamSegmentStoreWrapper, AutoScalerConfig.builder().build());
            this.tableStoreWrapper = new TableStoreWrapper(serviceBuilder.createTableStoreService());
            this.server = new PravegaConnectionListener(false, false, "localhost", servicePort, streamSegmentStoreWrapper,
                    tableStoreWrapper, monitor.getStatsRecorder(), monitor.getTableSegmentStatsRecorder(), new PassingTokenVerifier(),
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
    private class ControllerStarter {
        private int controllerPort = TestUtils.getAvailableListenPort();
        private String serviceHost = "localhost";
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

    @Test(timeout = 800000)
    public void testTier1Fail() throws Exception {
        // Creating tier 2 only once here.
        this.baseDir = Files.createTempDirectory("test_nfs").toFile().getAbsoluteFile();
        FileSystemStorageConfig fsConfig = FileSystemStorageConfig
                .builder()
                .with(FileSystemStorageConfig.ROOT, this.baseDir.getAbsolutePath())
                .build();
        this.storageFactory = new FileSystemStorageFactory(fsConfig, executorService);

        // Start a new BK & ZK, segment store and controller
        this.bkzk = setUpNewBK(0);
        this.segmentStoreStarter = startSegmentStore(this.storageFactory, null);
        log.info("First bk Port = {}", this.bkzk.bkPort.get());
        @Cleanup ControllerStarter controllerStarter = startController(this.bkzk.bkPort.get(), this.segmentStoreStarter.servicePort);

        createScopeStream(controllerStarter.controller, SCOPE, STREAM1);
        createScopeStream(controllerStarter.controller, SCOPE, STREAM2);

        @Cleanup ConnectionFactory connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());
        @Cleanup ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE, controllerStarter.controller, connectionFactory);
        @Cleanup ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(SCOPE, controllerStarter.controller, clientFactory, connectionFactory);

        writeEvents(STREAM1, clientFactory); // write 300 events on one segment
        writeEvents(STREAM2, clientFactory); // write 300 events on other segment

        // Verify events write by reading them.
        readAllEvents(STREAM1, clientFactory, readerGroupManager, "RG" + RANDOM.nextInt(Integer.MAX_VALUE),
                "R" + RANDOM.nextInt(Integer.MAX_VALUE));
        log.info("First read on stream 1");
        readAllEvents(STREAM2, clientFactory, readerGroupManager, "RG" + RANDOM.nextInt(Integer.MAX_VALUE),
                "R" + RANDOM.nextInt(Integer.MAX_VALUE));
        log.info("First read on stream 2");

        readerGroupManager.close();
        clientFactory.close();

        controllerStarter.close(); // Shut down the controller

        // Get names of all the segments created.
        HashSet<String> allSegments = new HashSet<>(this.segmentStoreStarter.streamSegmentStoreWrapper.getSegments());
        allSegments.addAll(this.segmentStoreStarter.tableStoreWrapper.getSegments());
        log.info("No. of segments = {}", allSegments.size());

        Storage tier2 = new AsyncStorageWrapper(new RollingStorage(this.storageFactory.createSyncStorage(),
                new SegmentRollingPolicy(DEFAULT_ROLLING_SIZE)), DataRecoveryTestUtils.createExecutorService(1));

        // wait for all segments to be flushed to the long term storage.
        waitForSegmentsInStorage(allSegments, this.segmentStoreStarter.streamSegmentStoreWrapper, tier2)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        sleep(5000); // Sleep to make sure all segments got flushed properly

        this.segmentStoreStarter.close(); // Shutdown SS
        log.info("Segment Store Shutdown");

        this.bkzk.close();
        log.info("BookKeeper & ZooKeeper shutdown");

        // start a new BookKeeper and ZooKeeper.
        this.bkzk = setUpNewBK(1);
        this.dataLogFactory = new BookKeeperLogFactory(this.bkzk.bkConfig.get(), this.bkzk.zkClient.get(), executorService);
        this.dataLogFactory.initialize();

        // Delete container metadata segment and attributes index segment corresponding to the container Id from the long term storage
        deleteSegment("_system/containers/metadata_" + CONTAINER_ID, tier2);
        deleteSegment("_system/containers/metadata_" + CONTAINER_ID + "$attributes.index", tier2);

        // List all segments from the long term storage
        Map<Integer, List<SegmentProperties>> segmentsToCreate = DataRecoveryTestUtils.listAllSegments(tier2, CONTAINER_COUNT);

        tier2.close(); // shut down tier 2

        // Start debug segment container using dataLogFactory from new BK instance and old long term storageFactory.
        DebugTool debugTool = createDebugTool(this.dataLogFactory, this.storageFactory);
        DebugStreamSegmentContainer debugStreamSegmentContainer = (DebugStreamSegmentContainer)
                debugTool.containerFactory.createDebugStreamSegmentContainer(CONTAINER_ID);

        // Re-create all segments which were listed.
        Services.startAsync(debugStreamSegmentContainer, executorService)
                .thenRun(new DataRecoveryTestUtils.Worker(debugStreamSegmentContainer, segmentsToCreate.get(CONTAINER_ID))).join();
        sleep(5000);
        Services.stopAsync(debugStreamSegmentContainer, executorService).join();
        debugStreamSegmentContainer.close();
        debugTool.close();

        // Start a new segment store and controller
        this.segmentStoreStarter = startSegmentStore(this.storageFactory, this.dataLogFactory);
        log.info("Second bk Port = {}", this.bkzk.bkPort.get());
        controllerStarter = startController(this.bkzk.bkPort.get(), this.segmentStoreStarter.servicePort);

        connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());
        clientFactory = new ClientFactoryImpl(SCOPE, controllerStarter.controller, connectionFactory);
        readerGroupManager = new ReaderGroupManagerImpl(SCOPE, controllerStarter.controller, clientFactory, connectionFactory);

        createScopeStream(controllerStarter.controller, SCOPE, STREAM1);
        createScopeStream(controllerStarter.controller, SCOPE, STREAM2);

        // Try reading all events again
        readAllEvents(STREAM1, clientFactory, readerGroupManager, "RG" + RANDOM.nextInt(Integer.MAX_VALUE),
                "R" + RANDOM.nextInt(Integer.MAX_VALUE));
        log.info("Second read on stream 1");
        readAllEvents(STREAM2, clientFactory, readerGroupManager, "RG" + RANDOM.nextInt(Integer.MAX_VALUE),
                "R" + RANDOM.nextInt(Integer.MAX_VALUE));
        log.info("Second read on stream 2");
    }

    public ScheduledExecutorService createExecutorService(int threadPoolSize) {
        ScheduledThreadPoolExecutor es = new ScheduledThreadPoolExecutor(threadPoolSize);
        es.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        es.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        es.setRemoveOnCancelPolicy(true);
        return es;
    }

    private void deleteSegment(String segmentName, Storage tier2) {
        SegmentHandle segmentHandle = tier2.openWrite(segmentName).join();
        tier2.delete(segmentHandle, TIMEOUT).join();
    }

    public void createScopeStream(Controller controller, String scopeName, String streamName) {
        try (ConnectionFactory cf = new ConnectionFactoryImpl(ClientConfig.builder().build());
             StreamManager streamManager = new StreamManagerImpl(controller, cf)) {
            boolean createScopeStatus = streamManager.createScope(scopeName);

            boolean createStreamStatus = streamManager.createStream(scopeName, streamName, config);
        }
    }

    private void writeEvents(String streamName, ClientFactoryImpl clientFactory) {
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
                new UTF8StringSerializer(),
                EventWriterConfig.builder().build());
        for (int i = 0; i < TOTAL_NUM_EVENTS;) {
            try {
                writer.writeEvent("", EVENT).join();
                i++;
            } catch (Throwable e) {
                Assert.fail("Error occurred while writing events.");
                break;
            }
        }
        writer.flush();
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
            try {
                String eventRead = reader.readNextEvent(SECONDS.toMillis(500)).getEvent();
                Assert.assertEquals("Event written and read back don't match", EVENT, eventRead);
                q++;
            } catch (Exception e) {
                Assert.fail("Error occurred while reading the events.");
            }
        }
    }

    private CompletableFuture<Void> waitForSegmentsInStorage(Collection<String> segmentNames, StreamSegmentStore baseStore,
                                                             Storage tier2) {
        ArrayList<CompletableFuture<Void>> segmentsCompletion = new ArrayList<>();
        for (String segmentName : segmentNames) {
            log.info("Segment Name = {}", segmentName);
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
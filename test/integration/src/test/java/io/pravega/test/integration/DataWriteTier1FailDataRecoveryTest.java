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

import com.google.common.util.concurrent.Runnables;
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
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.AsyncIterator;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.IteratorItem;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.contracts.tables.TableStore;
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
import io.pravega.segmentstore.server.containers.StreamSegmentContainerTests;
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
import io.pravega.segmentstore.server.store.StreamSegmentServiceTests;
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
import io.pravega.shared.metrics.StatsProvider;
import io.pravega.storage.filesystem.FileSystemStorageConfig;
import io.pravega.storage.filesystem.FileSystemStorageFactory;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.ThreadPooledTestSuite;
import io.pravega.test.integration.demo.ControllerWrapper;
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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static io.pravega.shared.NameUtils.getMetadataSegmentName;
import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
public class DataWriteTier1FailDataRecoveryTest extends ThreadPooledTestSuite {
    protected static final Duration TIMEOUT = Duration.ofMillis(60000 * 1000);

    private static int CONTAINER_ID = 0;
    private static int WRITE_COUNT = 500;

    private static int MAX_WRITE_ATTEMPTS = 3;
    private static int MAX_LEDGER_SIZE = 200 * Math.max(10, WRITE_COUNT / 20);

    private ServiceBuilder serviceBuilder = null;
    private PravegaConnectionListener server = null;
    private ControllerWrapper controllerWrapper = null;
    private Controller controller = null;

    private StatsProvider statsProvider = null;
    private AutoScaleMonitor monitor = null;

    private static final Duration timeout = Duration.ofSeconds(20000);
    ScheduledExecutorService executorService = createExecutorService(100);
    private InMemoryDurableDataLogFactory durableDataLogFactory;
    private File baseDir;
    private Storage storage = null;
    private int servicePort;
    private FileSystemStorageFactory storageFactory;
    private BookKeeperLogFactory dataLogFactory;
    protected final String APPEND_FORMAT = "Segment_%s_Append_%d";
    protected final long DEFAULT_ROLLING_SIZE = (int) (APPEND_FORMAT.length() * 1.5);
    final int containerCount = 1;
    private static final String stream1 = "testMetricsStream" + new Random().nextInt(Integer.MAX_VALUE);
    private static final String stream2 = "testMetricsStream" + new Random().nextInt(Integer.MAX_VALUE);
    private static long TOTAL_NUM_EVENTS = 300;
    private final String readerName = "reader" + new Random().nextInt(Integer.MAX_VALUE);

    private final String scope = "testMetricsScope";
    private final String readerGroupName = "testMetricsReaderGroup";
    private final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(1);
    private final StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(scalingPolicy).build();
    private StreamSegmentStore store;

    @After
    public void tearDown() throws Exception {

        if (this.controllerWrapper != null) {
            this.controllerWrapper.close();
            this.controllerWrapper = null;
        }

        if (this.server != null) {
            this.server.close();
            this.server = null;
        }

        if (this.serviceBuilder != null) {
            this.serviceBuilder.close();
            this.serviceBuilder = null;
        }

        if (this.durableDataLogFactory != null) {
            this.durableDataLogFactory.close();
            this.durableDataLogFactory = null;
        }

        if (this.storage != null) {
            this.storage.close();
            this.storage = null;
        }

        if (this.dataLogFactory != null) {
            this.dataLogFactory.close();
            this.dataLogFactory = null;
        }


    }

    @Override
    protected int getThreadPoolSize() {
        return 100;
    }



    BKZK setUpNewBK() throws Exception {
        return new BKZK();
    }

    private class BKZK implements AutoCloseable {
        private final AtomicBoolean SECURE_BK = new AtomicBoolean();
        private AtomicReference<BookKeeperConfig> bkConfig = new AtomicReference<>();
        private AtomicReference<CuratorFramework> zkClient = new AtomicReference<>();
        private int BOOKIE_COUNT = 1;
        private BookKeeperServiceRunner bookKeeperServiceRunner;
        private String namespace;
        private AtomicReference<BookKeeperServiceRunner> BK_SERVICE = new AtomicReference<>();
        private AtomicInteger BK_PORT = new AtomicInteger();

        BKZK() throws Exception {
            SECURE_BK.set(false);
            BK_PORT.set(TestUtils.getAvailableListenPort());
            val bookiePorts = new ArrayList<Integer>();
            for (int i = 0; i < BOOKIE_COUNT; i++) {
                bookiePorts.add(TestUtils.getAvailableListenPort());
            }

            this.bookKeeperServiceRunner = getBookKeeperServiceRunner(bookiePorts);

            this.bookKeeperServiceRunner.startAll();
            BK_SERVICE.set(this.bookKeeperServiceRunner);
            setZkClient();

            this.zkClient.get().start();

            setBkConfig();
        }

        public void setZkClient() {
            // Create a ZKClient with a unique namespace.
            this.namespace = "pravega/segmentstore/unittest_" + Long.toHexString(System.nanoTime());
            this.zkClient.set(CuratorFrameworkFactory
                    .builder()
                    .connectString("localhost:" + BK_PORT.get())
                    .namespace(this.namespace)
                    .retryPolicy(new ExponentialBackoffRetry(1000, 5))
                    .build());
            return;
        }

        public void setBkConfig() {
            // Setup config to use the port and namespace.
            this.bkConfig.set(BookKeeperConfig
                    .builder()
                    .with(BookKeeperConfig.ZK_ADDRESS, "localhost:" + BK_PORT.get())
                    .with(BookKeeperConfig.MAX_WRITE_ATTEMPTS, MAX_WRITE_ATTEMPTS)
                    .with(BookKeeperConfig.BK_LEDGER_MAX_SIZE, MAX_LEDGER_SIZE)
                    .with(BookKeeperConfig.ZK_METADATA_PATH, namespace)
                    .with(BookKeeperConfig.BK_LEDGER_PATH, "/pravega/bookkeeper/ledgers")
                    .with(BookKeeperConfig.BK_ENSEMBLE_SIZE, BOOKIE_COUNT)
                    .with(BookKeeperConfig.BK_WRITE_QUORUM_SIZE, BOOKIE_COUNT)
                    .with(BookKeeperConfig.BK_ACK_QUORUM_SIZE, BOOKIE_COUNT)
                    .with(BookKeeperConfig.BK_TLS_ENABLED, isSecure())
                    .with(BookKeeperConfig.BK_WRITE_TIMEOUT, 1000) // This is the minimum we can set anyway.
                    .build())
            ;
            return;
        }

        public BookKeeperServiceRunner getBookKeeperServiceRunner(List<Integer> bookiePorts) {
            return BookKeeperServiceRunner.builder()
                    .startZk(true)
                    .zkPort(BK_PORT.get())
                    .ledgersPath("/pravega/bookkeeper/ledgers")
                    .secureBK(isSecure())
                    .secureZK(isSecure())
                    .tlsTrustStore("/home/manish/DR/pravega/config/bookie.truststore.jks")
                    .tLSKeyStore("/home/manish/DR/pravega/config/bookie.keystore.jks")
                    .tLSKeyStorePasswordPath("/home/manish/DR/pravega/config/bookie.keystore.jks.passwd")
                    .bookiePorts(bookiePorts)
                    .build();
        }

        public boolean isSecure() {
            return SECURE_BK.get();
        }

        @Override
        public void close() throws Exception {
            val process = BK_SERVICE.getAndSet(null);
            if (process != null) {
                process.close();
            }

            val zkClient = this.zkClient.getAndSet(null);
            if (zkClient != null) {
                zkClient.close();
            }
            bookKeeperServiceRunner.close();
            BK_SERVICE.getAndSet(null).close();
        }
    }

    DebugTool createDebugTool(BookKeeperLogFactory dataLogFactory, StorageFactory storageFactory) {
        return new DebugTool(dataLogFactory, storageFactory);
    }

    private class DebugTool implements AutoCloseable {
        private CacheStorage cacheStorage;
        private OperationLogFactory operationLogFactory;
        private ReadIndexFactory readIndexFactory;
        private AttributeIndexFactory attributeIndexFactory;
        private WriterFactory writerFactory;
        private CacheManager cacheManager;
        private StreamSegmentContainerFactory containerFactory;
        private BookKeeperLogFactory dataLogFactory;
        private StorageFactory storageFactory;
        private Storage storage = null;

        private DurableLogConfig DEFAULT_DURABLE_LOG_CONFIG = DurableLogConfig
                .builder()
                .with(DurableLogConfig.CHECKPOINT_MIN_COMMIT_COUNT, 1)
                .with(DurableLogConfig.CHECKPOINT_COMMIT_COUNT, 10)
                .with(DurableLogConfig.CHECKPOINT_TOTAL_COMMIT_LENGTH, 10L * 1024 * 1024L)
                .with(DurableLogConfig.START_RETRY_DELAY_MILLIS, 20)
                .build();

        private final ReadIndexConfig DEFAULT_READ_INDEX_CONFIG = ReadIndexConfig.builder().with(ReadIndexConfig.STORAGE_READ_ALIGNMENT, 1024).build();
        private final AttributeIndexConfig DEFAULT_ATTRIBUTE_INDEX_CONFIG = AttributeIndexConfig
                .builder()
                .with(AttributeIndexConfig.MAX_INDEX_PAGE_SIZE, 2 * 1024)
                .with(AttributeIndexConfig.ATTRIBUTE_SEGMENT_ROLLING_SIZE, 1000)
                .build();
        private final WriterConfig DEFAULT_WRITER_CONFIG = WriterConfig
                .builder()
                .with(WriterConfig.FLUSH_THRESHOLD_BYTES, 1)
                .with(WriterConfig.FLUSH_THRESHOLD_MILLIS, 25L)
                .with(WriterConfig.MIN_READ_TIMEOUT_MILLIS, 10L)
                .with(WriterConfig.MAX_READ_TIMEOUT_MILLIS, 250L)
                .build();

        DebugTool(BookKeeperLogFactory dataLogFactory, StorageFactory storageFactory) {
            this.dataLogFactory = dataLogFactory;
            this.storageFactory = storageFactory;
            this.operationLogFactory = new DurableLogFactory(DEFAULT_DURABLE_LOG_CONFIG, this.dataLogFactory, executorService);

            this.cacheStorage = new DirectMemoryCache(Integer.MAX_VALUE);
            this.cacheManager = new CacheManager(CachePolicy.INFINITE, this.cacheStorage, executorService);
            this.readIndexFactory = new ContainerReadIndexFactory(DEFAULT_READ_INDEX_CONFIG, this.cacheManager, executorService);
            this.attributeIndexFactory = new ContainerAttributeIndexFactoryImpl(DEFAULT_ATTRIBUTE_INDEX_CONFIG, this.cacheManager, executorService);
            this.writerFactory = new StorageWriterFactory(DEFAULT_WRITER_CONFIG, executorService);

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
            this.dataLogFactory.close();
            this.cacheManager.close();
            this.cacheStorage.close();
        }
    }

    void startSegmentStore(StorageFactory storageFactory, BookKeeperLogFactory dataLogFactory) throws DurableDataLogException {
        this.servicePort = TestUtils.getAvailableListenPort();

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
        this.store = serviceBuilder.createStreamSegmentService(); // Creating SS
        this.monitor = new AutoScaleMonitor(store, AutoScalerConfig.builder().build());
        TableStore tableStore = serviceBuilder.createTableStoreService();

        this.server = new PravegaConnectionListener(false, false, "localhost", servicePort, store, tableStore,
                this.monitor.getStatsRecorder(), monitor.getTableSegmentStatsRecorder(), new PassingTokenVerifier(),
                null, null, true, this.serviceBuilder.getLowPriorityExecutor());
        this.server.startListening();
    }

    void startController(int bk_port) throws InterruptedException {
        int controllerPort = TestUtils.getAvailableListenPort();
        String serviceHost = "localhost";

        this.controllerWrapper = new ControllerWrapper("localhost:" + bk_port, false,
                controllerPort, serviceHost, this.servicePort, containerCount);
        this.controllerWrapper.awaitRunning();
        this.controller = controllerWrapper.getController();
    }

    @Test(timeout = 1200000000)
    public void testTier1Fail() throws Exception {
        // Creating tier 2 only once here.
        this.baseDir = Files.createTempDirectory("test_nfs").toFile().getAbsoluteFile();
        FileSystemStorageConfig fsConfig = FileSystemStorageConfig
                .builder()
                .with(FileSystemStorageConfig.ROOT, this.baseDir.getAbsolutePath())
                .build();
        this.storageFactory = new FileSystemStorageFactory(fsConfig, executorService);
        this.storage = this.storageFactory.createStorageAdapter();

        BKZK bkzk = setUpNewBK();
        startSegmentStore(this.storageFactory, null);
        startController(bkzk.BK_PORT.get());

        createScopeStream(scope, stream1);
        createScopeStream(scope, stream2);

        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory);
        ReaderGroupManager readerGroupManager = new ReaderGroupManagerImpl(scope, controller, clientFactory, connectionFactory);

        writeEvents(stream1, clientFactory); // write 300 events on one segment
        writeEvents(stream2, clientFactory); // write 300 events on other segment

        String readerGroupName1 = readerGroupName + "1";

        int time = 0;
        // Read all events for the first time
        readAllEvents(stream1, clientFactory, readerGroupManager, readerGroupName1, readerName, ++time);
        log.info("First Read");
        readAllEvents(stream1, clientFactory, readerGroupManager, readerGroupName1, readerName, ++time);
        log.info("Second Read");

        ContainerTableExtension ext = .getExtension(ContainerTableExtension.class);
        AsyncIterator<IteratorItem<TableKey>> it = ext.keyIterator(getMetadataSegmentName(containerId), null,
                Duration.ofSeconds(10)).join();
        Set<TableKey> segmentsInMD = new HashSet<>();
        it.forEachRemaining(k -> segmentsInMD.addAll(k.getEntries()), executorService).join();

        //sleep(120000); // Sleep for 120 s for tier2 flush
        waitForSegmentInStorage();

        this.controller.close(); // Shuts down controller
        this.controllerWrapper.close();

        //sleep(60000); // Tier2 Flush

        this.server.close();
        serviceBuilder.close(); // Shutdown SS
        log.info("SS Shutdown");
        bkzk.bookKeeperServiceRunner.close(); // Shuts down BK & ZK
        bkzk.BK_SERVICE.getAndSet(null).close();
        log.info("BK & ZK shutdown");

        bkzk = setUpNewBK();
        log.info("BK & ZK started again");

        this.dataLogFactory = new BookKeeperLogFactory(bkzk.bkConfig.get(), bkzk.zkClient.get(), executorService);
        this.dataLogFactory.initialize();

        DebugTool debugTool = createDebugTool(this.dataLogFactory, this.storageFactory);

        log.info("List segments");
        Storage tier2 = new AsyncStorageWrapper(new RollingStorage(this.storageFactory.createSyncStorage(),
                new SegmentRollingPolicy(DEFAULT_ROLLING_SIZE)), DataRecoveryTestUtils.createExecutorService(1));

        deleteSegment("_system/containers/metadata_0", tier2);
        deleteSegment("_system/containers/metadata_0$attributes.index", tier2);

        List<List<SegmentProperties>> segmentsToCreate = DataRecoveryTestUtils.listAllSegments(tier2, containerCount);

        log.info("Start DebugStreamSegmentContainer");
        DebugStreamSegmentContainer debugStreamSegmentContainer = (DebugStreamSegmentContainer)
                debugTool.containerFactory.createDebugStreamSegmentContainer(CONTAINER_ID);
        //DebugSegmentContainer debugSegmentContainer = debugTool.containerFactory.createDebugStreamSegmentContainer(CONTAINER_ID);
        debugStreamSegmentContainer.startAsync().awaitRunning();
        DataRecoveryTestUtils.createAllSegments(debugStreamSegmentContainer, segmentsToCreate.get(CONTAINER_ID));
        sleep(20000);
        debugStreamSegmentContainer.stopAsync().awaitTerminated();
        this.dataLogFactory.close();
        sleep(5000);
        startSegmentStore(this.storageFactory, this.dataLogFactory);
        startController(bkzk.BK_PORT.get());

        connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());
        clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory);
        readerGroupManager = new ReaderGroupManagerImpl(scope, controller, clientFactory, connectionFactory);

        createScopeStream(scope, stream1);
        createScopeStream(scope, stream2);

        readAllEvents(stream1, clientFactory, readerGroupManager, readerGroupName1, readerName, ++time);
        log.info("third Read");
        readAllEvents(stream1, clientFactory, readerGroupManager, readerGroupName1, readerName, ++time);
        log.info("fourth Read");
    }

    private CompletableFuture<Void> waitForSegmentsInStorage(Collection<String> segmentNames, SegmentContainer container,
                                                             StreamSegmentContainerTests.TestContext context) {
        ArrayList<CompletableFuture<Void>> segmentsCompletion = new ArrayList<>();
        for (String segmentName : segmentNames) {
            SegmentProperties sp = container.getStreamSegmentInfo(segmentName, TIMEOUT).join();
            segmentsCompletion.add(waitForSegmentInStorage(sp, context));
        }

        return Futures.allOf(segmentsCompletion);
    }

    private CompletableFuture<Void> waitForSegmentInStorage(SegmentProperties metadataProps, StreamSegmentContainerTests.TestContext context) {
        if (metadataProps.getLength() == 0) {
            // Empty segments may or may not exist in Storage, so don't bother complicating ourselves with this.
            return CompletableFuture.completedFuture(null);
        }

        // Check if the Storage Segment is caught up. If sealed, we want to make sure that both the Segment and its
        // Attribute Segment are sealed (or the latter has been deleted - for transactions). For all other, we want to
        // ensure that the length and truncation offsets have  caught up.
        BiFunction<SegmentProperties, SegmentProperties, Boolean> meetsConditions = (segmentProps, attrProps) ->
                metadataProps.isSealed() == (segmentProps.isSealed() && (attrProps.isSealed() || attrProps.isDeleted()))
                        && segmentProps.getLength() >= metadataProps.getLength()
                        && context.storageFactory.truncationOffsets.getOrDefault(metadataProps.getName(), 0L) >= metadataProps.getStartOffset();

        String attributeSegmentName = NameUtils.getAttributeSegmentName(metadataProps.getName());
        AtomicBoolean canContinue = new AtomicBoolean(true);
        TimeoutTimer timer = new TimeoutTimer(TIMEOUT);
        return Futures.loop(
                canContinue::get,
                () -> {
                    val segInfo = getStorageSegmentInfo(metadataProps.getName(), timer, context);
                    val attrInfo = getStorageSegmentInfo(attributeSegmentName, timer, context);
                    return CompletableFuture.allOf(segInfo, attrInfo)
                            .thenCompose(v -> {
                                if (meetsConditions.apply(segInfo.join(), attrInfo.join())) {
                                    canContinue.set(false);
                                    return CompletableFuture.completedFuture(null);
                                } else if (!timer.hasRemaining()) {
                                    return Futures.failedFuture(new TimeoutException());
                                } else {
                                    return Futures.delayedFuture(Duration.ofMillis(10), executorService());
                                }
                            }).thenRun(Runnables.doNothing());
                },
                executorService());
    }

    private CompletableFuture<SegmentProperties> getStorageSegmentInfo(String segmentName, TimeoutTimer timer, StreamSegmentContainerTests.TestContext context) {
        return Futures.exceptionallyExpecting(
                context.storage.getStreamSegmentInfo(segmentName, timer.getRemaining()),
                ex -> ex instanceof StreamSegmentNotExistsException,
                StreamSegmentInformation.builder().name(segmentName).deleted(true).build());
    }

    public static ScheduledExecutorService createExecutorService(int threadPoolSize) {
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

    public void createScopeStream(String scopeName, String streamName) {
        try (ConnectionFactory cf = new ConnectionFactoryImpl(ClientConfig.builder().build());
             StreamManager streamManager = new StreamManagerImpl(controller, cf)) {
            boolean createScopeStatus = streamManager.createScope(scopeName);

            boolean createStreamStatus = streamManager.createStream(scopeName, streamName, config);
        }
    }

    private void writeEvents(String StreamName, ClientFactoryImpl clientFactory) {
        EventStreamWriter<String> writer = clientFactory.createEventWriter(StreamName,
                new UTF8StringSerializer(),
                EventWriterConfig.builder().build());
        String event = "12345";
        for (int i = 0; i < TOTAL_NUM_EVENTS;) {
            try {
                log.info("Writing event {}", event);
                writer.writeEvent("", event).join();
                i++;
            } catch (Throwable e) {
                log.warn("Test exception writing events: {}", e);
                break;
            }
        }
        writer.flush();
    }

    private void readAllEvents(String streamName, ClientFactoryImpl clientFactory, ReaderGroupManager readerGroupManager,
                               String readerGroupName, String readerName, int time) {
        log.info("Creating Reader group : {}", readerGroupName);
        String timeTh = time + "th";
        readerGroupManager.createReaderGroup(readerGroupName,
                ReaderGroupConfig
                        .builder()
                        .stream(Stream.of(scope, streamName))
                        .automaticCheckpointIntervalMillis(2000)
                        .build());

        EventStreamReader<String> reader = clientFactory.createReader(readerName,
                readerGroupName,
                new UTF8StringSerializer(),
                ReaderConfig.builder().build());

        for (int q = 0; q < TOTAL_NUM_EVENTS;) {
            try {
                String eventRead2 = reader.readNextEvent(SECONDS.toMillis(500)).getEvent();
                Assert.assertNotNull(eventRead2);
                log.info("Reading event {} {}", eventRead2, timeTh);
                q++;
            } catch (ReinitializationRequiredException e) {
                log.warn("Test Exception while reading from the stream", e);
            }
        }
        reader.close();
        readerGroupManager.deleteReaderGroup(readerGroupName);
    }
}

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

import io.pravega.client.stream.impl.Controller;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
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
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.After;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.Thread.sleep;

@Slf4j
public class Tier1FailDataRecoveryTest extends ThreadPooledTestSuite {
    protected static final Duration TIMEOUT = Duration.ofMillis(60000 * 1000);

    private static final int CONTAINER_ID = 0;
    private static final int WRITE_COUNT = 500;
    private static final int MAX_WRITE_ATTEMPTS = 3;
    private static final int MAX_LEDGER_SIZE = 200 * Math.max(10, WRITE_COUNT / 20);

    private static final String APPEND_FORMAT = "Segment_%s_Append_%d";
    private static final long DEFAULT_ROLLING_SIZE = (int) (APPEND_FORMAT.length() * 1.5);
    private final int containerCount = 1;

    private ServiceBuilder serviceBuilder = null;
    private PravegaConnectionListener server = null;
    private ControllerWrapper controllerWrapper = null;
    private Controller controller = null;
    private AutoScaleMonitor monitor = null;

    private ScheduledExecutorService executorService = createExecutorService(100);
    private InMemoryDurableDataLogFactory durableDataLogFactory;
    private File baseDir;
    private Storage storage = null;
    private int servicePort;
    private FileSystemStorageFactory storageFactory;
    private BookKeeperLogFactory dataLogFactory;
    private StreamSegmentStoreWrapper streamSegmentStoreWrapper;
    private TableStoreWrapper tableStoreWrapper;

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
        private final AtomicBoolean secureBk = new AtomicBoolean();
        private final int bookieCount = 1;
        private AtomicReference<BookKeeperConfig> bkConfig = new AtomicReference<>();
        private AtomicReference<CuratorFramework> zkClient = new AtomicReference<>();
        private BookKeeperServiceRunner bookKeeperServiceRunner;
        private String namespace;
        private AtomicReference<BookKeeperServiceRunner> bkService = new AtomicReference<>();
        private AtomicInteger bkPort = new AtomicInteger();

        BKZK() throws Exception {
            secureBk.set(false);
            bkPort.set(TestUtils.getAvailableListenPort());
            val bookiePorts = new ArrayList<Integer>();
            for (int i = 0; i < bookieCount; i++) {
                bookiePorts.add(TestUtils.getAvailableListenPort());
            }

            this.bookKeeperServiceRunner = getBookKeeperServiceRunner(bookiePorts);

            this.bookKeeperServiceRunner.startAll();
            bkService.set(this.bookKeeperServiceRunner);
            setZkClient();

            this.zkClient.get().start();

            setBkConfig();
        }

        public void setZkClient() {
            // Create a ZKClient with a unique namespace.
            this.namespace = "pravega/segmentstore/unittest_" + Long.toHexString(System.nanoTime());
            this.zkClient.set(CuratorFrameworkFactory
                    .builder()
                    .connectString("localhost:" + bkPort.get())
                    .namespace(this.namespace)
                    .retryPolicy(new ExponentialBackoffRetry(1000, 5))
                    .build());
        }

        public void setBkConfig() {
            // Setup config to use the port and namespace.
            this.bkConfig.set(BookKeeperConfig
                    .builder()
                    .with(BookKeeperConfig.ZK_ADDRESS, "localhost:" + bkPort.get())
                    .with(BookKeeperConfig.MAX_WRITE_ATTEMPTS, MAX_WRITE_ATTEMPTS)
                    .with(BookKeeperConfig.BK_LEDGER_MAX_SIZE, MAX_LEDGER_SIZE)
                    .with(BookKeeperConfig.ZK_METADATA_PATH, namespace)
                    .with(BookKeeperConfig.BK_LEDGER_PATH, "/pravega/bookkeeper/ledgers")
                    .with(BookKeeperConfig.BK_ENSEMBLE_SIZE, bookieCount)
                    .with(BookKeeperConfig.BK_WRITE_QUORUM_SIZE, bookieCount)
                    .with(BookKeeperConfig.BK_ACK_QUORUM_SIZE, bookieCount)
                    .with(BookKeeperConfig.BK_TLS_ENABLED, isSecure())
                    .with(BookKeeperConfig.BK_WRITE_TIMEOUT, 1000) // This is the minimum we can set anyway.
                    .build());
        }

        public BookKeeperServiceRunner getBookKeeperServiceRunner(List<Integer> bookiePorts) {
            return BookKeeperServiceRunner.builder()
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
        }

        public boolean isSecure() {
            return secureBk.get();
        }

        @Override
        public void close() throws Exception {
            val process = bkService.getAndSet(null);
            if (process != null) {
                process.close();
            }

            val zkClient = this.zkClient.getAndSet(null);
            if (zkClient != null) {
                zkClient.close();
            }
            bookKeeperServiceRunner.close();
            bkService.getAndSet(null).close();
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

        private DurableLogConfig durableLogConfig = DurableLogConfig
                .builder()
                .with(DurableLogConfig.CHECKPOINT_MIN_COMMIT_COUNT, 10)
                .with(DurableLogConfig.CHECKPOINT_COMMIT_COUNT, 100)
                .with(DurableLogConfig.CHECKPOINT_TOTAL_COMMIT_LENGTH, 10 * 1024 * 1024L)
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
        this.streamSegmentStoreWrapper = new StreamSegmentStoreWrapper(serviceBuilder.createStreamSegmentService());
        this.monitor = new AutoScaleMonitor(this.streamSegmentStoreWrapper, AutoScalerConfig.builder().build());
        this.tableStoreWrapper = new TableStoreWrapper(serviceBuilder.createTableStoreService());

        this.server = new PravegaConnectionListener(false, false, "localhost", servicePort, streamSegmentStoreWrapper,
                this.tableStoreWrapper, this.monitor.getStatsRecorder(), monitor.getTableSegmentStatsRecorder(), new PassingTokenVerifier(),
                null, null, true, this.serviceBuilder.getLowPriorityExecutor());
        this.server.startListening();
    }

    void startController(int bkPort) throws InterruptedException {
        int controllerPort = TestUtils.getAvailableListenPort();
        String serviceHost = "localhost";

        this.controllerWrapper = new ControllerWrapper("localhost:" + bkPort, false,
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
        startController(bkzk.bkPort.get());

        this.controller.close(); // Shuts down controller
        this.controllerWrapper.close();

        HashSet<String> allSegments = new HashSet<>(this.streamSegmentStoreWrapper.getSegments());
        allSegments.addAll(this.tableStoreWrapper.getSegments());
        log.info("No. of segments = {}", allSegments.size());

        Storage tier2 = new AsyncStorageWrapper(new RollingStorage(this.storageFactory.createSyncStorage(),
                new SegmentRollingPolicy(DEFAULT_ROLLING_SIZE)), DataRecoveryTestUtils.createExecutorService(1));

        waitForSegmentsInStorage(allSegments, this.streamSegmentStoreWrapper, tier2)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        this.server.close();
        serviceBuilder.close(); // Shutdown SS
        log.info("SS Shutdown");
        bkzk.bookKeeperServiceRunner.close(); // Shuts down BK & ZK
        bkzk.bkService.getAndSet(null).close();
        log.info("BK & ZK shutdown");

        bkzk = setUpNewBK();
        log.info("BK & ZK started again");

        this.dataLogFactory = new BookKeeperLogFactory(bkzk.bkConfig.get(), bkzk.zkClient.get(), executorService);
        this.dataLogFactory.initialize();

        DebugTool debugTool = createDebugTool(this.dataLogFactory, this.storageFactory);

        log.info("List segments");

        deleteSegment("_system/containers/metadata_0", tier2);
        deleteSegment("_system/containers/metadata_0$attributes.index", tier2);

        Map<Integer, List<SegmentProperties>> segmentsToCreate = DataRecoveryTestUtils.listAllSegments(tier2, containerCount);

        log.info("Start DebugStreamSegmentContainer");
        DebugStreamSegmentContainer debugStreamSegmentContainer = (DebugStreamSegmentContainer) debugTool.containerFactory.createDebugStreamSegmentContainer(CONTAINER_ID);
        debugStreamSegmentContainer.startAsync().awaitRunning();
        DataRecoveryTestUtils.createAllSegments(debugStreamSegmentContainer, segmentsToCreate.get(CONTAINER_ID));
        sleep(20000);
        debugStreamSegmentContainer.stopAsync().awaitTerminated();
        this.dataLogFactory.close();
        sleep(5000);
        startSegmentStore(this.storageFactory, this.dataLogFactory);
        startController(bkzk.bkPort.get());
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

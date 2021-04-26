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
package io.pravega.cli.admin.dataRecovery;

import io.pravega.cli.admin.AdminCommandState;
import io.pravega.cli.admin.utils.TestUtils;
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
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.common.io.FileHelpers;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.segmentstore.server.writer.WriterConfig;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperServiceRunner;
import io.pravega.storage.filesystem.FileSystemStorageConfig;
import io.pravega.storage.filesystem.FileSystemStorageFactory;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import io.pravega.test.integration.demo.ControllerWrapper;
import lombok.Cleanup;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests Data recovery commands.
 */
@Slf4j
public class DataRecoveryTest extends ThreadPooledTestSuite {
    private static final Duration TIMEOUT = Duration.ofMillis(30 * 1000);
    private static final int NUM_EVENTS = 10;
    private static final String EVENT = "12345";
    private static final String SCOPE = "testScope";
    // Setup utility.
    private static final Duration READ_TIMEOUT = Duration.ofMillis(1000);
    private static final AtomicReference<AdminCommandState> STATE = new AtomicReference<>();

    @Rule
    public final Timeout globalTimeout = new Timeout(120, TimeUnit.SECONDS);

    private final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(1);
    private final StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(scalingPolicy).build();

    /**
     * A directory for FILESYSTEM storage as LTS.
     */
    private File baseDir = null;
    private FileSystemStorageConfig adapterConfig;
    private StorageFactory storageFactory = null;

    /**
     * A directory for storing logs and CSV files generated during the test..
     */
    private File logsDir = null;
    private BookKeeperLogFactory factory = null;

    @Override
    protected int getThreadPoolSize() {
        return 10;
    }

    @Before
    public void setUp() throws Exception {
        this.baseDir = Files.createTempDirectory("TestDataRecovery").toFile().getAbsoluteFile();
        this.logsDir = Files.createTempDirectory("DataRecovery").toFile().getAbsoluteFile();
        this.adapterConfig = FileSystemStorageConfig.builder()
                .with(FileSystemStorageConfig.ROOT, this.baseDir.getAbsolutePath())
                .with(FileSystemStorageConfig.REPLACE_ENABLED, true)
                .build();

        this.storageFactory = new FileSystemStorageFactory(adapterConfig, executorService());
    }

    @After
    public void tearDown() {
        STATE.get().close();
        if (this.factory != null) {
            this.factory.close();
        }
        FileHelpers.deleteFileOrDirectory(this.baseDir);
        FileHelpers.deleteFileOrDirectory(this.logsDir);
    }

    /**
     * Tests DurableLog recovery command.
     * @throws Exception    In case of any exception thrown while execution.
     */
    @Test
    public void testDataRecoveryCommand() throws Exception {
        int instanceId = 0;
        int bookieCount = 3;
        int containerCount = 1;
        @Cleanup
        PravegaRunner pravegaRunner = new PravegaRunner(instanceId++, bookieCount, containerCount, this.storageFactory);
        String streamName = "testDataRecoveryCommand";

        createScopeStream(pravegaRunner.getControllerRunner().getController(), SCOPE, streamName);
        try (val clientRunner = new ClientRunner(pravegaRunner.getControllerRunner())) {
            // Write events to the streams.
            writeEvents(streamName, clientRunner.getClientFactory());
        }
        pravegaRunner.shutDownControllerRunner(); // Shut down the controller

        // Flush all Tier 1 to LTS
        ServiceBuilder.ComponentSetup componentSetup = new ServiceBuilder.ComponentSetup(pravegaRunner.getSegmentStoreRunner().getServiceBuilder());
        for (int containerId = 0; containerId < containerCount; containerId++) {
            componentSetup.getContainerRegistry().getContainer(containerId).flushToStorage(TIMEOUT).join();
        }

        pravegaRunner.shutDownSegmentStoreRunner(); // Shutdown SegmentStore
        pravegaRunner.shutDownBookKeeperRunner(); // Shutdown BookKeeper & ZooKeeper

        // start a new BookKeeper and ZooKeeper.
        pravegaRunner.startBookKeeperRunner(instanceId++);

        // set pravega properties for the test
        STATE.set(new AdminCommandState());
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("pravegaservice.container.count", "1");
        pravegaProperties.setProperty("pravegaservice.storage.impl.name", "FILESYSTEM");
        pravegaProperties.setProperty("pravegaservice.storage.layout", "ROLLING_STORAGE");
        pravegaProperties.setProperty("filesystem.root", this.baseDir.getAbsolutePath());
        pravegaProperties.setProperty("pravegaservice.zk.connect.uri", "localhost:" + pravegaRunner.getBookKeeperRunner().getBkPort());
        pravegaProperties.setProperty("bookkeeper.ledger.path", pravegaRunner.getBookKeeperRunner().getLedgerPath());
        pravegaProperties.setProperty("bookkeeper.zk.metadata.path", pravegaRunner.getBookKeeperRunner().getLogMetaNamespace());
        pravegaProperties.setProperty("pravegaservice.clusterName", pravegaRunner.getBookKeeperRunner().getBaseNamespace());
        STATE.get().getConfigBuilder().include(pravegaProperties);

        // Command under test
        TestUtils.executeCommand("storage durableLog-recovery", STATE.get());

        // Start a new segment store and controller
        this.factory = new BookKeeperLogFactory(pravegaRunner.getBookKeeperRunner().getBkConfig().get(), pravegaRunner.getBookKeeperRunner().getZkClient().get(),
                executorService());
        pravegaRunner.restartControllerAndSegmentStore(this.storageFactory, this.factory);
        log.info("Started a controller and segment store.");
        // Create the client with new controller.
        try (val clientRunner = new ClientRunner(pravegaRunner.getControllerRunner())) {
            // Try reading all events to verify that the recovery was successful.
            readAllEvents(streamName, clientRunner.getClientFactory(), clientRunner.getReaderGroupManager(), "RG", "R");
            log.info("Read all events again to verify that segments were recovered.");
        }
        Assert.assertNotNull(StorageListSegmentsCommand.descriptor());
    }

    /**
     * Tests list segments command.
     * @throws Exception    In case of any exception thrown while execution.
     */
    @Test
    public void testListSegmentsCommand() throws Exception {
        int instanceId = 0;
        int bookieCount = 3;
        int containerCount = 1;
        @Cleanup
        PravegaRunner pravegaRunner = new PravegaRunner(instanceId++, bookieCount, containerCount, this.storageFactory);
        String streamName = "testListSegmentsCommand";

        createScopeStream(pravegaRunner.getControllerRunner().getController(), SCOPE, streamName);
        try (val clientRunner = new ClientRunner(pravegaRunner.getControllerRunner())) {
            // Write events to the streams.
            writeEvents(streamName, clientRunner.getClientFactory());
        }
        pravegaRunner.shutDownControllerRunner(); // Shut down the controller

        // Flush all Tier 1 to LTS
        ServiceBuilder.ComponentSetup componentSetup = new ServiceBuilder.ComponentSetup(pravegaRunner.getSegmentStoreRunner().getServiceBuilder());
        for (int containerId = 0; containerId < containerCount; containerId++) {
            componentSetup.getContainerRegistry().getContainer(containerId).flushToStorage(TIMEOUT).join();
        }

        pravegaRunner.shutDownSegmentStoreRunner(); // Shutdown SegmentStore
        pravegaRunner.shutDownBookKeeperRunner(); // Shutdown BookKeeper & ZooKeeper

        // set pravega properties for the test
        STATE.set(new AdminCommandState());
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("pravegaservice.container.count", "1");
        pravegaProperties.setProperty("pravegaservice.storage.impl.name", "FILESYSTEM");
        pravegaProperties.setProperty("pravegaservice.storage.layout", "ROLLING_STORAGE");
        pravegaProperties.setProperty("filesystem.root", this.baseDir.getAbsolutePath());
        STATE.get().getConfigBuilder().include(pravegaProperties);

        // Execute the command for list segments
        TestUtils.executeCommand("storage list-segments " + this.logsDir.getAbsolutePath(), STATE.get());
        // There should be a csv file created for storing segments in Container 0
        Assert.assertTrue(new File(this.logsDir.getAbsolutePath(), "Container_0.csv").exists());
        // Check if the file has segments listed in it
        Path path = Paths.get(this.logsDir.getAbsolutePath() + "/Container_0.csv");
        long lines = Files.lines(path).count();
        AssertExtensions.assertGreaterThan("There should be at least one segment.", 1, lines);
        Assert.assertNotNull(StorageListSegmentsCommand.descriptor());
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

    // write events to the given stream
    private void writeEvents(String streamName, ClientFactoryImpl clientFactory) {
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName,
                new UTF8StringSerializer(),
                EventWriterConfig.builder().build());
        for (int i = 0; i < NUM_EVENTS;) {
            writer.writeEvent("", EVENT).join();
            i++;
        }
        writer.flush();
        writer.close();
    }

    // read all events from the given stream
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

        for (int q = 0; q < NUM_EVENTS;) {
            String eventRead = reader.readNextEvent(READ_TIMEOUT.toMillis()).getEvent();
            Assert.assertEquals("Event written and read back don't match", EVENT, eventRead);
            q++;
        }
        reader.close();
    }

    /**
     * Sets up a new BookKeeper & ZooKeeper.
     */
    private static class BookKeeperRunner implements AutoCloseable {
        @Getter
        private final int bkPort;
        private final BookKeeperServiceRunner bookKeeperServiceRunner;
        @Getter
        private final AtomicReference<BookKeeperConfig> bkConfig = new AtomicReference<>();
        @Getter
        private final AtomicReference<CuratorFramework> zkClient = new AtomicReference<>();
        private final AtomicReference<BookKeeperServiceRunner> bkService = new AtomicReference<>();
        @Getter
        private final String ledgerPath;
        @Getter
        private final String logMetaNamespace;
        @Getter
        private final String baseNamespace;
        BookKeeperRunner(int instanceId, int bookieCount) throws Exception {
            this.ledgerPath = "/pravega/bookkeeper/ledgers" + instanceId;
            this.bkPort = io.pravega.test.common.TestUtils.getAvailableListenPort();
            val bookiePorts = new ArrayList<Integer>();
            for (int i = 0; i < bookieCount; i++) {
                bookiePorts.add(io.pravega.test.common.TestUtils.getAvailableListenPort());
            }
            this.bookKeeperServiceRunner = BookKeeperServiceRunner.builder()
                    .startZk(true)
                    .zkPort(bkPort)
                    .ledgersPath(ledgerPath)
                    .bookiePorts(bookiePorts)
                    .build();
            try {
                this.bookKeeperServiceRunner.startAll();
            } catch (Exception e) {
                log.error("Exception occurred while starting bookKeeper service.", e);
                this.close();
                throw e;
            }
            this.bkService.set(this.bookKeeperServiceRunner);

            // Create a ZKClient with a unique namespace.
            this.baseNamespace = "pravega" + instanceId;
            this.zkClient.set(CuratorFrameworkFactory
                    .builder()
                    .connectString("localhost:" + bkPort)
                    .namespace(baseNamespace)
                    .retryPolicy(new ExponentialBackoffRetry(1000, 10))
                    .build());

            this.zkClient.get().start();

            logMetaNamespace = "segmentstore/containers" + instanceId;
            this.bkConfig.set(BookKeeperConfig
                    .builder()
                    .with(BookKeeperConfig.ZK_ADDRESS, "localhost:" + bkPort)
                    .with(BookKeeperConfig.ZK_METADATA_PATH, logMetaNamespace)
                    .with(BookKeeperConfig.BK_LEDGER_PATH, ledgerPath)
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
        private final int servicePort = io.pravega.test.common.TestUtils.getAvailableListenPort();
        @Getter
        private final ServiceBuilder serviceBuilder;
        private final PravegaConnectionListener server;
        private final StreamSegmentStore streamSegmentStore;
        private final TableStore tableStore;

        SegmentStoreRunner(StorageFactory storageFactory, BookKeeperLogFactory dataLogFactory, int containerCount)
                throws DurableDataLogException {
            ServiceBuilderConfig.Builder configBuilder = ServiceBuilderConfig
                    .builder()
                    .include(ServiceConfig.builder()
                            .with(ServiceConfig.CONTAINER_COUNT, containerCount))
                    .include(WriterConfig.builder()
                            .with(WriterConfig.MIN_READ_TIMEOUT_MILLIS, 100L)
                            .with(WriterConfig.MAX_READ_TIMEOUT_MILLIS, 500L)
                    );
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
                    this.serviceBuilder.getLowPriorityExecutor());
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
        private final int controllerPort = io.pravega.test.common.TestUtils.getAvailableListenPort();
        private final String serviceHost = "localhost";
        private final ControllerWrapper controllerWrapper;
        @Getter
        private final Controller controller;
        private final URI controllerURI = URI.create("tcp://" + serviceHost + ":" + controllerPort);

        ControllerRunner(int bkPort, int servicePort, int containerCount) throws InterruptedException {
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
        @Getter
        private final ClientFactoryImpl clientFactory;
        @Getter
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
        private final int bookieCount;
        @Getter
        private BookKeeperRunner bookKeeperRunner;
        @Getter
        private SegmentStoreRunner segmentStoreRunner;
        @Getter
        private ControllerRunner controllerRunner;

        PravegaRunner(int instanceId, int bookieCount, int containerCount, StorageFactory storageFactory) throws Exception {
            this.containerCount = containerCount;
            this.bookieCount = bookieCount;
            startBookKeeperRunner(instanceId);
            restartControllerAndSegmentStore(storageFactory, null);
        }

        private void restartControllerAndSegmentStore(StorageFactory storageFactory, BookKeeperLogFactory dataLogFactory)
                throws DurableDataLogException, InterruptedException {
            this.segmentStoreRunner = new SegmentStoreRunner(storageFactory, dataLogFactory, this.containerCount);
            log.info("bk port to be connected = {}", this.bookKeeperRunner.bkPort);
            this.controllerRunner = new ControllerRunner(this.bookKeeperRunner.bkPort, this.segmentStoreRunner.servicePort, containerCount);
        }

        private void shutDownControllerRunner() throws Exception {
            this.controllerRunner.close();
        }

        private void shutDownSegmentStoreRunner() {
            this.segmentStoreRunner.close();
        }

        private void shutDownBookKeeperRunner() throws Exception {
            this.bookKeeperRunner.close();
        }

        private void startBookKeeperRunner(int instanceId) throws Exception {
            this.bookKeeperRunner = new BookKeeperRunner(instanceId, this.bookieCount);
        }

        @Override
        public void close() throws Exception {
            shutDownControllerRunner();
            shutDownSegmentStoreRunner();
            shutDownBookKeeperRunner();
        }
    }
}

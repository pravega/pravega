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
package io.pravega.test.integration.utils;

import io.pravega.client.control.impl.Controller;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.delegationtoken.PassingTokenVerifier;
import io.pravega.segmentstore.server.host.handler.AdminConnectionListener;
import io.pravega.segmentstore.server.host.handler.IndexAppendProcessor;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.segmentstore.server.writer.WriterConfig;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.StorageLayoutType;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperServiceRunner;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.net.URI;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static io.pravega.segmentstore.server.store.ServiceConfig.TLS_PROTOCOL_VERSION;

/**
 * Encapsulate utility methods to run Pravega server side services in process.
 */
@Slf4j
public class LocalServiceStarter {

    /**
     * Sets up a new BookKeeper &amp; ZooKeeper.
     */
    public static class BookKeeperRunner implements AutoCloseable {
        @Getter
        private final AtomicReference<CuratorFramework> zkClient = new AtomicReference<>();
        @Getter
        private final int bkPort;
        private final BookKeeperServiceRunner bookKeeperServiceRunner;
        @Getter
        private final AtomicReference<BookKeeperConfig> bkConfig = new AtomicReference<>();
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

            this.logMetaNamespace = "segmentstore/containers" + instanceId;
            this.bkConfig.set(BookKeeperConfig
                    .builder()
                    .with(BookKeeperConfig.BK_ENSEMBLE_SIZE, bookieCount)
                    .with(BookKeeperConfig.BK_WRITE_QUORUM_SIZE, bookieCount)
                    .with(BookKeeperConfig.BK_ACK_QUORUM_SIZE, bookieCount)
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
    public static class SegmentStoreRunner implements AutoCloseable {
        @Getter
        private final int servicePort = io.pravega.test.common.TestUtils.getAvailableListenPort();
        @Getter
        private final int adminPort = io.pravega.test.common.TestUtils.getAvailableListenPort();
        @Getter
        private ServiceBuilder serviceBuilder;
        private final PravegaConnectionListener server;

        private AdminConnectionListener adminServer = null;

        SegmentStoreRunner(StorageFactory storageFactory, BookKeeperLogFactory dataLogFactory, int containerCount, boolean enableAdminGateway)
                throws DurableDataLogException {
            ServiceBuilderConfig config = ServiceBuilderConfig
                    .builder()
                    .include(ServiceConfig.builder()
                            .with(ServiceConfig.CONTAINER_COUNT, containerCount)
                            .with(ServiceConfig.STORAGE_LAYOUT, StorageLayoutType.CHUNKED_STORAGE)
                            .with(ServiceConfig.STORAGE_IMPLEMENTATION, ServiceConfig.StorageType.FILESYSTEM.name())
                            .with(ServiceConfig.LISTENING_IP_ADDRESS, "localhost"))
                    .include(WriterConfig.builder()
                            .with(WriterConfig.MIN_READ_TIMEOUT_MILLIS, 100L)
                            .with(WriterConfig.MAX_READ_TIMEOUT_MILLIS, 500L)
                    ).build();

            // Components that are required for all types of SegmentStore.
            this.serviceBuilder = ServiceBuilder.newInMemoryBuilder(config);
            if (dataLogFactory != null) {
                this.serviceBuilder = this.serviceBuilder.withDataLogFactory(setup -> dataLogFactory);
            }
            if (storageFactory != null) {
                this.serviceBuilder = this.serviceBuilder.withStorageFactory(setup -> storageFactory);
            }
            this.serviceBuilder.initialize();
            StreamSegmentStore streamSegmentStore = this.serviceBuilder.createStreamSegmentService();
            TableStore tableStore = this.serviceBuilder.createTableStoreService();
            IndexAppendProcessor indexAppendProcessor = new IndexAppendProcessor(this.serviceBuilder.getLowPriorityExecutor(), streamSegmentStore);
            this.server = new PravegaConnectionListener(false, servicePort, streamSegmentStore, tableStore,
                    this.serviceBuilder.getLowPriorityExecutor(), indexAppendProcessor);
            this.server.startListening();
            if (enableAdminGateway) {
                this.adminServer = new AdminConnectionListener(false, false, "localhost", adminPort, streamSegmentStore,
                        tableStore, new PassingTokenVerifier(), null, null, TLS_PROTOCOL_VERSION.getDefaultValue().split(","),
                        indexAppendProcessor);
                this.adminServer.startListening();
            }
        }

        @Override
        public void close() {
            this.server.close();
            if (adminServer != null) {
                this.adminServer.close();
            }
            this.serviceBuilder.close();
        }
    }

    /**
     * Creates a controller instance and runs it.
     */
    public static class ControllerRunner implements AutoCloseable {
        private final int controllerPort = io.pravega.test.common.TestUtils.getAvailableListenPort();
        private final String serviceHost = "localhost";
        private final ControllerWrapper controllerWrapper;
        @Getter
        private final Controller controller;
        @Getter
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
     * Creates a Pravega instance.
     */
    public static class PravegaRunner implements AutoCloseable {
        private final int containerCount;
        private final int bookieCount;
        @Getter
        private BookKeeperRunner bookKeeperRunner;
        @Getter
        private SegmentStoreRunner segmentStoreRunner;
        @Getter
        private ControllerRunner controllerRunner;

        public PravegaRunner(int bookieCount, int containerCount) {
            this.containerCount = containerCount;
            this.bookieCount = bookieCount;
        }

        public void startBookKeeperRunner(int instanceId) throws Exception {
            this.bookKeeperRunner = new BookKeeperRunner(instanceId, this.bookieCount);
        }

        public void startControllerAndSegmentStore(StorageFactory storageFactory, BookKeeperLogFactory dataLogFactory)
                throws DurableDataLogException {
            startControllerAndSegmentStore(storageFactory, dataLogFactory, false);
        }

        public void startControllerAndSegmentStore(StorageFactory storageFactory, BookKeeperLogFactory dataLogFactory, boolean enableAdminGateway)
                throws DurableDataLogException {
            this.segmentStoreRunner = new SegmentStoreRunner(storageFactory, dataLogFactory, this.containerCount, enableAdminGateway);
            log.info("bk port to be connected = {}", this.bookKeeperRunner.bkPort);
            this.controllerRunner = new ControllerRunner(this.bookKeeperRunner.bkPort, this.segmentStoreRunner.servicePort, containerCount);
        }

        public void shutDownControllerRunner() throws Exception {
            this.controllerRunner.close();
        }

        public void shutDownSegmentStoreRunner() {
            this.segmentStoreRunner.close();
        }

        public void shutDownBookKeeperRunner() throws Exception {
            this.bookKeeperRunner.close();
        }

        @Override
        public void close() throws Exception {
            shutDownControllerRunner();
            shutDownSegmentStoreRunner();
            shutDownBookKeeperRunner();
        }
    }
}

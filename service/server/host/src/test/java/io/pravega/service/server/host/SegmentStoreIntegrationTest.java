/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.service.server.host;

import io.pravega.common.io.FileHelpers;
import io.pravega.service.server.store.ServiceBuilder;
import io.pravega.service.server.store.ServiceBuilderConfig;
import io.pravega.service.server.store.StreamSegmentStoreTestBase;
import io.pravega.service.storage.Storage;
import io.pravega.service.storage.StorageFactory;
import io.pravega.service.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.service.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.service.storage.impl.bookkeeper.BookKeeperServiceRunner;
import io.pravega.service.storage.impl.hdfs.HDFSClusterHelpers;
import io.pravega.service.storage.impl.hdfs.HDFSStorageConfig;
import io.pravega.service.storage.impl.hdfs.HDFSStorageFactory;
import io.pravega.service.storage.impl.rocksdb.RocksDBCacheFactory;
import io.pravega.service.storage.impl.rocksdb.RocksDBConfig;
import io.pravega.test.common.TestUtils;
import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReference;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;

/**
 * End-to-end tests for SegmentStore, with integrated Storage and DurableDataLog.
 */
public class SegmentStoreIntegrationTest extends StreamSegmentStoreTestBase {
    //region Test Configuration and Setup

    private static final int BOOKIE_COUNT = 3;
    private File baseDir = null;
    private MiniDFSCluster hdfsCluster = null;
    private BookKeeperServiceRunner bkRunner;
    private CuratorFramework zkClient;

    /**
     * Starts BookKeeper and HDFS MiniCluster.
     */
    @Before
    public void setUp() throws Exception {
        // BookKeeper
        // Pick random ports to reduce chances of collisions during concurrent test executions.
        int zkPort = TestUtils.getAvailableListenPort();
        val bookiePorts = new ArrayList<Integer>();
        for (int i = 0; i < BOOKIE_COUNT; i++) {
            bookiePorts.add(TestUtils.getAvailableListenPort());
        }

        this.bkRunner = BookKeeperServiceRunner.builder()
                                               .startZk(true)
                                               .zkPort(zkPort)
                                               .bookiePorts(bookiePorts)
                                               .build();
        this.bkRunner.start();

        // Create a ZKClient with a base namespace.
        String baseNamespace = "pravega/" + Long.toHexString(System.nanoTime());
        this.zkClient = CuratorFrameworkFactory
                .builder()
                .connectString("localhost:" + zkPort)
                .namespace(baseNamespace)
                .retryPolicy(new ExponentialBackoffRetry(1000, 5))
                .build();
        this.zkClient.start();

        // Attach a sub-namespace for the Container Metadata.
        String logMetaNamespace = "segmentstore/containers";
        this.configBuilder.include(BookKeeperConfig
                .builder()
                .with(BookKeeperConfig.ZK_ADDRESS, "localhost:" + zkPort)
                .with(BookKeeperConfig.ZK_METADATA_PATH, logMetaNamespace)
                .with(BookKeeperConfig.BK_LEDGER_PATH, "/ledgers"));

        // HDFS
        this.baseDir = Files.createTempDirectory("test_hdfs").toFile().getAbsoluteFile();
        this.hdfsCluster = HDFSClusterHelpers.createMiniDFSCluster(this.baseDir.getAbsolutePath());

        this.configBuilder.include(HDFSStorageConfig
                .builder()
                .with(HDFSStorageConfig.REPLICATION, 1)
                .with(HDFSStorageConfig.URL, String.format("hdfs://localhost:%d/", hdfsCluster.getNameNodePort())));
    }

    /**
     * Shuts down BookKeeper and HDFS MiniCluster.
     */
    @After
    public void tearDown() throws Exception {
        // BookKeeper
        val bk = this.bkRunner;
        if (bk != null) {
            bk.close();
            this.bkRunner = null;
        }

        val zk = this.zkClient;
        if (zk != null) {
            zk.close();
            this.zkClient = null;
        }

        // HDFS
        val hdfs = this.hdfsCluster;
        if (hdfs != null) {
            hdfs.shutdown();
            this.hdfsCluster = null;
            FileHelpers.deleteFileOrDirectory(this.baseDir);
            this.baseDir = null;
        }
    }

    //endregion

    //region StreamSegmentStoreTestBase Implementation

    @Override
    protected synchronized ServiceBuilder createBuilder(ServiceBuilderConfig builderConfig, AtomicReference<Storage> storage) {
        return ServiceBuilder
                .newInMemoryBuilder(builderConfig)
                .withCacheFactory(setup -> new RocksDBCacheFactory(builderConfig.getConfig(RocksDBConfig::builder)))
                .withStorageFactory(setup -> {
                    StorageFactory f = new HDFSStorageFactory(setup.getConfig(HDFSStorageConfig::builder), setup.getExecutor());
                    return new ListenableStorageFactory(f, storage::set);
                })
                .withDataLogFactory(setup -> new BookKeeperLogFactory(setup.getConfig(BookKeeperConfig::builder), this.zkClient, setup.getExecutor()));
    }

    //endregion
}

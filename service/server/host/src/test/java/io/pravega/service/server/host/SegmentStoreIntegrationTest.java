/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
import io.pravega.testcommon.TestUtils;
import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReference;
import lombok.val;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;

/**
 * End-to-end tests for SegmentStore, with integrated Storage and DurableDataLog.
 */
public class SegmentStoreIntegrationTest extends StreamSegmentStoreTestBase {
    //region Test Configuration and Setup

    private static final String BK_NAMESPACE = "/pravega/segmentstore/e2etest_" + Long.toHexString(System.nanoTime());
    private static final int BOOKIE_COUNT = 3;

    private File baseDir = null;
    private MiniDFSCluster hdfsCluster = null;
    private BookKeeperServiceRunner bkRunner;

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

        this.configBuilder.include(BookKeeperConfig
                .builder()
                .with(BookKeeperConfig.ZK_ADDRESS, "localhost:" + zkPort)
                .with(BookKeeperConfig.ZK_NAMESPACE, BK_NAMESPACE));

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
                .withDataLogFactory(setup -> new BookKeeperLogFactory(setup.getConfig(BookKeeperConfig::builder), setup.getExecutor()));
    }

    //endregion
}

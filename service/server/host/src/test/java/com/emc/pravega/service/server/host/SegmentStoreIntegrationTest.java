/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.server.host;

import com.emc.pravega.common.io.FileHelpers;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.service.server.store.StreamSegmentStoreTestBase;
import com.emc.pravega.service.storage.DurableDataLogException;
import com.emc.pravega.service.storage.DurableDataLogFactory;
import com.emc.pravega.service.storage.Storage;
import com.emc.pravega.service.storage.StorageFactory;
import com.emc.pravega.service.storage.impl.distributedlog.DistributedLogConfig;
import com.emc.pravega.service.storage.impl.distributedlog.DistributedLogDataLogFactory;
import com.emc.pravega.service.storage.impl.distributedlog.DistributedLogStarter;
import com.emc.pravega.service.storage.impl.hdfs.HDFSClusterHelpers;
import com.emc.pravega.service.storage.impl.hdfs.HDFSStorageConfig;
import com.emc.pravega.service.storage.impl.hdfs.HDFSStorageFactory;
import com.emc.pravega.service.storage.impl.rocksdb.RocksDBCacheFactory;
import com.emc.pravega.service.storage.impl.rocksdb.RocksDBConfig;
import com.emc.pravega.testcommon.TestUtils;
import java.io.File;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicReference;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;

/**
 * End-to-end tests for SegmentStore, with integrated Storage and DurableDataLog.
 */
public class SegmentStoreIntegrationTest extends StreamSegmentStoreTestBase {
    //region Test Configuration and Setup

    private static final String DLOG_NAMESPACE = "pravegae2e";

    private File baseDir = null;
    private MiniDFSCluster hdfsCluster = null;
    private Process dlogProcess;

    /**
     * Starts DistributedLog and HDFS MiniCluster.
     */
    @Before
    public void setUp() throws Exception {
        // DistributedLog
        // Pick a random port to reduce chances of collisions during concurrent test executions.
        int dlogPort = TestUtils.getAvailableListenPort();
        this.dlogProcess = DistributedLogStarter.startOutOfProcess(dlogPort);
        DistributedLogStarter.createNamespace(DLOG_NAMESPACE, dlogPort);

        this.configBuilder.include(DistributedLogConfig
                .builder()
                .with(DistributedLogConfig.HOSTNAME, DistributedLogStarter.DLOG_HOST)
                .with(DistributedLogConfig.PORT, dlogPort)
                .with(DistributedLogConfig.NAMESPACE, DLOG_NAMESPACE));

        // HDFS
        this.baseDir = Files.createTempDirectory("test_hdfs").toFile().getAbsoluteFile();
        this.hdfsCluster = HDFSClusterHelpers.createMiniDFSCluster(this.baseDir.getAbsolutePath());

        this.configBuilder.include(HDFSStorageConfig
                .builder()
                .with(HDFSStorageConfig.REPLICATION, 1)
                .with(HDFSStorageConfig.URL, String.format("hdfs://localhost:%d/", hdfsCluster.getNameNodePort())));
    }

    /**
     * Shuts down DistributedLog and HDFS MiniCluster.
     */
    @After
    public void tearDown() throws Exception {
        // DistributedLog
        val process = this.dlogProcess;
        if (process != null) {
            process.destroy();
            this.dlogProcess = null;
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
                .withDataLogFactory(this::createDistributedLogDataLogFactory);
    }

    @SneakyThrows(DurableDataLogException.class)
    private DurableDataLogFactory createDistributedLogDataLogFactory(ServiceBuilder.ComponentSetup setup) {
        DistributedLogDataLogFactory f = new DistributedLogDataLogFactory("End2End",
                setup.getConfig(DistributedLogConfig::builder), setup.getExecutor());
        f.initialize();
        return f;
    }

    //endregion
}

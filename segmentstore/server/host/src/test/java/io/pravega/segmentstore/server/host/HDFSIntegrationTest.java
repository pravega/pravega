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
package io.pravega.segmentstore.server.host;

import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.storage.hdfs.HDFSClusterHelpers;
import io.pravega.storage.hdfs.HDFSSimpleStorageFactory;
import io.pravega.storage.hdfs.HDFSStorageConfig;
import lombok.val;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;

import java.time.Duration;

/**
 * End-to-end tests for SegmentStore, with integrated Storage and DurableDataLog.
 */
public class HDFSIntegrationTest extends BookKeeperIntegrationTestBase {
    //region Test Configuration and Setup

    private MiniDFSCluster hdfsCluster = null;

    /**
     * Starts BookKeeper and HDFS MiniCluster.
     */
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.hdfsCluster = HDFSClusterHelpers.createMiniDFSCluster(getBaseDir().getAbsolutePath());
        this.configBuilder.include(HDFSStorageConfig
                .builder()
                .with(HDFSStorageConfig.REPLICATION, 1)
                .with(HDFSStorageConfig.URL, String.format("hdfs://localhost:%d/", hdfsCluster.getNameNodePort())));
    }

    /**
     * Shuts down BookKeeper and HDFS MiniCluster.
     */
    @Override
    @After
    public void tearDown() throws Exception {
        val hdfs = this.hdfsCluster;
        if (hdfs != null) {
            hdfs.shutdown();
            this.hdfsCluster = null;
        }

        super.tearDown();
    }

    //endregion

    //region StreamSegmentStoreTestBase Implementation

    @Override
    protected boolean appendAfterMerging() {
        return false; // HDFS is slow enough as it is; adding this would cause the test to take even longer.
    }

    @Override
    protected ServiceBuilder createBuilder(ServiceBuilderConfig.Builder configBuilder, int instanceId, boolean useChunkedSegmentStorage) {
        ServiceBuilderConfig builderConfig = getBuilderConfig(configBuilder, instanceId);
        return ServiceBuilder
                .newInMemoryBuilder(builderConfig)
                .withStorageFactory(setup -> new HDFSSimpleStorageFactory(ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                                .journalSnapshotInfoUpdateFrequency(Duration.ofMillis(10))
                                .maxJournalUpdatesPerSnapshot(5)
                                .garbageCollectionDelay(Duration.ofMillis(10))
                                .garbageCollectionSleep(Duration.ofMillis(10))
                                .selfCheckEnabled(true)
                                .build(),
                                setup.getConfig(HDFSStorageConfig::builder),
                                setup.getStorageExecutor()))
                .withDataLogFactory(setup -> new BookKeeperLogFactory(setup.getConfig(BookKeeperConfig::builder), getBookkeeper().getZkClient(), setup.getCoreExecutor()));
    }
    //endregion
}

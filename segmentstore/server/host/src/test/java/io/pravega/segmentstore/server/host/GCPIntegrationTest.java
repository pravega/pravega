/**
 * Copyright Pravega Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pravega.segmentstore.server.host;

import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.google.common.base.Preconditions;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.storage.SimpleStorageFactory;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import io.pravega.storage.gcp.GCPChunkStorage;
import io.pravega.storage.gcp.GCPStorageConfig;
import lombok.Getter;
import org.junit.After;
import org.junit.Before;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;

/**
 * End-to-end tests for SegmentStore, with integrated GCP Storage and DurableDataLog.
 */

public class GCPIntegrationTest extends BookKeeperIntegrationTestBase {
    //region Test Configuration and Setup

    com.google.cloud.storage.Storage mockGCPStorage;

    @Getter
    private final ChunkedSegmentStorageConfig chunkedSegmentStorageConfig = ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
            .journalSnapshotInfoUpdateFrequency(Duration.ofMillis(10))
            .maxJournalUpdatesPerSnapshot(5)
            .garbageCollectionDelay(Duration.ofMillis(10))
            .garbageCollectionSleep(Duration.ofMillis(10))
            .minSizeLimitForConcat(100)
            .maxSizeLimitForConcat(1000)
            .selfCheckEnabled(true)
            .build();

    /**
     * Starts BookKeeper.
     */

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        String bucketName = "test-bucket";
        String prefix = "Integration" + UUID.randomUUID();
        this.configBuilder.include(GCPStorageConfig.builder()
                .with(GCPStorageConfig.BUCKET, bucketName)
                .with(GCPStorageConfig.PREFIX, prefix));
        mockGCPStorage = LocalStorageHelper.getOptions().getService();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    //endregion

    //region StreamSegmentStoreTestBase Implementation

    @Override
    protected ServiceBuilder createBuilder(ServiceBuilderConfig.Builder configBuilder, int instanceId, boolean useChunkedSegmentStorage) {
        Preconditions.checkState(useChunkedSegmentStorage);
        ServiceBuilderConfig builderConfig = getBuilderConfig(configBuilder, instanceId);
        return ServiceBuilder
                .newInMemoryBuilder(builderConfig)
                .withStorageFactory(setup -> new LocalGCPSimpleStorageFactory(setup.getConfig(GCPStorageConfig::builder), setup.getStorageExecutor()))
                .withDataLogFactory(setup -> new BookKeeperLogFactory(setup.getConfig(BookKeeperConfig::builder),
                        getBookkeeper().getZkClient(), setup.getCoreExecutor()));
    }

    @Override
    public void testFlushToStorage() {
    }

    //endregion

    private class LocalGCPSimpleStorageFactory implements SimpleStorageFactory {
        private final GCPStorageConfig config;

        @Getter
        private final ChunkedSegmentStorageConfig chunkedSegmentStorageConfig = ChunkedSegmentStorageConfig.DEFAULT_CONFIG.toBuilder()
                .journalSnapshotInfoUpdateFrequency(Duration.ofMillis(10))
                .maxJournalUpdatesPerSnapshot(5)
                .garbageCollectionDelay(Duration.ofMillis(10))
                .garbageCollectionSleep(Duration.ofMillis(10))
                .selfCheckEnabled(true)
                .build();

        @Getter
        private final ScheduledExecutorService executor;

        LocalGCPSimpleStorageFactory(GCPStorageConfig config, ScheduledExecutorService executor) {
            this.config = Preconditions.checkNotNull(config, "config");
            this.executor = Preconditions.checkNotNull(executor, "executor");
        }

        @Override
        public Storage createStorageAdapter(int containerId, ChunkMetadataStore metadataStore) {
            return new ChunkedSegmentStorage(containerId,
                    createChunkStorage(),
                    metadataStore,
                    this.executor,
                    this.chunkedSegmentStorageConfig);
        }

        /**
         * Creates a new instance of a Storage adapter.
         */

        @Override
        public Storage createStorageAdapter() {
            throw new UnsupportedOperationException("SimpleStorageFactory requires ChunkMetadataStore");
        }

        @Override
        public ChunkStorage createChunkStorage() {
            return new GCPChunkStorage(mockGCPStorage, this.config, executorService());
        }
    }
}

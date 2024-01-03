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
import io.pravega.storage.azure.AzureStorageConfig;
import io.pravega.storage.azure.AzureClient;
import io.pravega.storage.azure.AzureChunkStorage;
import io.pravega.storage.azure.MockAzureClient;
import lombok.Getter;
import lombok.val;
import org.junit.After;
import org.junit.Before;

import java.net.URI;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;

/**
 * End-to-end tests for SegmentStore, with integrated AZURE Storage and DurableDataLog.
 */
public class AzureIntegrationTest extends BookKeeperIntegrationTestBase {
    //region Test Configuration and Setup

    private String azureEndpoint;

    private AzureClient azureClient;

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
        azureEndpoint = "https://localhost";
        String containerName = "test-container" + UUID.randomUUID();
        String prefix = "Integration" + UUID.randomUUID();
        val builder = AzureStorageConfig.builder()
                .with(AzureStorageConfig.ENDPOINT, "http://127.0.0.1:10000/devstoreaccount1")
                                        .with(AzureStorageConfig.CONNECTION_STRING,
                                              "DefaultEndpointsProtocol=http;" + "AccountName=devstoreaccount1;"
                                            + "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
                                            + "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;")
                                        .with(AzureStorageConfig.CONTAINER, containerName)
                .with(AzureStorageConfig.PREFIX, prefix)
                .with(AzureStorageConfig.CREATE_CONTAINER, true);
        this.configBuilder.include(builder);
        this.azureClient = new MockAzureClient(builder.build());
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
                .withStorageFactory(setup -> new LocalAzureSimpleStorageFactory(setup.getConfig(AzureStorageConfig::builder), setup.getStorageExecutor()))
                .withDataLogFactory(setup -> new BookKeeperLogFactory(setup.getConfig(BookKeeperConfig::builder),
                        getBookkeeper().getZkClient(), setup.getCoreExecutor()));
    }

    /**
     * AzureChunkStorage does not support rolling storage.
     */
    @Override
    public void testFlushToStorage() {
    }

    //endregion

    private class LocalAzureSimpleStorageFactory implements SimpleStorageFactory {
        private final AzureStorageConfig config;

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

        LocalAzureSimpleStorageFactory(AzureStorageConfig config, ScheduledExecutorService executor) {
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
            URI uri = URI.create(azureEndpoint);
            AzureClient azureClient = createAzureClient(this.config);
            return new AzureChunkStorage(azureClient, this.config, this.executor, true);
        }

        private AzureClient createAzureClient(AzureStorageConfig config) {
            return azureClient;
        }
    }
}

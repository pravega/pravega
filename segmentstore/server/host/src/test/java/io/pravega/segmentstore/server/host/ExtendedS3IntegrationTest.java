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

import com.emc.object.s3.S3Config;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.google.common.base.Preconditions;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.SimpleStorageFactory;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import io.pravega.segmentstore.storage.rolling.RollingStorage;
import io.pravega.storage.extendeds3.ExtendedS3ChunkStorage;
import io.pravega.storage.extendeds3.ExtendedS3Storage;
import io.pravega.storage.extendeds3.ExtendedS3StorageConfig;
import io.pravega.storage.extendeds3.S3ClientMock;
import io.pravega.storage.extendeds3.S3Mock;
import io.pravega.test.common.TestUtils;
import java.net.URI;
import java.util.concurrent.ScheduledExecutorService;

import lombok.Getter;
import org.junit.After;
import org.junit.Before;

/**
 * End-to-end tests for SegmentStore, with integrated Extended S3 Storage and DurableDataLog.
 */
public class ExtendedS3IntegrationTest extends BookKeeperIntegrationTestBase {
    //region Test Configuration and Setup

    private String s3ConfigUri;
    private S3Mock s3Mock;

    /**
     * Starts BookKeeper.
     */
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        s3ConfigUri = "http://127.0.0.1:" + TestUtils.getAvailableListenPort() + "?identity=x&secretKey=x";
        s3Mock = new S3Mock();
        this.configBuilder.include(ExtendedS3StorageConfig.builder()
                .with(ExtendedS3StorageConfig.CONFIGURI, s3ConfigUri)
                .with(ExtendedS3StorageConfig.BUCKET, "kanpravegatest"));
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
        ServiceBuilderConfig builderConfig = getBuilderConfig(configBuilder, instanceId);
        return ServiceBuilder
                .newInMemoryBuilder(builderConfig)
                .withStorageFactory(setup -> useChunkedSegmentStorage ?
                        new LocalExtendedS3SimpleStorageFactory(setup.getConfig(ExtendedS3StorageConfig::builder), setup.getStorageExecutor())
                        : new LocalExtendedS3StorageFactory(setup.getConfig(ExtendedS3StorageConfig::builder), setup.getStorageExecutor()))
                .withDataLogFactory(setup -> new BookKeeperLogFactory(setup.getConfig(BookKeeperConfig::builder),
                        getBookkeeper().getZkClient(), setup.getCoreExecutor()));
    }

    private class LocalExtendedS3StorageFactory implements StorageFactory {

        private final ExtendedS3StorageConfig config;
        private final ScheduledExecutorService storageExecutor;

        LocalExtendedS3StorageFactory(ExtendedS3StorageConfig config, ScheduledExecutorService executor) {
            this.config = Preconditions.checkNotNull(config, "config");
            this.storageExecutor = Preconditions.checkNotNull(executor, "executor");
        }

        @Override
        public Storage createStorageAdapter() {
            URI uri = URI.create(s3ConfigUri);
            S3Config s3Config = new S3Config(uri);

            s3Config = s3Config
                    .withRetryEnabled(false)
                    .withInitialRetryDelay(1)
                    .withProperty("com.sun.jersey.client.property.connectTimeout", 100);

            S3ClientMock client = new S3ClientMock(s3Config, s3Mock);
            return getStorage(client);
        }

        protected Storage getStorage(S3JerseyClient client) {
            return new AsyncStorageWrapper(new RollingStorage(new ExtendedS3Storage(client, config, false)), this.storageExecutor);
        }
    }

    private class LocalExtendedS3SimpleStorageFactory implements SimpleStorageFactory {
        private final ExtendedS3StorageConfig config;

        @Getter
        private final ScheduledExecutorService executor;

        LocalExtendedS3SimpleStorageFactory(ExtendedS3StorageConfig config, ScheduledExecutorService executor) {
            this.config = Preconditions.checkNotNull(config, "config");
            this.executor = Preconditions.checkNotNull(executor, "executor");
        }

        @Override
        public Storage createStorageAdapter(int containerId, ChunkMetadataStore metadataStore) {
            URI uri = URI.create(s3ConfigUri);
            S3Config s3Config = new S3Config(uri);

            s3Config = s3Config
                    .withRetryEnabled(false)
                    .withInitialRetryDelay(1)
                    .withProperty("com.sun.jersey.client.property.connectTimeout", 100);

            S3ClientMock client = new S3ClientMock(s3Config, s3Mock);
            return new ChunkedSegmentStorage(containerId,
                    new ExtendedS3ChunkStorage(client, this.config, executorService(), true, false),
                    metadataStore,
                    this.executor,
                    ChunkedSegmentStorageConfig.DEFAULT_CONFIG);
        }

        /**
         * Creates a new instance of a Storage adapter.
         */
        @Override
        public Storage createStorageAdapter() {
            throw new UnsupportedOperationException("SimpleStorageFactory requires ChunkMetadataStore");
        }
    }
    //endregion
}

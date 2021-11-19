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
package io.pravega.segmentstore.server.logs;

import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.CachePolicy;
import io.pravega.segmentstore.server.MetadataBuilder;
import io.pravega.segmentstore.server.ReadIndex;
import io.pravega.segmentstore.server.TestDurableDataLog;
import io.pravega.segmentstore.server.TestDurableDataLogFactory;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.server.reading.ContainerReadIndex;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.cache.CacheStorage;
import io.pravega.segmentstore.storage.cache.DirectMemoryCache;
import io.pravega.segmentstore.storage.mocks.InMemoryDurableDataLogFactory;
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import lombok.Builder;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A class that will setup a {@link DurableLog} for testing.
 */

public class ContainerSetup implements AutoCloseable {
    final ScheduledExecutorService executorService;
    final TestDurableDataLogFactory dataLogFactory;
    final AtomicReference<TestDurableDataLog> dataLog;
    final UpdateableContainerMetadata metadata;
    final ReadIndex readIndex;
    final CacheManager cacheManager;
    final Storage storage;
    final CacheStorage cacheStorage;
    final int checkPointMinCommitCount;
    final int startRetryDelayMillis;
    DurableLogConfig durableLogConfig;

    @Builder
    ContainerSetup(ScheduledExecutorService executorService, int maxAppendSize, int containerId, ReadIndexConfig readIndexConfig,
                   int checkPointMinCommitCount, int startRetryDelayMillis) {
        this.dataLog = new AtomicReference<>();
        this.executorService = executorService;
        this.dataLogFactory = new TestDurableDataLogFactory(new InMemoryDurableDataLogFactory(maxAppendSize, this.executorService), this.dataLog::set);
        this.metadata = new MetadataBuilder(containerId).build();
        this.storage = InMemoryStorageFactory.newStorage(executorService);
        this.storage.initialize(1);
        this.cacheStorage = new DirectMemoryCache(Integer.MAX_VALUE);
        this.cacheManager = new CacheManager(CachePolicy.INFINITE, this.cacheStorage, this.executorService);
        this.readIndex = new ContainerReadIndex(readIndexConfig, metadata, this.storage, this.cacheManager, this.executorService);
        this.checkPointMinCommitCount = checkPointMinCommitCount;
        this.startRetryDelayMillis = startRetryDelayMillis;
    }

    @Override
    public void close() {
        this.readIndex.close();
        this.dataLogFactory.close();
        this.storage.close();
        this.cacheManager.close();
        this.cacheStorage.close();
    }

    public DurableLog createDurableLog() {
        DurableLogConfig config = this.durableLogConfig == null ? defaultDurableLogConfig() : this.durableLogConfig;
        return new DurableLog(config, this.metadata, this.dataLogFactory, this.readIndex, this.executorService);
    }

    void setDurableLogConfig(DurableLogConfig config) {
        this.durableLogConfig = config;
    }

    static DurableLogConfig defaultDurableLogConfig() {
        return createDurableLogConfig(null, null, null, null);
    }

    static DurableLogConfig createDurableLogConfig(Integer checkpointCommitCount, Long checkpointMinTotalCommitLength, Integer startRetryDelayMillis, Integer checkpointMinCommitCount) {
        if (checkpointCommitCount == null) {
            checkpointCommitCount = Integer.MAX_VALUE;
        }

        if (checkpointMinTotalCommitLength == null) {
            checkpointMinTotalCommitLength = Long.MAX_VALUE;
        }

        if (startRetryDelayMillis == null) {
            startRetryDelayMillis = 20;
        }

        if (checkpointMinCommitCount == null) {
            checkpointMinCommitCount = Integer.MAX_VALUE;
        }

        return DurableLogConfig
                .builder()
                .with(DurableLogConfig.CHECKPOINT_MIN_COMMIT_COUNT, checkpointMinCommitCount)
                .with(DurableLogConfig.CHECKPOINT_COMMIT_COUNT, checkpointCommitCount)
                .with(DurableLogConfig.CHECKPOINT_TOTAL_COMMIT_LENGTH, checkpointMinTotalCommitLength)
                .with(DurableLogConfig.START_RETRY_DELAY_MILLIS, startRetryDelayMillis)
                .build();
    }
}
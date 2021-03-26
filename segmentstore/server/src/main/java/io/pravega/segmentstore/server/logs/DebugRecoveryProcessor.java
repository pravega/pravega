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

import com.google.common.base.Preconditions;
import io.pravega.common.function.Callbacks;
import io.pravega.common.util.AbstractDrainingQueue;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.CachePolicy;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.ReadIndexFactory;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.server.containers.ContainerConfig;
import io.pravega.segmentstore.server.containers.StreamSegmentContainerMetadata;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.reading.ContainerReadIndexFactory;
import io.pravega.segmentstore.server.reading.ReadIndexConfig;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.cache.NoOpCache;
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import lombok.RequiredArgsConstructor;

/**
 * Recovery Processor used for Debugging purposes.
 * NOTE: this class is not meant to be used for regular, production code. It exposes operations that should only be executed
 * from the admin tools.
 */
public class DebugRecoveryProcessor extends RecoveryProcessor implements AutoCloseable {
    //region Members

    private final OperationCallbacks callbacks;
    private final ReadIndexFactory readIndexFactory;
    private final CacheManager cacheManager;
    private final Storage storage;

    //endregion

    //region Constructor

    private DebugRecoveryProcessor(UpdateableContainerMetadata metadata, DurableDataLog durableDataLog, ReadIndexFactory readIndexFactory,
                                   Storage storage, CacheManager cacheManager, OperationCallbacks callbacks) {
        super(metadata, durableDataLog, new MemoryStateUpdater(new NoOpInMemoryLog(), readIndexFactory.createReadIndex(metadata, storage)));
        this.readIndexFactory = readIndexFactory;
        this.storage = storage;
        this.callbacks = callbacks;
        this.cacheManager = cacheManager;
    }

    @Override
    public void close() {
        this.readIndexFactory.close();
        this.cacheManager.close();
        this.storage.close();
    }

    /**
     * Creates a new instance of the DebugRecoveryProcessor class with the given arguments.
     *
     * @param containerId     The Id of the Container to recover.
     * @param durableDataLog  A DurableDataLog to recover from.
     * @param config          A ContainerConfig to use during recovery.
     * @param readIndexConfig A ReadIndexConfig to use during recovery.
     * @param executor        An Executor to use for background tasks.
     * @param callbacks       Callbacks to invoke during recovery.
     * @return A new instance of the DebugRecoveryProcessor.
     */
    public static DebugRecoveryProcessor create(int containerId, DurableDataLog durableDataLog, ContainerConfig config, ReadIndexConfig readIndexConfig,
                                                ScheduledExecutorService executor, OperationCallbacks callbacks) {
        Preconditions.checkNotNull(durableDataLog, "durableDataLog");
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(readIndexConfig, "readIndexConfig");
        Preconditions.checkNotNull(executor, "executor");
        Preconditions.checkNotNull(callbacks, callbacks);

        StreamSegmentContainerMetadata metadata = new StreamSegmentContainerMetadata(containerId, config.getMaxActiveSegmentCount());
        CacheManager cacheManager = new CacheManager(new CachePolicy(Long.MAX_VALUE, Duration.ofHours(10), Duration.ofHours(1)),
                new NoOpCache(), executor);
        cacheManager.startAsync().awaitRunning();
        ContainerReadIndexFactory rf = new ContainerReadIndexFactory(readIndexConfig, cacheManager, executor);
        Storage s = new InMemoryStorageFactory(executor).createStorageAdapter();
        return new DebugRecoveryProcessor(metadata, durableDataLog, rf, s, cacheManager, callbacks);
    }

    //endregion

    //region RecoveryProcessor Overrides

    @Override
    protected void recoverOperation(DataFrameRecord<Operation> dataFrameRecord, OperationMetadataUpdater metadataUpdater) throws DataCorruptionException {
        if (this.callbacks.beginRecoverOperation != null) {
            Callbacks.invokeSafely(this.callbacks.beginRecoverOperation, dataFrameRecord.getItem(), dataFrameRecord.getFrameEntries(), null);
        }

        try {
            super.recoverOperation(dataFrameRecord, metadataUpdater);
        } catch (Throwable ex) {
            if (this.callbacks.operationFailed != null) {
                Callbacks.invokeSafely(this.callbacks.operationFailed, dataFrameRecord.getItem(), ex, null);
            }

            throw ex;
        }

        if (this.callbacks.operationSuccess != null) {
            Callbacks.invokeSafely(this.callbacks.operationSuccess, dataFrameRecord.getItem(), null);
        }
    }

    //endregion

    //region OperationCallbacks

    /**
     * Callbacks to pass in to the DebugRecoveryProcessor that will be invoked during recovery.
     */
    @RequiredArgsConstructor
    public static class OperationCallbacks {
        /**
         * Invoked before attempting to recover an operation. Args: Operation, DataFrameEntries making up that operation.
         */
        private final BiConsumer<Operation, List<DataFrameRecord.EntryInfo>> beginRecoverOperation;

        /**
         * Invoked when an operation was successfully recovered.
         */
        private final Consumer<Operation> operationSuccess;

        /**
         * Invoked when an operation failed to recover.
         */
        private final BiConsumer<Operation, Throwable> operationFailed;
    }

    //endregion

    //region NoOpInMemoryLog

    private static class NoOpInMemoryLog extends AbstractDrainingQueue<Operation> {
        @Override
        protected void addInternal(Operation item) {

        }

        @Override
        protected int sizeInternal() {
            return 0;
        }

        @Override
        protected Operation peekInternal() {
            return null;
        }

        @Override
        protected Queue<Operation> fetch(int maxCount) {
            return new ArrayDeque<>(0);
        }
    }

    //endregion
}

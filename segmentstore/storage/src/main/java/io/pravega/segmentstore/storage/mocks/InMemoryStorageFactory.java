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
package io.pravega.segmentstore.storage.mocks;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.SyncStorage;
import io.pravega.segmentstore.storage.rolling.RollingStorage;
import lombok.Getter;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * In-Memory mock for StorageFactory. Contents is destroyed when object is garbage collected.
 */
public class InMemoryStorageFactory implements StorageFactory, AutoCloseable {
    @VisibleForTesting
    protected SharedStorage baseStorage;
    @VisibleForTesting
    @Getter
    protected ScheduledExecutorService executor;

    public InMemoryStorageFactory(ScheduledExecutorService executor) {
        this.executor = Preconditions.checkNotNull(executor, "executor");
        initialize();
    }

    public InMemoryStorageFactory() {

    }

    @Override
    public Storage createStorageAdapter() {
        return new AsyncStorageWrapper(new RollingStorage(this.baseStorage), this.executor);
    }

    @Override
    public SyncStorage createSyncStorage() {
        return this.baseStorage;
    }

    @Override
    public void close() {
        this.baseStorage.closeInternal();
    }

    /**
     * Creates a new InMemory Storage, without a rolling wrapper.
     *
     * @param executor An Executor to use for async operations.
     * @return A new InMemoryStorage.
     */
    @VisibleForTesting
    public static Storage newStorage(Executor executor) {
        return new AsyncStorageWrapper(new InMemoryStorage(), executor);
    }

    public void initialize() {
            this.baseStorage = new InMemoryStorageFactory.SharedStorage();
            this.baseStorage.initializeInternal(1); // InMemoryStorage does not use epochs.
    }

    //region SharedStorage

    private static class SharedStorage extends InMemoryStorage {
        private void closeInternal() {
            super.close();
        }

        private void initializeInternal(long epoch) {
            super.initialize(epoch);
        }

        @Override
        public void initialize(long epoch) {
            Preconditions.checkArgument(epoch > 0, "epoch must be a positive number.");
        }

        @Override
        public void close() {
            // We purposefully do not close the base adapter, as that is shared between all instances of this class.
        }
    }

    //endregion
}

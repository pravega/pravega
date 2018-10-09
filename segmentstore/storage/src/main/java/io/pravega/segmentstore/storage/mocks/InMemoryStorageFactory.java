/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.mocks;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.rolling.RollingStorage;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * In-Memory mock for StorageFactory. Contents is destroyed when object is garbage collected.
 */
public class InMemoryStorageFactory implements StorageFactory, AutoCloseable {
    @VisibleForTesting
    protected SharedStorage baseStorage;
    @VisibleForTesting
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

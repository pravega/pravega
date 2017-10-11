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
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * In-Memory mock for StorageFactory. Contents is destroyed when object is garbage collected.
 */
public class InMemoryStorageFactory implements StorageFactory, AutoCloseable {
    private final InMemoryStorage baseStorage;
    private final ScheduledExecutorService executor;

    public InMemoryStorageFactory(ScheduledExecutorService executor) {
        this.executor = Preconditions.checkNotNull(executor, "executor");
        this.baseStorage = new InMemoryStorage();
        this.baseStorage.initialize(1); // InMemoryStorage does not use epochs.
    }

    @Override
    public Storage createStorageAdapter() {
        return new FencedWrapper(this.baseStorage, this.executor, true);
    }

    @Override
    public void close() {
        this.baseStorage.close();
    }

    /**
     * Creates a new fenced InMemory Storage.
     *
     * @param executor An Executor to use for async operations.
     * @return A new InMemoryStorage.
     */
    @VisibleForTesting
    public static Storage newStorage(Executor executor) {
        return new FencedWrapper(new InMemoryStorage(), executor, false);
    }

    //region FencedWrapper

    private static class FencedWrapper extends AsyncStorageWrapper implements Storage {
        private final AtomicBoolean closed = new AtomicBoolean();
        private final InMemoryStorage baseStorage;
        private final boolean isPreInitialized;

        FencedWrapper(InMemoryStorage baseStorage, Executor executor, boolean isPreInitialized) {
            super(baseStorage, executor);
            this.baseStorage = baseStorage;
            this.isPreInitialized = isPreInitialized;
        }

        @Override
        public void initialize(long epoch) {
            Preconditions.checkArgument(epoch > 0, "epoch must be a positive number.");

            // InMemoryStorage does not use epochs.
            if (!this.isPreInitialized) {
                this.baseStorage.initialize(epoch);
            }
        }

        @Override
        public void close() {
            // We purposefully do not close the base adapter, as that is shared between all instances of this class.
            this.closed.set(true);
        }
    }

    //endregion
}

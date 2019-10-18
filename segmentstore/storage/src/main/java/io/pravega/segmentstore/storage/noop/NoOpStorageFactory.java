/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.noop;

import com.google.common.base.Preconditions;
import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.rolling.RollingStorage;
import java.util.concurrent.Executor;

/**
 * Factory for No-Op mode Storage adapters.
 */
public class NoOpStorageFactory implements StorageFactory {
    private final StorageExtraConfig config;
    private final Executor executor;
    private final StorageFactory baseStorageFactory;

    public NoOpStorageFactory(StorageExtraConfig config, Executor executor, StorageFactory baseStorageFactory) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(executor, "executor");
        Preconditions.checkNotNull(baseStorageFactory, "baseStorageFactory");
        this.config = config;
        this.executor = executor;
        this.baseStorageFactory = baseStorageFactory;
    }

    @Override
    public Storage createStorageAdapter() {
        NoOpStorage s = new NoOpStorage(this.config, this.baseStorageFactory.createSyncStorage());
        return new AsyncStorageWrapper(new RollingStorage(s), this.executor);
    }
}

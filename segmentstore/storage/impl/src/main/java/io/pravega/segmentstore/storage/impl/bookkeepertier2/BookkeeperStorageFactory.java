/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.bookkeepertier2;

import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.impl.extendeds3.ExtendedS3StorageConfig;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.curator.framework.CuratorFramework;

public class BookkeeperStorageFactory implements StorageFactory{
    private final BookkeeperStorageConfig config;
    private final ExecutorService executor;
    private final CuratorFramework zkClient;

    /**
     * Creates a new instance of the NFSStorageFactory class.
     *
     * @param config   The Configuration to use.
     * @param zkClient The curator framework object.
     * @param executor An executor to use for background operations.
     */
    public BookkeeperStorageFactory(BookkeeperStorageConfig config, CuratorFramework zkClient, ScheduledExecutorService executor) {
        this.config = config;
        this.executor = executor;
        this.zkClient = zkClient;
    }

    @Override
    public Storage createStorageAdapter() {
        return new BookkeeperStorage(config, zkClient, executor);
    }
}

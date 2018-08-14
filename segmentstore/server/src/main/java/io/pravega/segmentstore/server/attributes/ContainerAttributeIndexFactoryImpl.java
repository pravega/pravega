/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.attributes;

import com.google.common.base.Preconditions;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.ContainerMetadata;
import io.pravega.segmentstore.server.OperationLog;
import io.pravega.segmentstore.storage.CacheFactory;
import io.pravega.segmentstore.storage.Storage;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Default implementation for AttributeIndexFactory.
 */
public class ContainerAttributeIndexFactoryImpl implements AttributeIndexFactory {
    private final AttributeIndexConfig config;
    private final CacheFactory cacheFactory;
    private final CacheManager cacheManager;
    private final ScheduledExecutorService executor;

    /**
     * Creates a new instance of the ContainerAttributeIndexFactoryImpl class.
     *
     * @param config       The AttributeIndexConfig to use for all Indices.
     * @param cacheFactory A CacheFactory that can be used to create Caches for storing data into.
     * @param cacheManager The CacheManager to use for cache lifecycle management.
     * @param executor     Executor for async operations.
     */
    public ContainerAttributeIndexFactoryImpl(AttributeIndexConfig config, CacheFactory cacheFactory, CacheManager cacheManager,
                                              ScheduledExecutorService executor) {
        this.config = Preconditions.checkNotNull(config, "config");
        this.cacheFactory = Preconditions.checkNotNull(cacheFactory, "cacheFactory");
        this.cacheManager = Preconditions.checkNotNull(cacheManager, "cacheManager");
        this.executor = Preconditions.checkNotNull(executor, "executor");
    }

    @Override
    public ContainerAttributeIndexImpl createContainerAttributeIndex(ContainerMetadata containerMetadata, Storage storage) {
        return new ContainerAttributeIndexImpl(containerMetadata, storage, this.cacheFactory, this.cacheManager, this.config, this.executor);
    }
}

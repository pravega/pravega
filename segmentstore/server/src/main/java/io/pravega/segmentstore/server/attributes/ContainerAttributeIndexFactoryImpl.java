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
import io.pravega.segmentstore.server.ContainerMetadata;
import io.pravega.segmentstore.server.OperationLog;
import io.pravega.segmentstore.storage.Storage;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Default implementation for AttributeIndexFactory.
 */
public class ContainerAttributeIndexFactoryImpl implements AttributeIndexFactory {
    private final AttributeIndexConfig config;
    private final ScheduledExecutorService executor;

    /**
     * Creates a new instance of the ContainerAttributeIndexFactoryImpl class.
     *
     * @param config   The AttributeIndexConfig to use for all Indices.
     * @param executor Executor for async operations.
     */
    public ContainerAttributeIndexFactoryImpl(AttributeIndexConfig config, ScheduledExecutorService executor) {
        this.config = Preconditions.checkNotNull(config, "config");
        this.executor = Preconditions.checkNotNull(executor, "executor");
    }

    @Override
    public ContainerAttributeIndexImpl createContainerAttributeIndex(ContainerMetadata containerMetadata, Storage storage, OperationLog operationLog) {
        return new ContainerAttributeIndexImpl(containerMetadata, storage, operationLog, this.config, this.executor);
    }
}

/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.logs;

import io.pravega.segmentstore.server.OperationLog;
import io.pravega.segmentstore.server.OperationLogFactory;
import io.pravega.segmentstore.server.ReadIndex;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.storage.DurableDataLogFactory;
import com.google.common.base.Preconditions;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Default Factory for DurableLogs.
 */
public class DurableLogFactory implements OperationLogFactory {
    private final DurableDataLogFactory dataLogFactory;
    private final ScheduledExecutorService executor;
    private final DurableLogConfig config;

    /**
     * Creates a new instance of the DurableLogFactory class.
     *
     * @param config         The DurableLogConfig to use.
     * @param dataLogFactory The DurableDataLogFactory to use.
     * @param executor       The Executor to use.
     */
    public DurableLogFactory(DurableLogConfig config, DurableDataLogFactory dataLogFactory, ScheduledExecutorService executor) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(dataLogFactory, "dataLogFactory");
        Preconditions.checkNotNull(executor, "executor");
        this.dataLogFactory = dataLogFactory;
        this.executor = executor;
        this.config = config;
    }

    @Override
    public OperationLog createDurableLog(UpdateableContainerMetadata containerMetadata, ReadIndex readIndex) {
        return new DurableLog(config, containerMetadata, this.dataLogFactory, readIndex, this.executor);
    }
}

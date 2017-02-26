/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server.logs;

import com.emc.pravega.service.server.OperationLog;
import com.emc.pravega.service.server.OperationLogFactory;
import com.emc.pravega.service.server.UpdateableContainerMetadata;
import com.emc.pravega.service.storage.DurableDataLogFactory;
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
    public OperationLog createDurableLog(UpdateableContainerMetadata containerMetadata, CacheUpdater cacheUpdater) {
        return new DurableLog(config, containerMetadata, this.dataLogFactory, cacheUpdater, this.executor);
    }
}

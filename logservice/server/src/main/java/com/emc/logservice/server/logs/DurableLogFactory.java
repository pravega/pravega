package com.emc.logservice.server.logs;

import com.emc.logservice.server.*;
import com.emc.logservice.storageabstraction.DurableDataLogFactory;
import com.google.common.base.Preconditions;

import java.util.concurrent.Executor;

/**
 * Default Factory for DurableLogs.
 */
public class DurableLogFactory implements OperationLogFactory {
    private final DurableDataLogFactory dataLogFactory;
    private final Executor executor;

    public DurableLogFactory(DurableDataLogFactory dataLogFactory, Executor executor){
        Preconditions.checkNotNull(dataLogFactory, "dataLogFactory");
        Preconditions.checkNotNull(executor, "executor");
        this.dataLogFactory = dataLogFactory;
        this.executor = executor;
    }

    @Override
    public OperationLog createDurableLog(UpdateableContainerMetadata containerMetadata, Cache cache) {
        return new DurableLog(containerMetadata, this.dataLogFactory, cache, this.executor);
    }
}

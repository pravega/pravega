package com.emc.logservice.server.logs;

import com.emc.logservice.storageabstraction.DurableDataLogFactory;
import com.emc.logservice.server.*;

/**
 * Default Factory for DurableLogs.
 */
public class DurableLogFactory implements OperationLogFactory {
    private final DurableDataLogFactory dataLogFactory;

    public DurableLogFactory(DurableDataLogFactory dataLogFactory){
        if(dataLogFactory == null){
            throw new NullPointerException("dataLogFactory");
        }

        this.dataLogFactory = dataLogFactory;
    }

    @Override
    public OperationLog createDurableLog(UpdateableContainerMetadata containerMetadata, Cache cache) {
        return new DurableLog(containerMetadata, this.dataLogFactory, cache);
    }
}

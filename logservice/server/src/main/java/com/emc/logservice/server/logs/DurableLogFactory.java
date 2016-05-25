package com.emc.logservice.server.logs;

import com.emc.logservice.storageabstraction.DurableDataLogFactory;
import com.emc.logservice.server.*;

/**
 * Default Factory for DurableLogs.
 */
public class DurableLogFactory implements OperationLogFactory {
    private final DurableDataLogFactory dataFrameLogFactory;

    public DurableLogFactory(DurableDataLogFactory dataFrameLogFactory){
        if(dataFrameLogFactory == null){
            throw new NullPointerException("dataFrameLogFactory");
        }

        this.dataFrameLogFactory = dataFrameLogFactory;
    }

    @Override
    public OperationLog createDurableLog(UpdateableContainerMetadata containerMetadata, Cache cache) {
        return new DurableLog(containerMetadata, this.dataFrameLogFactory, cache);
    }
}

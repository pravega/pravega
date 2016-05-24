package com.emc.logservice.logs;

import com.emc.logservice.*;

/**
 * Default Factory for DurableLogs.
 */
public class DurableLogFactory implements OperationLogFactory {
    private final DataFrameLogFactory dataFrameLogFactory;

    public DurableLogFactory(DataFrameLogFactory dataFrameLogFactory){
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

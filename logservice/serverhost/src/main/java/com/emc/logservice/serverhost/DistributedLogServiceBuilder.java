package com.emc.logservice.serverhost;

import com.emc.logservice.server.mocks.InMemoryServiceBuilder;
import com.emc.logservice.storageabstraction.DurableDataLogFactory;
import com.emc.logservice.storageimplementation.distributedlog.DistributedLogConfig;
import com.emc.logservice.storageimplementation.distributedlog.DistributedLogDataLogFactory;

import java.util.Properties;
import java.util.concurrent.CompletionException;

/**
 * Distributed-Log Service builder (that still writes to memory).
 */
public class DistributedLogServiceBuilder extends InMemoryServiceBuilder {

    public DistributedLogServiceBuilder(int containerCount){
        super(containerCount);
    }

    @Override
    protected DurableDataLogFactory createDataLogFactory() {
        DistributedLogConfig dlConfig;
        try {
            Properties dlProperties = new Properties();
            dlProperties.put("dlog.hostname", "localhost");
            dlProperties.put("dlog.port", "7000");
            dlProperties.put("dlog.namespace", "messaging/distributedlog");

            dlConfig = new DistributedLogConfig(dlProperties);
            DistributedLogDataLogFactory factory = new DistributedLogDataLogFactory("interactive-console", dlConfig);
            factory.initialize();
            return factory;
        }
        catch (Exception ex) {
            throw new CompletionException(ex);
        }
    }

}

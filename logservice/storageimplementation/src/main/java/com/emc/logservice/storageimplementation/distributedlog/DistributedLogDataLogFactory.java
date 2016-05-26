package com.emc.logservice.storageimplementation.distributedlog;

import com.emc.logservice.storageabstraction.DurableDataLog;
import com.emc.logservice.storageabstraction.DurableDataLogFactory;

/**
 * Represents a DurableDataLogFactory that creates and manages instances of DistributedLogDataLog instances.
 */
public class DistributedLogDataLogFactory implements DurableDataLogFactory, AutoCloseable {
    private final DistributedLogConfig config;
    private final LogClient client;

    public DistributedLogDataLogFactory(DistributedLogConfig config) {
        this.config = config;
        this.client = new LogClient(config);
    }

    public void initialize() {
        this.client.initialize();
    }

    @Override
    public DurableDataLog createDurableDataLog(String containerId) {
        return null;
    }

    @Override
    public void close() throws Exception {
        this.client.close();
    }
}

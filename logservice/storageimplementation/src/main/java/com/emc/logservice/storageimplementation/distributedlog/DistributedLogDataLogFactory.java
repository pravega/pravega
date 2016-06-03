package com.emc.logservice.storageimplementation.distributedlog;

import com.emc.logservice.common.ObjectClosedException;
import com.emc.logservice.storageabstraction.*;

/**
 * Represents a DurableDataLogFactory that creates and manages instances of DistributedLogDataLog instances.
 */
public class DistributedLogDataLogFactory implements DurableDataLogFactory {
    private final DistributedLogConfig config;
    private final LogClient client;

    /**
     * Creates a new instance of the DistributedLogDataLogFactory class.
     *
     * @param clientId The Id of the client to set for the DistributedLog client.
     * @param config   DistributedLog configuration.
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If the clientId is invalid.
     */
    public DistributedLogDataLogFactory(String clientId, DistributedLogConfig config) {
        this.config = config;
        this.client = new LogClient(clientId, config);
    }

    /**
     * Initializes this instance of the DistributedLogDataLogFactory.
     *
     * @throws ObjectClosedException   If the DistributedLogDataLogFactory is closed.
     * @throws IllegalStateException   If the DistributedLogDataLogFactory is already initialized.
     * @throws DurableDataLogException If an exception is thrown during initialization. The actual exception thrown may
     *                                 be a derived exception from this one, which provides more information about
     *                                 the failure reason.
     */
    public void initialize() throws DurableDataLogException {
        this.client.initialize();
    }

    //region DurableDataLogFactory Implementation

    @Override
    public DurableDataLog createDurableDataLog(String containerId) {
        String logName = ContainerToLogNameConverter.getLogName(containerId);
        return new DistributedLogDataLog(logName, this.client);
    }

    @Override
    public void close() {
        this.client.close();
    }

    //endregion
}

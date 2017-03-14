/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.storage.impl.distributedlog;

import com.emc.pravega.common.util.Retry;
import com.emc.pravega.service.storage.DurableDataLog;
import com.emc.pravega.service.storage.DurableDataLogException;
import com.emc.pravega.service.storage.DurableDataLogFactory;
import com.google.common.base.Preconditions;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Represents a DurableDataLogFactory that creates and manages instances of DistributedLogDataLog instances.
 */
public class DistributedLogDataLogFactory implements DurableDataLogFactory {

    private static final Retry.RetryAndThrowBase<DurableDataLogException> RETRY_POLICY = Retry // TODO: config.
            .withExpBackoff(100, 4, 5, 30000)
            .retryWhen(DistributedLogDataLog::isRetryable)
            .throwingOn(DurableDataLogException.class);
    private final LogClient client;
    private final ScheduledExecutorService executor;

    /**
     * Creates a new instance of the DistributedLogDataLogFactory class.
     *
     * @param clientId The Id of the client to set for the DistributedLog client.
     * @param config   DistributedLog configuration.
     * @param executor An Executor to use for async operations.
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If the clientId is invalid.
     */
    public DistributedLogDataLogFactory(String clientId, DistributedLogConfig config, ScheduledExecutorService executor) {
        Preconditions.checkNotNull(executor, "executor");
        this.executor = executor;
        this.client = new LogClient(clientId, config);
    }

    /**
     * Initializes this instance of the DistributedLogDataLogFactory.
     *
     * @throws IllegalStateException   If the DistributedLogDataLogFactory is already initialized.
     * @throws DurableDataLogException If an exception is thrown during initialization. The actual exception thrown may
     *                                 be a derived exception from this one, which provides more information about
     *                                 the failure reason.
     */
    public void initialize() throws DurableDataLogException {
        RETRY_POLICY.run(() -> {
            this.client.initialize();
            return null;
        });
    }

    //region DurableDataLogFactory Implementation

    @Override
    public DurableDataLog createDurableDataLog(int containerId) {
        String logName = ContainerToLogNameConverter.getLogName(containerId);
        return new DistributedLogDataLog(logName, this.client, this.executor);
    }

    @Override
    public void close() {
        this.client.close();
    }

    //endregion
}

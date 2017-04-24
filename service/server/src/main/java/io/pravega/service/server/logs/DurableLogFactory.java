/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.service.server.logs;

import io.pravega.service.server.OperationLog;
import io.pravega.service.server.OperationLogFactory;
import io.pravega.service.server.ReadIndex;
import io.pravega.service.server.UpdateableContainerMetadata;
import io.pravega.service.storage.DurableDataLogFactory;
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

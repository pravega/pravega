/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server.reading;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.service.server.ContainerMetadata;
import com.emc.pravega.service.server.ReadIndex;
import com.emc.pravega.service.server.ReadIndexFactory;
import com.emc.pravega.service.storage.CacheFactory;
import com.emc.pravega.service.storage.ReadOnlyStorage;
import com.google.common.base.Preconditions;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Default implementation for ReadIndexFactory.
 */
public class ContainerReadIndexFactory implements ReadIndexFactory {
    private final ScheduledExecutorService executorService;
    private final CacheFactory cacheFactory;
    private final ReadIndexConfig config;
    private final CacheManager cacheManager;
    private boolean closed;

    /**
     * Creates a new instance of the ContainerReadIndexFactory class.
     *
     * @param config          Configuration for the ReadIndex.
     * @param cacheFactory    The CacheFactory to use to create Caches for the ReadIndex.
     * @param executorService The Executor to use to invoke async callbacks.
     */
    public ContainerReadIndexFactory(ReadIndexConfig config, CacheFactory cacheFactory, ScheduledExecutorService executorService) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(cacheFactory, "cacheFactory");
        Preconditions.checkNotNull(executorService, "executorService");

        this.config = config;
        this.cacheFactory = cacheFactory;
        this.executorService = executorService;
        this.cacheManager = new CacheManager(config.getCachePolicy(), this.executorService);

        // Start the CacheManager. It's OK to wait for it to start, as it doesn't do anything expensive during that phase.
        this.cacheManager.startAsync().awaitRunning();
    }

    @Override
    public ReadIndex createReadIndex(ContainerMetadata containerMetadata, ReadOnlyStorage storage) {
        Exceptions.checkNotClosed(this.closed, this);
        return new ContainerReadIndex(this.config, containerMetadata, this.cacheFactory, storage, this.cacheManager, this.executorService);
    }

    @Override
    public void close() {
        if (!this.closed) {
            this.cacheManager.close(); // Closing the CacheManager also stops it.
            this.closed = true;
        }
    }
}

package com.emc.logservice.server;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Manages the lifecycle of all SegmentContainers within a SegmentContainerRegistry.
 * Implementations of this interface will connect to cluster organizers (i.e. ZooKeeper) and control the SegmentContainers
 * within a given Registry based on events from such entities.
 */
public interface SegmentContainerManager extends AutoCloseable {
    /**
     * Initializes the SegmentContainerManager.
     *
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, indicates that this operation completed. If the operation failed,
     * the Future will contain the Exception that caused the failure.
     */
    CompletableFuture<Void> initialize(Duration timeout);

    @Override
    void close();
}

/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.server.segmentstore.service;

/**
 * Manages the lifecycle of all SegmentContainers within a SegmentContainerRegistry.
 * Implementations of this interface will connect to cluster organizers (i.e. ZooKeeper) and control the SegmentContainers
 * within a given Registry based on events from such entities.
 */
public interface SegmentContainerManager extends AutoCloseable {
    /**
     * Initializes the SegmentContainerManager.
     *
     */
    void initialize();

    @Override
    void close();
}

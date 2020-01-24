/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server;

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

/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.fault;

import java.util.concurrent.TimeUnit;

/**
 * Controller cluster configuration. This includes configuration parameters for
 * the cluster listener's thread pool, including minimum and maximum thread pool
 * size, maximum idle time and max queue size.
 */
public interface ControllerClusterListenerConfig {
    /**
     * Fetches the minimum number of threads in the cluster listener executor.
     *
     * @return The minimum number of threads in the cluster listener executor.
     */
    int getMinThreads();

    /**
     * Fetches the maximum number of threads in the cluster listener executor.
     *
     * @return The maximum number of threads in the cluster listener executor.
     */
    int getMaxThreads();

    /**
     * Fetches the maximum idle time for threads in the cluster listener executor.
     *
     * @return the maximum idle time for threads in the cluster listener executor.
     */
    int getIdleTime();

    /**
     * Fetches the timeunit of idleTime parameter.
     *
     * @return The timeunit of idleTime parameter.
     */
    TimeUnit getIdleTimeUnit();

    /**
     * Fetches the maximum size of cluster listener executor's queue.
     *
     * @return The maximum size of cluster listener executor's queue.
     */
    int getMaxQueueSize();
}

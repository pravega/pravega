/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.fault;

import java.util.concurrent.TimeUnit;

/**
 * Controller cluster configuration.
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

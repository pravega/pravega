/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.store.host;

import com.emc.pravega.shared.common.cluster.Host;

import java.util.Map;
import java.util.Set;

/**
 * Configuration for controller's host monitor module.
 */
public interface HostMonitorConfig {
    /**
     * Fetches whether the host monitor module is enabled.
     *
     * @return Whether the host monitor module is enabled.
     */
    boolean isHostMonitorEnabled();

    /**
     * Fetches the minimum interval between two consecutive segment container rebalance operation.
     *
     * @return The minimum interval between two consecutive segment container rebalance operation.
     */
    int getHostMonitorMinRebalanceInterval();

    /**
     * Fetches the maximum number of segment containers.
     *
     * @return The maximum number of segment containers.
     */
    int getContainerCount();

    /**
     * Fetches the host to container mapping, which is required for creating in-memory map.
     *
     * @return The host to container mapping, which is required for creating in-memory map.
     */
    Map<Host, Set<Integer>> getHostContainerMap();
}

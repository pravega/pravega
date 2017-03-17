/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.store.host;

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
     * Fetches the hostname of SSS.
     *
     * @return The hostname of SSS.
     */
    String getSssHost();

    /**
     * Fetches the port on which SSS listens.
     *
     * @return The port on which SSS listens.
     */
    int getSssPort();

    /**
     * Fetches the total number of stream segment containers configured in the system.
     *
     * @return The total number of stream segment containers configured in the system.
     */
    int getContainerCount();
}

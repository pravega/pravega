/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.metrics;

/**
 * Defines a Gauge, which will wrap a gauge instance and its name.
 */
public interface Gauge {
    /**
     * Gets name.
     *
     * @return the name of Gauge
     */
    String getName();
}

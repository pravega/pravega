/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.metrics;

/**
 * Simple stats that require only increment functions on a Long. Metrics like the number of topics, persist queue size
 * etc. should use this.
 */
public interface Counter extends Metric {
    /**
     * Clear this stat.
     */
    void clear();

    /**
     * Increment the value associated with this stat.
     */
    void inc();

    /**
     * Add delta to the value associated with this stat.
     *
     * @param delta the delta
     */
    void add(long delta);

    /**
     * Get the value associated with this stat.
     */
    long get();
}

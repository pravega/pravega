/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.shared.metrics;

/**
 * Simple stats that require only increment and decrement
 * functions on a Long. Metrics like the number of topics, persist queue size
 * etc. should use this.
 */
public interface Counter {
    /**
     * Clear this stat.
     */
    void clear();

    /**
     * Increment the value associated with this stat.
     */
    void inc();

    /**
     * Decrement the value associated with this stat.
     */
    void dec();

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

    /**
     * Gets name.
     */
    String getName();
}

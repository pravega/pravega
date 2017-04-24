/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.shared.metrics;

/**
 * A meter metric which measures mean throughput and exponentially-weighted moving average throughput.
 */
public interface Meter {
    /**
     * Record the occurrence of an event in Meter.
     */
    void recordEvent();

    /**
     * Record the occurrence of a given number of events in Meter.
     *
     * @param n the number of events to mark
     */
    void recordEvents(long n);

    /**
     * Returns the number of events which have been marked.
     *
     * @return the count
     */
    long getCount();

    /**
     * Gets name.
     *
     * @return the name of Gauge
     */
    String getName();
}

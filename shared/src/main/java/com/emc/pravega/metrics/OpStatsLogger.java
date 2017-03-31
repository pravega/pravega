/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.metrics;

import java.time.Duration;

/**
 * This interface handles logging of statistics related to each operation (Write, Read etc.).
 */
public interface OpStatsLogger {

    /**
     * Increment the succeeded op counter with the given eventLatency in NanoSeconds.
     *
     * @param duration the event latency
     */
    void reportSuccessEvent(Duration duration);

    /**
     * Increment the failed op counter with the given eventLatency in NanoSeconds.
     *
     * @param duration the event latency
     */
    void reportFailEvent(Duration duration);

    /**
     * An operation with the given value succeeded.
     *
     * @param value the value
     */
    void reportSuccessValue(long value);

    /**
     * An operation with the given value failed.
     *
     * @param value the value
     */
    void reportFailValue(long value);

    /**
     * To op Stats data. Need this function to support JMX exports and inner test.
     *
     * @return Returns an OpStatsData object with necessary values.
     */
    OpStatsData toOpStatsData();

    /**
     * Clear stats for this operation.
     */
    void clear();
}

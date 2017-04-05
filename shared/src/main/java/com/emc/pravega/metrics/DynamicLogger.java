/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.metrics;

/**
 * A simple interface that only exposes simple type metrics: Counter/Gauge.
 */
public interface DynamicLogger {

    /**
     * Increase Counter with value <i>delta</i> .
     *
     * @param name  the name of Counter
     * @param delta the delta to be added
     */
    void incCounterValue(String name, long delta);

    /**
     * Report gauge value.
     *
     * @param <T>   the type of value
     * @param name  the name of gauge
     * @param value the value to be reported
     */
    <T extends Number> void reportGaugeValue(String name, T value);

    /**
     * Record the occurrence of a given number of events in Meter.
     *
     * @param name   the name of Meter
     * @param number the number of events occurrence
     */
    void recordMeterEvents(String name, long number);
}
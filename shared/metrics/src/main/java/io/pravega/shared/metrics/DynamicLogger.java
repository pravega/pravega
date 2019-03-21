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
     * Updates the counter with value <i>value</i>.
     *
     * @param name              the name of counter
     * @param value             the value to be updated
     */
    void updateCounterValue(String name, long value);

    /**
     * Notifies that the counter will not be updated.
     *
     * @param name              the name of counter
     */
    void freezeCounter(String name);

    /**
     * Notifies that the counters will not be updated.
     *
     * @param names the names of counters
     */
    default void freezeCounters(String... names) {
        for (String name : names) {
            freezeCounter(name);
        }
    }

    /**
     * Report gauge value.
     *
     * @param <T>   the type of value
     * @param name  the name of gauge
     * @param value the value to be reported
     */
    <T extends Number> void reportGaugeValue(String name, T value);

    /**
     * Notifies that the gauge value will not be updated.
     *
     * @param name  the name of gauge
     */
    void freezeGaugeValue(String name);

    /**
     * Record the occurrence of a given number of events in Meter.
     *
     * @param name   the name of Meter
     * @param number the number of events occurrence
     */
    void recordMeterEvents(String name, long number);
}
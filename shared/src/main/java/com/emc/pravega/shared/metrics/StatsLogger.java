/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.shared.metrics;

import java.util.function.Supplier;

/**
 * A simple interface provides create and register Counter/Gauge/OpStatsLogger,
 * and create logger of new scope.
 */
public interface StatsLogger {
    /**
     * Create op stats logger.
     *
     * @param name Stats Name
     * @return logger for an OpStat described by the <i>name</i>.
     */
    OpStatsLogger createStats(String name);

    /**
     * Create counter.
     *
     * @param name Stats Name
     * @return counter described by the <i>name</i>
     */
    Counter createCounter(String name);

    /**
     * Create meter.
     *
     * @param name the meter name
     * @return Create and register Meter described by the <i>name</i>
     */
    Meter createMeter(String name);

    /**
     * Register gauge.
     * <i>value</i> is usually get of Number: AtomicInteger::get, AtomicLong::get
     *
     * @param <T>   the type of value
     * @param name  the name of gauge
     * @param value the supplier to provide value through get()
     */
    <T extends Number> Gauge registerGauge(String name, Supplier<T> value);

    /**
     * Create the stats logger under scope <i>scope</i>.
     *
     * @param scope scope name.
     * @return stats logger under scope <i>scope</i>.
     */
    StatsLogger createScopeLogger(String scope);

}

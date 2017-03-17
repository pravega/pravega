/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */

package com.emc.pravega.common.metrics;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StatsLoggerProxy implements StatsLogger {
    private final AtomicReference<StatsLogger> statsLoggerRef = new AtomicReference<>(new NullStatsLogger());

    StatsLoggerProxy(StatsLogger logger) {
        this.statsLoggerRef.set(logger);
    }

    void setLogger(StatsLogger logger) {
        this.statsLoggerRef.set(logger);
    }

    @Override
    public OpStatsLogger createStats(String name) {
        return this.statsLoggerRef.get().createStats(name);
    }

    @Override
    public Counter createCounter(String name) {
        return this.statsLoggerRef.get().createCounter(name);
    }

    @Override
    public Meter createMeter(String name) {
        return this.statsLoggerRef.get().createMeter(name);
    }

    @Override
    public <T extends Number> Gauge registerGauge(String name, Supplier<T> value) {
        log.info("Registering gauge");
        return this.statsLoggerRef.get().registerGauge(name, value);
    }

    @Override
    public StatsLogger createScopeLogger(String scope) {
        return this.statsLoggerRef.get().createScopeLogger(scope);
    }
}

/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.metrics;

import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StatsLoggerProxy implements StatsLogger {
    private final AtomicReference<StatsLogger> statsLoggerRef = new AtomicReference<>(new NullStatsLogger());
    private final ConcurrentHashMap<OpStatsLoggerProxy, String> opStatsLoggers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<CounterProxy, String> counters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<MeterProxy, String> meters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<GaugeProxy, Map.Entry<String, Supplier<? extends Number>>> gauges = new ConcurrentHashMap<>();

    StatsLoggerProxy(StatsLogger logger) {
        this.statsLoggerRef.set(logger);
    }

    void setLogger(StatsLogger logger) {
        this.statsLoggerRef.set(logger);
        opStatsLoggers.forEach((k, v) -> {
            k.setLogger(createStats(v));
        });

        counters.forEach((k, v) -> {
            k.setCounter(createCounter(v));
        });

        meters.forEach((k, v) -> {
            k.setMeter(createMeter(v));
        });

        gauges.forEach((k, v) -> {
            k.setGauge(registerGauge(v.getKey(), v.getValue()));
        });
    }

    @Override
    public OpStatsLogger createStats(String name) {
        OpStatsLogger logger = this.statsLoggerRef.get().createStats(name);
        OpStatsLoggerProxy proxy = new OpStatsLoggerProxy(logger);
        opStatsLoggers.put(proxy, name);

        return proxy;
    }

    @Override
    public Counter createCounter(String name) {
        Counter counter = this.statsLoggerRef.get().createCounter(name);
        CounterProxy proxy = new CounterProxy(counter);
        counters.put(proxy, name);

        return proxy;
    }

    @Override
    public Meter createMeter(String name) {
        Meter meter = this.statsLoggerRef.get().createMeter(name);
        MeterProxy proxy = new MeterProxy(meter);
        meters.put(proxy, name);

        return proxy;
    }

    @Override
    public <T extends Number> Gauge registerGauge(String name, Supplier<T> value) {
        Gauge gauge = this.statsLoggerRef.get().registerGauge(name, value);
        GaugeProxy proxy = new GaugeProxy(gauge);
        gauges.put(proxy, new AbstractMap.SimpleImmutableEntry<>(name, value));

        return proxy;
    }

    @Override
    public StatsLogger createScopeLogger(String scope) {
        StatsLogger logger = this.statsLoggerRef.get().createScopeLogger(scope);
        StatsLoggerProxy proxy = new StatsLoggerProxy(logger);

        return proxy;
    }
}

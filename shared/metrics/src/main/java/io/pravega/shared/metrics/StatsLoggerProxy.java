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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StatsLoggerProxy implements StatsLogger {
    private final AtomicReference<StatsLogger> statsLoggerRef = new AtomicReference<>(new NullStatsLogger());
    private final ConcurrentHashMap<String, OpStatsLoggerProxy> opStatsLoggers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CounterProxy> counters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, MeterProxy> meters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, GaugeProxy> gauges = new ConcurrentHashMap<>();

    StatsLoggerProxy(StatsLogger logger) {
        this.statsLoggerRef.set(logger);
    }

    void setLogger(StatsLogger logger) {
        this.statsLoggerRef.set(logger);
        this.opStatsLoggers.values().forEach(v -> v.updateInstance(createStats(v.getName())));
        this.counters.values().forEach(v -> v.updateInstance(createCounter(v.getName())));
        this.meters.values().forEach(v -> v.updateInstance(createMeter(v.getName())));
        this.gauges.values().forEach(v -> v.updateInstance(registerGauge(v.getName(), v.getValueSupplier())));
    }

    @Override
    public OpStatsLogger createStats(String name) {
        return getOrSet(this.opStatsLoggers,
                () -> new OpStatsLoggerProxy(this.statsLoggerRef.get().createStats(name), this.opStatsLoggers::remove));
    }

    @Override
    public Counter createCounter(String name) {
        return getOrSet(this.counters,
                () -> new CounterProxy(this.statsLoggerRef.get().createCounter(name), this.counters::remove));
    }

    @Override
    public Meter createMeter(String name) {
        return getOrSet(this.meters,
                () -> new MeterProxy(this.statsLoggerRef.get().createMeter(name), this.meters::remove));
    }

    @Override
    public <T extends Number> Gauge registerGauge(String name, Supplier<T> value) {
        return getOrSet(this.gauges,
                () -> new GaugeProxy(this.statsLoggerRef.get().registerGauge(name, value), value, this.gauges::remove));
    }

    @Override
    public StatsLogger createScopeLogger(String scope) {
        StatsLogger logger = this.statsLoggerRef.get().createScopeLogger(scope);
        return new StatsLoggerProxy(logger);
    }


    /**
     * Atomically gets an existing MetricProxy from the given cache or creates a new one and adds it.
     *
     * @param cache         The Cache to get or insert into.
     * @param proxySupplier A Supplier that creates a new MetricProxy.
     * @param <T>           Type of Metric.
     * @param <V>           Type of MetricProxy.
     * @return Either the existing MetricProxy (if it is already registered) or the newly created one.
     */
    private <T extends Metric, V extends MetricProxy<T>> V getOrSet(ConcurrentHashMap<String, V> cache, Supplier<V> proxySupplier) {
        // We could simply use Map.computeIfAbsent to do everything atomically, however in ConcurrentHashMap, the function
        // is evaluated while holding the lock. As per the method's guidelines, the computation should be quick and not
        // do any IO or acquire other locks, however we have no control over new Metric creation. As such, we use optimistic
        // concurrency, where we assume that the MetricProxy does not exist, create it, and then if it does exist, close
        // the newly created one.
        V newObject = proxySupplier.get();
        V existingObject = cache.putIfAbsent(newObject.getName(), newObject);
        if (existingObject != null) {
            newObject.close();
            return existingObject;
        } else {
            return newObject;
        }
    }
}

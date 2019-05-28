/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import io.pravega.shared.MetricsNames;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.shared.MetricsNames.metricKey;

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
        this.opStatsLoggers.values().forEach(v -> v.updateInstance(this.statsLoggerRef.get().createStats(v.getProxyName())));
        this.counters.values().forEach(v -> v.updateInstance(this.statsLoggerRef.get().createCounter(v.getProxyName())));
        this.meters.values().forEach(v -> v.updateInstance(this.statsLoggerRef.get().createMeter(v.getProxyName())));
        this.gauges.values().forEach(v -> v.updateInstance(this.statsLoggerRef.get().registerGauge(v.getProxyName(), v.getSupplier())));
    }

    @Override
    public OpStatsLogger createStats(String name, String... tags) {
        return getOrSet(this.opStatsLoggers, name,
                metricName -> this.statsLoggerRef.get().createStats(metricName, tags), OpStatsLoggerProxy::new, tags);
    }

    @Override
    public Counter createCounter(String name, String... tags) {
        return getOrSet(this.counters, name,
                metricName -> this.statsLoggerRef.get().createCounter(metricName, tags), CounterProxy::new, tags);
    }

    @Override
    public Meter createMeter(String name, String... tags) {
        return getOrSet(this.meters, name,
                metricName -> this.statsLoggerRef.get().createMeter(metricName, tags), MeterProxy::new, tags);
    }

    @Override
    public Gauge registerGauge(String name, Supplier<Number> supplier, String... tags) {
        return getOrSet(this.gauges, name,
                metricName -> this.statsLoggerRef.get().registerGauge(metricName, supplier, tags),
                (metric, proxyName, c) -> new GaugeProxy(metric, proxyName, c), tags);
    }

    @Override
    public StatsLogger createScopeLogger(String scope) {
        StatsLogger logger = this.statsLoggerRef.get().createScopeLogger(scope);
        return new StatsLoggerProxy(logger);
    }


    /**
     * Atomically gets an existing MetricProxy from the given cache or creates a new one and adds it.
     *
     * @param cache        The Cache to get or insert into.
     * @param name         Metric/Proxy name.
     * @param createMetric A Function that creates a new Metric given its name.
     * @param createProxy  A Function that creates a MetricProxy given its input.
     * @param <T>          Type of Metric.
     * @param <V>          Type of MetricProxy.
     * @return Either the existing MetricProxy (if it is already registered) or the newly created one.
     */
    private <T extends Metric, V extends MetricProxy<T>> V getOrSet(ConcurrentHashMap<String, V> cache, String name,
                                                                    Function<String, T> createMetric,
                                                                    ProxyCreator<T, V> createProxy, String... tags) {
        // We could simply use Map.computeIfAbsent to do everything atomically, however in ConcurrentHashMap, the function
        // is evaluated while holding the lock. As per the method's guidelines, the computation should be quick and not
        // do any IO or acquire other locks, however we have no control over new Metric creation. As such, we use optimistic
        // concurrency, where we assume that the MetricProxy does not exist, create it, and then if it does exist, close
        // the newly created one.
        MetricsNames.MetricKey keys = metricKey(name, tags);
        T newMetric = createMetric.apply(keys.getRegistryKey());
        V newProxy = createProxy.apply(newMetric, keys.getCacheKey(), cache::remove);
        V existingProxy = cache.putIfAbsent(newProxy.getProxyName(), newProxy);
        if (existingProxy != null) {
            newProxy.close();
            newMetric.close();
            return existingProxy;
        } else {
            return newProxy;
        }
    }

    private interface ProxyCreator<T1, R> {
        R apply(T1 metricInstance, String proxyName, Consumer<String> closeCallback);
    }
}

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

import io.micrometer.core.instrument.MeterRegistry;
import io.pravega.common.Exceptions;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import io.pravega.shared.MetricsNames;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.shared.MetricsNames.metricKey;

@Slf4j
public class DynamicLoggerImpl implements DynamicLogger {

    private final long cacheSize;
    private final long cacheEvictionDuration;

    private final MeterRegistry metrics;
    private final StatsLogger underlying;
    private final Cache<String, Counter> countersCache;
    private final Cache<String, Gauge> gaugesCache;
    private final Cache<String, Meter> metersCache;

    public DynamicLoggerImpl(MetricsConfig metricsConfig, MeterRegistry metrics, StatsLogger statsLogger) {
        Preconditions.checkNotNull(metricsConfig, "metricsConfig");
        Preconditions.checkNotNull(metrics, "metrics");
        Preconditions.checkNotNull(statsLogger, "statsLogger");
        this.metrics = metrics;
        this.underlying = statsLogger;
        this.cacheSize = metricsConfig.getDynamicCacheSize();
        this.cacheEvictionDuration = metricsConfig.getDynamicCacheEvictionDurationMinutes().toMillis();

        countersCache = CacheBuilder.newBuilder().
                maximumSize(cacheSize).expireAfterAccess(cacheEvictionDuration, TimeUnit.MILLISECONDS).
                removalListener(new RemovalListener<String, Counter>() {
                    @Override
                    public void onRemoval(RemovalNotification<String, Counter> removal) {
                        Counter counter = removal.getValue();
                        if (removal.getCause() != RemovalCause.REPLACED) {
                            metrics.remove(counter.getId());
                            log.debug("Removed Counter: {}.", counter.getId());
                        }
                    }
                }).
                build();

        gaugesCache = CacheBuilder.newBuilder().
                maximumSize(cacheSize).expireAfterAccess(cacheEvictionDuration, TimeUnit.MILLISECONDS).
                removalListener(new RemovalListener<String, Gauge>() {
                    @Override
                    public void onRemoval(RemovalNotification<String, Gauge> removal) {
                        Gauge gauge = removal.getValue();
                        if (removal.getCause() != RemovalCause.REPLACED) {
                            metrics.remove(gauge.getId());
                            log.debug("Removed Gauge: {}.", gauge.getId());
                        }
                    }
                }).
                build();

        metersCache = CacheBuilder.newBuilder().
            maximumSize(cacheSize).expireAfterAccess(cacheEvictionDuration, TimeUnit.MILLISECONDS).
            removalListener(new RemovalListener<String, Meter>() {
                @Override
                public void onRemoval(RemovalNotification<String, Meter> removal) {
                    Meter meter = removal.getValue();
                    if (removal.getCause() != RemovalCause.REPLACED) {
                        metrics.remove(meter.getId());
                        log.debug("Removed Meter: {}.", meter.getId());
                    }
                }
            }).
            build();
    }

    @Override
    public void incCounterValue(String name, long delta, String... tags) {
        Exceptions.checkNotNullOrEmpty(name, "name");
        Preconditions.checkNotNull(delta);
        try {
            MetricsNames.MetricKey keys = metricKey(name, tags);
            Counter counter = countersCache.get(keys.getCacheKey(), new Callable<Counter>() {
                @Override
                public Counter call() throws Exception {
                    return underlying.createCounter(keys.getRegistryKey(), tags);
                }
            });
            counter.add(delta);
            log.debug("Metrics: increased counter {} by {} ", keys.getCacheKey(), delta);
        } catch (ExecutionException e) {
            log.error("Error while countersCache create counter", e);
        }
    }

    @Override
    public void updateCounterValue(String name, long value, String... tags) {
        Exceptions.checkNotNullOrEmpty(name, "name");
        MetricsNames.MetricKey keys = metricKey(name, tags);
        Counter counter = countersCache.getIfPresent(keys.getCacheKey());
        if (counter != null) {
            counter.clear();
            log.debug("Metrics: cleared counter {}", keys.getCacheKey());
        } else {
            counter = underlying.createCounter(keys.getRegistryKey(), tags);
        }
        counter.add(value);
        log.debug("Metrics: increased counter {} by {} ", keys.getCacheKey(), value);
        countersCache.put(keys.getCacheKey(), counter);
    }

    @Override
    public void freezeCounter(String name, String... tags) {
        MetricsNames.MetricKey keys = metricKey(name, tags);
        Counter counter = countersCache.getIfPresent(keys.getCacheKey());
        if (counter != null) {
            metrics.remove(counter.getId());
            log.debug("Metrics: removed counter {} from registry.", keys.getCacheKey());
        }
        countersCache.invalidate(keys.getRegistryKey());
    }

    @Override
    public <T extends Number> void reportGaugeValue(String name, T value, String... tags) {
        Exceptions.checkNotNullOrEmpty(name, "name");
        Preconditions.checkNotNull(value);
        try {
            MetricsNames.MetricKey keys = metricKey(name, tags);
            Gauge gauge = gaugesCache.get(keys.getCacheKey(), new Callable<Gauge>() {
                @Override
                public Gauge call() throws Exception {
                    return underlying.registerGauge(keys.getRegistryKey(), value::doubleValue, tags);
                }
            });
            gauge.setSupplier(value::doubleValue);
            log.debug("Metrics: set supplier for gauge {}; and current value is {}", keys.getCacheKey(), value.doubleValue());
        } catch (ExecutionException e) {
            log.error("Error accessing gauge through gaugesCache", e);
        }
    }

    @Override
    public void freezeGaugeValue(String name, String... tags) {
        MetricsNames.MetricKey keys = metricKey(name, tags);
        Gauge gauge = gaugesCache.getIfPresent(keys.getCacheKey());
        if (gauge != null) {
            metrics.remove(gauge.getId());
            log.debug("Metrics: removed gauge {} from registry", keys.getCacheKey());
        }
        gaugesCache.invalidate(keys.getCacheKey());
    }

    @Override
    public void recordMeterEvents(String name, long number, String... tags) {
        Exceptions.checkNotNullOrEmpty(name, "name");
        Preconditions.checkNotNull(number);
        MetricsNames.MetricKey keys = metricKey(name, tags);
        try {
            Meter meter = metersCache.get(keys.getCacheKey(), new Callable<Meter>() {
                @Override
                public Meter call() throws Exception {
                    return underlying.createMeter(keys.getRegistryKey(), tags);
                }
            });
            meter.recordEvents(number);
            log.debug("Metrics: record meter {} for event {}", keys.getCacheKey(), number);
        } catch (ExecutionException e) {
            log.error("Error while metersCache create meter", e);
        }
    }
}

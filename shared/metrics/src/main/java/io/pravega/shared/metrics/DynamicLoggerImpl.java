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

import lombok.extern.slf4j.Slf4j;

import static io.pravega.shared.MetricsNames.nameFromTags;

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
        String counterName = nameFromTags(name, tags) + ".Counter";
        try {
            Counter counter = countersCache.get(counterName, new Callable<Counter>() {
                @Override
                public Counter call() throws Exception {
                    return underlying.createCounter(
                            (tags == null || tags.length == 0) ? counterName : name, tags);
                }
            });
            counter.add(delta);
        } catch (ExecutionException e) {
            log.error("Error while countersCache create counter", e);
        }
    }

    @Override
    public void updateCounterValue(String name, long value, String... tags) {
        Exceptions.checkNotNullOrEmpty(name, "name");
        String counterName = nameFromTags(name, tags) + ".Counter";

        Counter counter = countersCache.getIfPresent(counterName);
        if (counter != null) {
            counter.clear();
        } else {
            counter = underlying.createCounter(
                    (tags == null || tags.length == 0) ? counterName : name, tags);
        }
        counter.add(value);
        countersCache.put(name, counter);
    }

    @Override
    public void freezeCounter(String name, String... tags) {
        String counterName = nameFromTags(name, tags) + ".Counter";
        Counter counter = countersCache.getIfPresent(counterName);
        if (counter != null) {
            metrics.remove(counter.getId());
        }
        countersCache.invalidate(counterName);
    }

    @Override
    public <T extends Number> void reportGaugeValue(String name, T value, String... tags) {
        Exceptions.checkNotNullOrEmpty(name, "name");
        Preconditions.checkNotNull(value);
        Gauge newGauge = null;
        String gaugeCacheKey = nameFromTags(name, tags) + ".Gauge";
        String gaugeRegisterKey = (tags == null || tags.length == 0) ? gaugeCacheKey : name;

        if (value instanceof Float) {
            newGauge = underlying.registerGauge(gaugeRegisterKey, value::floatValue, tags);
        } else if (value instanceof Double) {
            newGauge = underlying.registerGauge(gaugeRegisterKey, value::doubleValue, tags);
        } else if (value instanceof Byte) {
            newGauge = underlying.registerGauge(gaugeRegisterKey, value::byteValue, tags);
        } else if (value instanceof Short) {
            newGauge = underlying.registerGauge(gaugeRegisterKey, value::shortValue, tags);
        } else if (value instanceof Integer) {
            newGauge = underlying.registerGauge(gaugeRegisterKey, value::intValue, tags);
        } else if (value instanceof Long) {
            newGauge = underlying.registerGauge(gaugeRegisterKey, value::longValue);
        }

        if (null == newGauge) {
            log.error("Unsupported Number type: {}.", value.getClass().getName());
        } else {
            gaugesCache.put(gaugeCacheKey, newGauge);
        }
    }

    @Override
    public void freezeGaugeValue(String name, String... tags) {
        String gaugeName = nameFromTags(name, tags) + ".Gauge";
        Gauge gauge = gaugesCache.getIfPresent(gaugeName);
        metrics.remove(gauge.getId());
        gaugesCache.invalidate(gaugeName);
    }

    @Override
    public void recordMeterEvents(String name, long number, String... tags) {
        Exceptions.checkNotNullOrEmpty(name, "name");
        Preconditions.checkNotNull(number);
        String meterName = nameFromTags(name, tags) + ".Meter";
        try {
            Meter meter = metersCache.get(meterName, new Callable<Meter>() {
                @Override
                public Meter call() throws Exception {
                    return underlying.createMeter(
                            (tags == null || tags.length == 0) ? meterName : name, tags);
                }
            });
            meter.recordEvents(number);
        } catch (ExecutionException e) {
            log.error("Error while metersCache create meter", e);
        }
    }
}

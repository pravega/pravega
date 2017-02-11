/**
 *  Copyright (c) 2016 Dell Inc. or its subsidiaries. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.common.metrics;

import com.codahale.metrics.MetricRegistry;
import com.emc.pravega.common.Exceptions;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class YammerDynamicLogger implements DynamicLogger {
    private static final long CACHESIZE = MetricsConfig.getDynamicCacheSize();
    private static final long TTLSECONDS = MetricsConfig.getDynamicTTLSeconds();

    protected final MetricRegistry metrics;
    protected final StatsLogger underlying;
    private final Cache<String, Counter> countersCache;
    private final Cache<String, Gauge> gaugesCache;
    private final Cache<String, Meter> metersCache;

    public YammerDynamicLogger(MetricRegistry metrics, StatsLogger statsLogger) {
        Preconditions.checkNotNull(metrics, "metrics");
        Preconditions.checkNotNull(statsLogger, "statsLogger");
        this.metrics = metrics;
        this.underlying = statsLogger;

        countersCache = CacheBuilder.newBuilder().
                maximumSize(CACHESIZE).
                expireAfterAccess(TTLSECONDS, TimeUnit.SECONDS).
                removalListener(new RemovalListener<String, Counter>() {
                    public void onRemoval(RemovalNotification<String, Counter> removal) {
                        Counter counter = removal.getValue();
                        Exceptions.checkNotNullOrEmpty(counter.getName(), "counter");
                        metrics.remove(counter.getName());
                        log.debug("TTL expired, removed Counter: {}.", counter.getName());
                    }
                }).
                build();

        gaugesCache = CacheBuilder.newBuilder().
                maximumSize(CACHESIZE).
                expireAfterAccess(TTLSECONDS, TimeUnit.SECONDS).
                removalListener(new RemovalListener<String, Gauge>() {
                    public void onRemoval(RemovalNotification<String, Gauge> removal) {
                        Gauge gauge = removal.getValue();
                        metrics.remove(gauge.getName());
                        log.debug("TTL expired, removed Gauge: {}.", gauge.getName());
                    }
                }).
                build();

        metersCache = CacheBuilder.newBuilder().
            maximumSize(CACHESIZE).
            expireAfterAccess(TTLSECONDS, TimeUnit.SECONDS).
            removalListener(new RemovalListener<String, Meter>() {
                public void onRemoval(RemovalNotification<String, Meter> removal) {
                    Meter meter = removal.getValue();
                    metrics.remove(meter.getName());
                    log.debug("TTL expired, removed Meter: {}.", meter.getName());
                }
            }).
            build();
    }

    @Override
    public void incCounterValue(String name, long delta) {
        Exceptions.checkNotNullOrEmpty(name, "name");
        Preconditions.checkNotNull(delta);
        String counterName = name + ".Counter";
        try {
            Counter counter = countersCache.get(counterName, new Callable<Counter>() {
                @Override
                public Counter call() throws Exception {
                    return underlying.createCounter(counterName);
                }
            });
            counter.add(delta);
        } catch (ExecutionException e) {
            log.error("Error while countersCache create counter", e);
        }
    }

    @Override
    public <T extends Number> void reportGaugeValue(String name, T value) {
        Exceptions.checkNotNullOrEmpty(name, "name");
        Preconditions.checkNotNull(value);
        Gauge newGauge = null;
        String gaugeName = name + ".Gauge";
        if (value instanceof Float) {
            newGauge = underlying.registerGauge(gaugeName, value::floatValue);
        } else if (value instanceof Double) {
            newGauge = underlying.registerGauge(gaugeName, value::doubleValue);
        } else if (value instanceof Byte) {
            newGauge = underlying.registerGauge(gaugeName, value::byteValue);
        } else if (value instanceof Short) {
            newGauge = underlying.registerGauge(gaugeName, value::shortValue);
        } else if (value instanceof Integer) {
            newGauge = underlying.registerGauge(gaugeName, value::intValue);
        } else if (value instanceof Long) {
            newGauge = underlying.registerGauge(gaugeName, value::longValue);
        }

        if (null == newGauge) {
            log.error("Unsupported Number type: {}.", value.getClass().getName());
        } else {
            if (null == gaugesCache.getIfPresent(gaugeName)) {
                gaugesCache.put(gaugeName, newGauge);
            }
        }
    }

    @Override
    public void recordMeterEvents(String name, long number) {
        Exceptions.checkNotNullOrEmpty(name, "name");
        Preconditions.checkNotNull(number);
        String meterName = name + ".Meter";
        try {
            Meter meter = metersCache.get(meterName, new Callable<Meter>() {
                @Override
                public Meter call() throws Exception {
                    return underlying.createMeter(meterName);
                }
            });
            meter.recordEvents(number);
        } catch (ExecutionException e) {
            log.error("Error while metersCache create meter", e);
        }
    }
}

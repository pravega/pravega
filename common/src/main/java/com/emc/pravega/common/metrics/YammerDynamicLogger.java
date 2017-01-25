/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.common.metrics;

import com.codahale.metrics.MetricRegistry;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@Slf4j
public class YammerDynamicLogger implements DynamicLogger {
    private static final long CACHESIZE = 1000000L;
    private static final long TTLSECONDS = 120L;

    protected final MetricRegistry metrics;
    protected final StatsLogger underlying;
    protected final ConcurrentMap<String, Counter> counters;
    protected final ConcurrentMap<String, Gauge> gauges;
    final Cache<String, Counter> counterCache;
    final Cache<String, Gauge> gaugeCache;

    public YammerDynamicLogger(MetricRegistry metrics, StatsLogger statsLogger) {
        this.metrics = metrics;
        this.underlying = statsLogger;

        counterCache = CacheBuilder.newBuilder().
                maximumSize(CACHESIZE).
                expireAfterAccess(TTLSECONDS, TimeUnit.SECONDS).
                removalListener(new RemovalListener<String, Counter>() {
                    public void onRemoval(RemovalNotification<String, Counter> removal) {
                        Counter counter = removal.getValue();
                        metrics.remove(counter.getName());
                        log.debug("TTL expire, removed Counter: " + counter.getName());
                    }
                }).
                build();

        this.counters = counterCache.asMap();

        gaugeCache = CacheBuilder.newBuilder().
                maximumSize(CACHESIZE).
                expireAfterAccess(TTLSECONDS, TimeUnit.SECONDS).
                removalListener(new RemovalListener<String, Gauge>() {
                    public void onRemoval(RemovalNotification<String, Gauge> removal) {
                        Gauge gauge = removal.getValue();
                        metrics.remove(gauge.getName());
                        log.debug("TTL expire, removed Gauge: " + gauge.getName());
                    }
                }).
                build();
        this.gauges = gaugeCache.asMap();
    }

    @Override
    public Counter createCounter(String name) {
        String counterName = name + ".Counter";
        Counter counter = counters.get(counterName);
        if (null == counter) {
            Counter newCounter = underlying.createCounter(counterName);
            Counter oldCounter = counters.putIfAbsent(counterName, newCounter);
            counter = (null == oldCounter) ? newCounter : oldCounter;
        }
        log.debug("created Counter: " + counter.getName());
        return counter;
    }

    @Override
    public <T extends Number> Gauge registerGauge(String name, Supplier<T> value) {
        String gaugeName = name + ".Gauge";
        Gauge gauge = gauges.get(gaugeName);
        if (null == gauge) {
            Gauge newGauge = underlying.registerGauge(gaugeName, value);
            Gauge oldGauge = gauges.putIfAbsent(gaugeName, newGauge);
            gauge = (null == oldGauge) ? newGauge : oldGauge;
        }
        log.debug("registerd Gauge: " + gauge.getName());
        return gauge;
    }
}

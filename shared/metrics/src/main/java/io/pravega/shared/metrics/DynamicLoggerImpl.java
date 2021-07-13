/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.shared.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.pravega.common.Exceptions;
import io.pravega.common.util.SimpleCache;
import io.pravega.shared.MetricsNames;
import java.time.Duration;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.shared.MetricsNames.metricKey;

@Slf4j
class DynamicLoggerImpl implements DynamicLogger {
    private final MeterRegistry metrics;
    private final StatsLogger underlying;
    private final SimpleCache<String, Counter> countersCache;
    private final SimpleCache<String, Gauge> gaugesCache;
    private final SimpleCache<String, Meter> metersCache;

    DynamicLoggerImpl(@NonNull MetricsConfig metricsConfig, @NonNull MeterRegistry metrics, @NonNull StatsLogger statsLogger) {
        this.metrics = metrics;
        this.underlying = statsLogger;

        int cacheSize = metricsConfig.getDynamicCacheSize();
        Duration cacheEvictionDuration = metricsConfig.getDynamicCacheEvictionDurationMinutes();
        this.countersCache = new SimpleCache<>(cacheSize, cacheEvictionDuration, this::unregister);
        this.gaugesCache = new SimpleCache<>(cacheSize, cacheEvictionDuration, this::unregister);
        this.metersCache = new SimpleCache<>(cacheSize, cacheEvictionDuration, this::unregister);
    }

    @Override
    public void incCounterValue(String name, long delta, String... tags) {
        getCounter(name, tags).add(delta);
    }

    @Override
    public void updateCounterValue(String name, long value, String... tags) {
        Counter c = getCounter(name, tags);
        c.clear();
        c.add(value);
    }

    private Counter getCounter(String name, String... tags) {
        MetricsNames.MetricKey keys = metricKey(name, tags);
        Counter counter = this.countersCache.get(keys.getCacheKey());
        if (counter == null) {
            counter = this.underlying.createCounter(keys.getRegistryKey(), tags);
            this.countersCache.putIfAbsent(keys.getCacheKey(), counter);
        }
        return counter;
    }

    @Override
    public <T extends Number> void reportGaugeValue(String name, T value, String... tags) {
        Exceptions.checkNotNullOrEmpty(name, "name");
        MetricsNames.MetricKey keys = metricKey(name, tags);
        Gauge gauge = this.gaugesCache.get(keys.getCacheKey());
        if (gauge == null) {
            gauge = this.underlying.registerGauge(keys.getRegistryKey(), value::doubleValue, tags);
            this.gaugesCache.putIfAbsent(keys.getCacheKey(), gauge);
        }
        gauge.setSupplier(value::doubleValue);
    }

    @Override
    public void recordMeterEvents(String name, long number, String... tags) {
        Exceptions.checkNotNullOrEmpty(name, "name");
        MetricsNames.MetricKey keys = metricKey(name, tags);
        Meter meter = this.metersCache.get(keys.getCacheKey());
        if (meter == null) {
            meter = this.underlying.createMeter(keys.getRegistryKey(), tags);
            this.metersCache.putIfAbsent(keys.getCacheKey(), meter);
        }

        meter.recordEvents(number);
    }

    @Override
    public void freezeCounter(String name, String... tags) {
        freeze(this.countersCache, name, tags);
    }

    @Override
    public void freezeGaugeValue(String name, String... tags) {
        freeze(this.gaugesCache, name, tags);
    }

    @Override
    public void freezeMeter(String name, String... tags) {
        freeze(this.metersCache, name, tags);
    }

    private <T extends Metric> void freeze(SimpleCache<String, T> cache, String name, String... tags) {
        Exceptions.checkNotNullOrEmpty(name, "name");
        MetricsNames.MetricKey keys = metricKey(name, tags);
        T metric = cache.remove(keys.getCacheKey());
        if (metric != null) {
            metric.close();
            unregister(metric);
        }
    }

    private void unregister(String cacheKey, Metric m) {
        unregister(m);
    }

    private void unregister(Metric m) {
        this.metrics.remove(m.getId());
        log.trace("Closed Metric: {}.", m.getId());
    }
}

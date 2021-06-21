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
     * In this case a read-modify-write cycle needs to happen atomically. A number of threads may gain a reference
     * to any given {@link MetricProxy}, which makes available the underlying {@link Meter} instance that is bound to
     * a {@link io.micrometer.core.instrument.composite.CompositeMeterRegistry}. We must be considerate when closing the
     * {@link Meter} from the proxy as that operation has side effects.
     *
     * If one thread has a reference to a {@link MetricProxy} we have to ensure that when another thread gains a reference
     * to that proxy, the {@link Meter} is still in a valid state (not removed from the CompositeMeterRegistry). Furthermore
     * the {@link MetricProxy} removes itself from the {@link StatsLoggerProxy} cache using a callback, so calling {@link MetricProxy#close()}
     * on one thread, may invalidate it in another.
     *
     * @param cache        The Cache to get or insert into.
     * @param name         Metric/Proxy name.
     * @param createMetric A Function that creates a new Metric given its name.
     * @param createProxy  A Function that creates a MetricProxy given its input.
     * @param <T>          Type of Metric.
     * @param <V>          Type of MetricProxy.
     * @return Either the existing MetricProxy (if it is already registered) or the newly created one.
     */
    private static <T extends Metric, V extends MetricProxy<T, V>> V getOrSet(ConcurrentHashMap<String, V> cache, String name,
                                                                              Function<String, T> createMetric,
                                                                              ProxyCreator<T, V> createProxy, String... tags) {
        // We have to create the metric inside computeIfAbsent even though it is under lock, because micrometer is optimized
        // such that the call to create will return the original metric if it has already been created. So when close is called
        // on one of the metrics it will close both of them. 
        MetricsNames.MetricKey keys = metricKey(name, tags);
        Consumer<V> closeCallback = m -> cache.remove(m.getProxyName(), m);
        return cache.computeIfAbsent(keys.getCacheKey(), k -> {            
            T newMetric = createMetric.apply(keys.getRegistryKey());
            return createProxy.apply(newMetric, keys.getCacheKey(), closeCallback);
        });
    }

    private interface ProxyCreator<T1, R> {
        R apply(T1 metricInstance, String proxyName, Consumer<R> closeCallback);
    }
}

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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.influx.InfluxMeterRegistry;
import io.micrometer.statsd.StatsdMeterRegistry;
import lombok.Getter;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;

import static io.pravega.shared.MetricsTags.DEFAULT_HOSTNAME_KEY;
import static io.pravega.shared.MetricsTags.createHostTag;

@Slf4j
class StatsProviderImpl implements StatsProvider {
    @Getter
    private final CompositeMeterRegistry metrics;
    private final MetricsConfig conf;

    StatsProviderImpl(MetricsConfig conf) {
        this(conf, Metrics.globalRegistry);
    }

    @VisibleForTesting
    StatsProviderImpl(MetricsConfig conf, CompositeMeterRegistry registry) {
        this.conf = Preconditions.checkNotNull(conf, "conf");
        this.metrics = registry;
    }

    @Synchronized
    private void init() {
        new JvmMemoryMetrics().bindTo(metrics);
        new JvmGcMetrics().bindTo(metrics);
        new ProcessorMetrics().bindTo(metrics);
        new JvmThreadMetrics().bindTo(metrics);
    }

    @Synchronized
    @Override
    public void start() {
        init();
        log.info("Metrics prefix: {}", conf.getMetricsPrefix());

        if (conf.isEnableStatsDReporter()) {
            metrics.add(new StatsdMeterRegistry(RegistryConfigUtil.createStatsDConfig(conf), Clock.SYSTEM));
        }

        if (conf.isEnableInfluxDBReporter()) {
            metrics.add(new InfluxMeterRegistry(RegistryConfigUtil.createInfluxConfig(conf), Clock.SYSTEM));
        }
        metrics.config().commonTags(createHostTag(DEFAULT_HOSTNAME_KEY));
        Preconditions.checkArgument(metrics.getRegistries().size() != 0,
                "No meter register bound hence no storage for metrics!");
    }

    @Synchronized
    @Override
    public void startWithoutExporting() {

        for (MeterRegistry registry : new ArrayList<MeterRegistry>(metrics.getRegistries())) {
            metrics.remove(registry);
        }

        Metrics.addRegistry(new SimpleMeterRegistry());
        metrics.config().commonTags(createHostTag(DEFAULT_HOSTNAME_KEY));
    }

    @Synchronized
    @Override
    public void close() {
        for (MeterRegistry registry : metrics.getRegistries()) {
            registry.close();
            metrics.remove(registry);
        }
    }

    @Override
    public StatsLogger createStatsLogger(String name) {
        init();
        return new StatsLoggerImpl(getMetrics());
    }

    @Override
    public DynamicLogger createDynamicLogger() {
        init();
        return new DynamicLoggerImpl(conf, metrics, new StatsLoggerImpl(getMetrics()));
    }
}

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

import com.google.common.base.Preconditions;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.influx.InfluxMeterRegistry;
import io.micrometer.statsd.StatsdMeterRegistry;
import lombok.Getter;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class StatsProviderImpl implements StatsProvider {
    @Getter
    private final MeterRegistry metrics = Metrics.globalRegistry;
    private final MetricsConfig conf;

    StatsProviderImpl(MetricsConfig conf) {
        this.conf = Preconditions.checkNotNull(conf, "conf");
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

        if (conf.isEnableStatsdReporter()) {
            Metrics.addRegistry(new StatsdMeterRegistry(RegistryConfigUtil.createStatsdConfig(conf), Clock.SYSTEM));
        }

        if (conf.isEnableInfluxDBReporter()) {
            Metrics.addRegistry(new InfluxMeterRegistry(RegistryConfigUtil.createInfluxConfig(conf), Clock.SYSTEM));
        }

        if (Metrics.globalRegistry.getRegistries().size() == 0) {
            log.error("Error! No concrete metrics register bound, the composite registry runs as no-op!");
        }
    }

    @Synchronized
    @Override
    public void startWithoutExporting() {
        for (MeterRegistry registry : Metrics.globalRegistry.getRegistries()) {
            Metrics.globalRegistry.remove(registry);
        }
        Metrics.addRegistry(new SimpleMeterRegistry());
    }

    @Synchronized
    @Override
    public void close() {
        for (MeterRegistry registry : Metrics.globalRegistry.getRegistries()) {
            registry.close();
            Metrics.globalRegistry.remove(registry);
        }
    }

    @Override
    public StatsLogger createStatsLogger(String name) {
        init();
        return new StatsLoggerImpl(getMetrics(), "pravega." + name);
    }

    @Override
    public DynamicLogger createDynamicLogger() {
        init();
        return new DynamicLoggerImpl(conf, metrics, new StatsLoggerImpl(getMetrics(), "pravega"));
    }
}

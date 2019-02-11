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
import java.time.Duration;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.influx.InfluxConfig;
import io.micrometer.influx.InfluxMeterRegistry;
import io.micrometer.statsd.StatsdConfig;
import io.micrometer.statsd.StatsdFlavor;
import io.micrometer.statsd.StatsdMeterRegistry;
import lombok.Getter;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class StatsProviderImpl implements StatsProvider {
    @Getter
    private final MeterRegistry metrics = MetricsProvider.METRIC_REGISTRY;
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
            log.info("Configuring stats with statsD at {}:{}", conf.getStatsDHost(), conf.getStatsDPort());
            StatsdConfig config = new StatsdConfig() {
                @Override
                public Duration step() {
                    return Duration.ofSeconds(conf.getStatsOutputFrequencySeconds().getSeconds());
                }

                @Override
                public String prefix() {
                    return conf.getMetricsPrefix();
                }

                @Override
                public String host() {
                    return conf.getStatsDHost();
                }

                @Override
                public int port() {
                    return conf.getStatsDPort();
                }

                @Override
                public StatsdFlavor flavor() {
                    return StatsdFlavor.TELEGRAF; //Dimension supported
                }

                @Override
                public String get(String key) {
                    return null;
                }
            };

            MeterRegistry registry = new StatsdMeterRegistry(config, Clock.SYSTEM);
            Metrics.addRegistry(registry);
        }

        if (conf.isEnableInfluxDBReporter()) {
            log.info("Configuring stats with direct InfluxDB at {}", conf.getInfluxDBUri());
            InfluxConfig config = new InfluxConfig() {
                @Override
                public Duration step() {
                    return Duration.ofSeconds(conf.getStatsOutputFrequencySeconds().getSeconds());
                }
                
                @Override
                public String prefix() {
                    return conf.getMetricsPrefix();
                }

                @Override
                public String uri() {
                    return conf.getInfluxDBUri();
                }

                @Override
                public String db() {
                    return conf.getInfluxDBName();
                }

                @Override
                public String userName() {
                    return conf.getInfluxDBUserName();
                }

                @Override
                public String password() {
                    return conf.getInfluxDBPassword();
                }

                @Override
                public String get(String k) {
                    return null;
                }
            };

            MeterRegistry registry = new InfluxMeterRegistry(config, Clock.SYSTEM);
            Metrics.addRegistry(registry);
        }
        //TODO: add more registries
    }

    @Synchronized
    @Override
    public void close() {
        metrics.close();
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

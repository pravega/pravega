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

import io.micrometer.influx.InfluxConfig;
import io.micrometer.statsd.StatsdConfig;
import io.micrometer.statsd.StatsdFlavor;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;

@Slf4j
public class RegistryConfigUtil {

    /**
     * Create StatsdConfig for StatsD Register.
     *
     * @param conf the metric config from Pravega.
     * @return instance of StatsdConfig to be used by StatsD Register.
     */
    public static StatsdConfig createStatsDConfig(MetricsConfig conf) {
        log.info("Configuring stats with statsD at {}:{}", conf.getStatsDHost(), conf.getStatsDPort());
        return new StatsdConfig() {
            @Override
            public Duration step() {
                return Duration.ofSeconds(conf.getOutputFrequencySeconds().getSeconds());
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
                return StatsdFlavor.TELEGRAF;
            }

            @Override
            public String get(String key) {
                return null;
            }
        };
    }

    /**
     * Create InfluxConfig for InfluxDB Register.
     *
     * @param conf the metric config from Pravega.
     * @return     instance of InfluxConfig to be used by InfluxDB register.
     */
    public static InfluxConfig createInfluxConfig(MetricsConfig conf) {
        log.info("Configuring stats with direct InfluxDB at {}", conf.getInfluxDBUri());
        return new InfluxConfig() {
            @Override
            public Duration step() {
                return Duration.ofSeconds(conf.getOutputFrequencySeconds().getSeconds());
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
    }
}

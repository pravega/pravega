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

import io.micrometer.influx.InfluxConfig;
import io.micrometer.statsd.StatsdConfig;
import io.micrometer.statsd.StatsdFlavor;
import io.micrometer.prometheus.PrometheusConfig;
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
                return null; // accept the rest of the defaults; see https://micrometer.io/docs/registry/statsD.
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
                return String.valueOf(conf.getInfluxDBPassword());
            }

            @Override
            public String retentionPolicy() {
                return conf.getInfluxDBRetention();
            }

            @Override
            public String get(String k) {
                return null;  // accept the rest of the defaults, see https://micrometer.io/docs/registry/influx.
            }
        };
    }

    /**
     * Create PrometheusConfig for Prometheus Register.
     *
     * @param conf The metric config from Pravega.
     * @return     An instance of PrometheusConfig to be used by Prometheus register.
     */
    public static PrometheusConfig createPrometheusConfig(MetricsConfig conf) {
        log.info("Configuring stats with prometheus");
        return new PrometheusConfig() {
            @Override
            public Duration step() {
                return Duration.ofSeconds(conf.getOutputFrequencySeconds().getSeconds());
            }

            @Override
            public String prefix() {
                return conf.getMetricsPrefix();
            }

            @Override
            public String get(String k) {
                return null;  // accept the rest of the defaults
            }
        };
    }

}

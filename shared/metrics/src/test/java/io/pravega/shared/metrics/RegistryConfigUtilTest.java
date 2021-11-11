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
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Tests RegistryConfigUtil
 */
@Slf4j
public class RegistryConfigUtilTest {

    @Test
    public void testStatsDConfig() {
        MetricsConfig appConfig = MetricsConfig.builder()
                .with(MetricsConfig.ENABLE_STATISTICS, true)
                .with(MetricsConfig.OUTPUT_FREQUENCY, 37)
                .with(MetricsConfig.METRICS_PREFIX, "statsDPrefix")
                .with(MetricsConfig.STATSD_HOST, "localhost")
                .with(MetricsConfig.STATSD_PORT, 8225)
                .build();

        StatsdConfig testConfig = RegistryConfigUtil.createStatsDConfig(appConfig);
        assertTrue(37 == testConfig.step().getSeconds());
        assertEquals("statsDPrefix", testConfig.prefix());
        assertEquals("localhost", testConfig.host());
        assertTrue(8225 == testConfig.port());
        assertEquals(StatsdFlavor.TELEGRAF, testConfig.flavor());
        assertNull(testConfig.get("Undefined Key"));
    }

    @Test
    public void testInfluxConfig() {
        MetricsConfig appConfig = MetricsConfig.builder()
                .with(MetricsConfig.OUTPUT_FREQUENCY, 39)
                .with(MetricsConfig.METRICS_PREFIX, "influxPrefix")
                .with(MetricsConfig.INFLUXDB_URI, "http://localhost:2375")
                .with(MetricsConfig.INFLUXDB_NAME, "databaseName")
                .with(MetricsConfig.INFLUXDB_USERNAME, "admin")
                .with(MetricsConfig.INFLUXDB_PASSWORD, "changeme")
                .with(MetricsConfig.INFLUXDB_RETENTION_POLICY, "2h")
                .build();

        InfluxConfig testConfig = RegistryConfigUtil.createInfluxConfig(appConfig);
        assertTrue(39 == testConfig.step().getSeconds());
        assertEquals("influxPrefix", testConfig.prefix());
        assertEquals("http://localhost:2375", testConfig.uri());
        assertEquals("databaseName", testConfig.db());
        assertEquals("admin", testConfig.userName());
        assertEquals("changeme", testConfig.password());
        assertEquals("2h", testConfig.retentionPolicy());
        assertNull(testConfig.get("Undefined Key"));
    }

    @Test
    public void testPrometheusConfig() {
        MetricsConfig appConfig = MetricsConfig.builder()
                .with(MetricsConfig.OUTPUT_FREQUENCY, 40)
                .with(MetricsConfig.METRICS_PREFIX, "prometheusPrefix")
                .build();

        InfluxConfig testConfig = RegistryConfigUtil.createInfluxConfig(appConfig);
        assertTrue(40 == testConfig.step().getSeconds());
        assertEquals("prometheusPrefix", testConfig.prefix());
        assertNull(testConfig.get("Undefined Key"));
    }
}

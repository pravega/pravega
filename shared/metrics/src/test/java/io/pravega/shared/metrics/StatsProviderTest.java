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
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.influx.InfluxMeterRegistry;
import io.micrometer.statsd.StatsdMeterRegistry;
import io.pravega.test.common.SerializedClassRunner;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for StatsProvider
 */
@Slf4j
@RunWith(SerializedClassRunner.class)
public class StatsProviderTest {
    @Test
    public void testStatsProviderStartAndClose() {
        //To improve test case isolation, create a new registry instead of using the global one.
        @Cleanup
        CompositeMeterRegistry localRegistry = new CompositeMeterRegistry();

        MetricsConfig appConfig = MetricsConfig.builder()
                .with(MetricsConfig.ENABLE_STATISTICS, true)
                .with(MetricsConfig.ENABLE_STATSD_REPORTER, true)
                .with(MetricsConfig.ENABLE_INFLUXDB_REPORTER, false)
                .build();

        @Cleanup
        StatsProvider statsProvider = new StatsProviderImpl(appConfig, localRegistry);
        statsProvider.start();

        for (MeterRegistry registry : localRegistry.getRegistries()) {
            assertFalse(registry instanceof InfluxMeterRegistry);
            assertTrue(registry instanceof StatsdMeterRegistry);
        }

        statsProvider.close();
        assertTrue(0 == localRegistry.getRegistries().size());
    }

    @Test
    public void testStatsProviderStartWithoutExporting() {
        MetricsConfig appConfig = MetricsConfig.builder()
                .with(MetricsConfig.ENABLE_STATISTICS, true)
                .with(MetricsConfig.ENABLE_STATSD_REPORTER, true)
                .with(MetricsConfig.ENABLE_INFLUXDB_REPORTER, true)
                .build();
        @Cleanup
        CompositeMeterRegistry localRegistry = new CompositeMeterRegistry();

        @Cleanup
        StatsProvider statsProvider = new StatsProviderImpl(appConfig, localRegistry);
        statsProvider.startWithoutExporting();

        for (MeterRegistry registry : localRegistry.getRegistries()) {
            assertTrue(registry instanceof SimpleMeterRegistry);
            assertFalse(registry instanceof InfluxMeterRegistry);
            assertFalse(registry instanceof StatsdMeterRegistry);
        }

        statsProvider.close();
        assertTrue(0 == localRegistry.getRegistries().size());
    }

    @Test (expected = Exception.class)
    public void testStatsProviderNoRegisterBound() {
        MetricsConfig appConfig = MetricsConfig.builder()
                .with(MetricsConfig.ENABLE_STATISTICS, true)
                .with(MetricsConfig.ENABLE_STATSD_REPORTER, false)
                .with(MetricsConfig.ENABLE_INFLUXDB_REPORTER, false)
                .build();
        @Cleanup
        CompositeMeterRegistry localRegistry = new CompositeMeterRegistry();

        @Cleanup
        StatsProvider statsProvider = new StatsProviderImpl(appConfig, localRegistry);
        statsProvider.start();
    }
}

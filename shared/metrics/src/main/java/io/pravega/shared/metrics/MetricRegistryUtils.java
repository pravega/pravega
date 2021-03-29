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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.DistributionSummary;

import lombok.extern.slf4j.Slf4j;

/**
 * Utility class to access metrics stored in the local cache of the metrics registry.
 */
@Slf4j
public class MetricRegistryUtils {

    public static Counter getCounter(String metricsName, String... tags) {
        return Metrics.globalRegistry.find(metricsName).tags(tags).counter();
    }

    public static DistributionSummary getMeter(String metricsName, String... tags) {
        return Metrics.globalRegistry.find(metricsName).tags(tags).summary();
    }

    public static Gauge getGauge(String metricsName, String... tags) {
        return Metrics.globalRegistry.find(metricsName).tags(tags).gauge();
    }

    public static Timer getTimer(String metricsName, String... tags) {
        return Metrics.globalRegistry.find(metricsName).tags(tags).timer();
    }
}

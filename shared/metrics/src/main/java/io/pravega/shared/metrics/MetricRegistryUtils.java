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
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.util.Collection;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * FOR TESTS ONLY. 
 * Utility class to access metrics stored in the local cache of the metrics registry.
 * NOTE: All operations on this class take O(n) where n is the number of metrics in the system.
 */
@Slf4j
@VisibleForTesting
public class MetricRegistryUtils {

    public static Counter getCounter(String metricsName, String... tags) {
        Collection<Counter> counters = Metrics.globalRegistry.find(metricsName).tags(tags).counters();
        int size = counters.size();
        Preconditions.checkState(size <= 1, "Metric was not unique %s", counters.stream().map(m -> m.getId()).collect(Collectors.toList()));
        if (size == 0) {
            return null;
        } else {
            return counters.iterator().next();
        }
    }

    public static DistributionSummary getMeter(String metricsName, String... tags) {
        Collection<DistributionSummary> summaries = Metrics.globalRegistry.find(metricsName).tags(tags).summaries();
        int size = summaries.size();
        Preconditions.checkState(size <= 1, "Metric was not unique %s", summaries.stream().map(m -> m.getId()).collect(Collectors.toList()));
        if (size == 0) {
            return null;
        } else {
            return summaries.iterator().next();
        }
    }

    public static Gauge getGauge(String metricsName, String... tags) {
        Collection<Gauge> gauges = Metrics.globalRegistry.find(metricsName).tags(tags).gauges();
        int size = gauges.size();
        Preconditions.checkState(size <= 1, "Metric was not unique %s", gauges.stream().map(m -> m.getId()).collect(Collectors.toList()));
        if (size == 0) {
            return null;
        } else {
            return gauges.iterator().next();
        }
    }

    public static Timer getTimer(String metricsName, String... tags) {
        Collection<Timer> timers = Metrics.globalRegistry.find(metricsName).tags(tags).timers();
        int size = timers.size();
        Preconditions.checkState(size <= 1, "Metric was not unique %s", timers.stream().map(m -> m.getId()).collect(Collectors.toList()));
        if (size == 0) {
            return null;
        } else {
            return timers.iterator().next();
        } 
    }
}

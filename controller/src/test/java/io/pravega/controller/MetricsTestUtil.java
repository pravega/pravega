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
package io.pravega.controller;

import io.pravega.shared.metrics.MetricRegistryUtils;
import io.pravega.shared.metrics.MetricsConfig;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.StatsProvider;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import lombok.val;

/**
 * A Utility class having metrics related helping methods.
 */
public class MetricsTestUtil {

    /**
     * Initialize MetricsConfig.
     * @return  StatsProvider.
     */
    public static  StatsProvider getInitializedStatsProvider() {
        //Start Metrics service
        StatsProvider statsProvider = null;
        MetricsConfig metricsConfig = MetricsConfig.builder()
                                                   .with(MetricsConfig.ENABLE_STATISTICS, true)
                                                   .with(MetricsConfig.ENABLE_STATSD_REPORTER, false)
                                                   .build();
        metricsConfig.setDynamicCacheEvictionDuration(Duration.ofSeconds(60));
        MetricsProvider.initialize(metricsConfig);
        statsProvider = MetricsProvider.getMetricsProvider();
        statsProvider.startWithoutExporting();
        return statsProvider;
    }

    /**
     * Get time in milliseconds.
     * @param timerName    The timerName.
     * @return             The time in millis.
     */
    public static long getTimerMillis(String timerName) {
        val timer = MetricRegistryUtils.getTimer(timerName);
        return (long) timer.totalTime(TimeUnit.MILLISECONDS);
    }
}

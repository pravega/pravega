package io.pravega.controller;

import io.pravega.shared.metrics.MetricRegistryUtils;
import io.pravega.shared.metrics.MetricsConfig;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.StatsProvider;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import lombok.val;

public class MetricsTestUtil {

    /**
     * Start Metrics service
     * @return StatsProvider
     */
    public static  StatsProvider getInitialisedStatsProvider(){
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

    public static long getTimerMillis(String timerName) {
        val timer = MetricRegistryUtils.getTimer(timerName);
        return (long) timer.totalTime(TimeUnit.MILLISECONDS);
    }


}

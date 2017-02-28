/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.common.metrics;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.google.common.base.Strings;
import com.readytalk.metrics.StatsDReporter;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.GuardedBy;

import lombok.RequiredArgsConstructor;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class YammerStatsProvider implements StatsProvider {
    @GuardedBy("$lock")
    private MetricRegistry metrics = null;
    private final List<ScheduledReporter> reporters = new ArrayList<ScheduledReporter>();
    private final MetricsConfig conf;

    @Synchronized
    void init() {
        if (metrics == null) {
            metrics = MetricsProvider.YAMMERMETRICS;
            metrics.registerAll(new MemoryUsageGaugeSet());
            metrics.registerAll(new GarbageCollectorMetricSet());
        }
    }

    @Synchronized
    public MetricRegistry getMetrics() {
        return metrics;
    }

    @Synchronized
    @Override
    public void start() {
        init();

        int metricsOutputFrequency = conf.getStatsOutputFrequency();
        String prefix = conf.getMetricsPrefix();
        String csvDir = conf.getCSVEndpoint();
        String statsDHost = conf.getStatsDHost();
        Integer statsDPort = conf.getStatsDPort();

        if (!Strings.isNullOrEmpty(csvDir)) {
            // NOTE:  metrics output files are exclusive to a given process
            File outdir;
            if (!Strings.isNullOrEmpty(prefix)) {
                outdir = new File(csvDir, prefix);
            } else {
                outdir = new File(csvDir);
            }
            outdir.mkdirs();
            log.info("Configuring stats with csv output to directory [{}]", outdir.getAbsolutePath());
            reporters.add(CsvReporter.forRegistry(getMetrics())
                          .convertRatesTo(TimeUnit.SECONDS)
                          .convertDurationsTo(TimeUnit.MILLISECONDS)
                          .build(outdir));
        }
        if (!Strings.isNullOrEmpty(statsDHost)) {
            log.info("Configuring stats with statsD at: {} {}", statsDHost, statsDPort);
            reporters.add(StatsDReporter.forRegistry(getMetrics())
                          .build(statsDHost, statsDPort));
        }

        for (ScheduledReporter r : reporters) {
            r.start(metricsOutputFrequency, TimeUnit.SECONDS);
        }
    }

    @Synchronized
    @Override
    public void close() {
        for (ScheduledReporter r : reporters) {
            try {
                r.report();
                r.stop();
            } catch (Exception e) {
                log.error("Exception report or stop reporter", e);
            }
        }
    }

    @Override
    public StatsLogger createStatsLogger(String name) {
        init();
        return new YammerStatsLogger(getMetrics(), name);
    }

    @Override
    public DynamicLogger createDynamicLogger() {
        init();
        return new YammerDynamicLogger(conf, metrics, new YammerStatsLogger(getMetrics(), "DYNAMIC"));
    }
}

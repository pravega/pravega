/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.common.metrics;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.ganglia.GangliaReporter;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.google.common.base.Strings;
import com.readytalk.metrics.StatsDReporter;
import info.ganglia.gmetric4j.gmetric.GMetric;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
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
    private MetricRegistry metrics = MetricsProvider.YAMMERMETRICS;
    private final List<ScheduledReporter> reporters = new ArrayList<ScheduledReporter>();
    private final MetricsConfig conf;

    @Synchronized
    void init() {
        // I'm not entirely sure that re-inserting is necessary, but given that
        // at this point we are preserving the registry, it seems safer to remove
        // and re-insert.
        MemoryUsageGaugeSet memoryGaugeNames = new MemoryUsageGaugeSet();
        GarbageCollectorMetricSet gcMetricSet = new GarbageCollectorMetricSet();

        memoryGaugeNames.getMetrics().forEach((key, value) -> {
            metrics.remove(key);
        });

        gcMetricSet.getMetrics().forEach((key, value) -> {
           metrics.remove(key);
        });

        metrics.registerAll(new MemoryUsageGaugeSet());
        metrics.registerAll(new GarbageCollectorMetricSet());
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
        String graphiteHost = conf.getGraphiteHost();
        Integer graphitePort = conf.getGraphitePort();
        String jmxDomain = conf.getJMXDomain();
        String gangliaHost = conf.getGangliaHost();
        Integer gangliaPort = conf.getGangliaPort();
        boolean enableConsole = conf.enableConsoleReporter();

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
            log.info("Configuring stats with statsD at {}:{}", statsDHost, statsDPort);
            reporters.add(StatsDReporter.forRegistry(getMetrics())
                          .build(statsDHost, statsDPort));
        }
        if (!Strings.isNullOrEmpty(graphiteHost)) {
            log.info("Configuring stats with graphite at {}:{}", graphiteHost, graphitePort);
            final Graphite graphite = new Graphite(new InetSocketAddress(graphiteHost, graphitePort));
            reporters.add(GraphiteReporter.forRegistry(getMetrics())
                .prefixedWith(prefix)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(MetricFilter.ALL)
                .build(graphite));
        }
        if (!Strings.isNullOrEmpty(jmxDomain)) {
            log.info("Configuring stats with jmx");
            final JmxReporter jmx = JmxReporter.forRegistry(getMetrics())
                .inDomain(jmxDomain)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
            jmx.start();
        }
        if (!Strings.isNullOrEmpty(gangliaHost)) {
            try {
                log.info("Configuring stats with ganglia at {}:{}", gangliaHost, gangliaPort);
                final GMetric ganglia = new GMetric(gangliaHost, gangliaPort, GMetric.UDPAddressingMode.MULTICAST, 1);
                reporters.add(GangliaReporter.forRegistry(getMetrics())
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build(ganglia));
            } catch (IOException e) {
                log.warn("ganglia create failure: {}", e);
            }
        }
        if (enableConsole) {
            log.info("Configuring console reporter");
            reporters.add(ConsoleReporter.forRegistry(getMetrics())
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build());
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

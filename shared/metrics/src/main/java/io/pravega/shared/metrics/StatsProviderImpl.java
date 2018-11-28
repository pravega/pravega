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
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.readytalk.metrics.StatsDReporter;
import info.ganglia.gmetric4j.gmetric.GMetric;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class StatsProviderImpl implements StatsProvider {
    @Getter
    private final MetricRegistry metrics = MetricsProvider.METRIC_REGISTRY;
    private final List<ScheduledReporter> reporters = new ArrayList<ScheduledReporter>();
    private final MetricsConfig conf;

    StatsProviderImpl(MetricsConfig conf) {
        this.conf = Preconditions.checkNotNull(conf, "conf");
    }

    @Synchronized
    private void init() {
        // I'm not entirely sure that re-inserting is necessary, but given that
        // at this point we are preserving the registry, it seems safer to remove
        // and re-insert.
        MemoryUsageGaugeSet memoryGaugeNames = new MemoryUsageGaugeSet();
        GarbageCollectorMetricSet gcMetricSet = new GarbageCollectorMetricSet();

        memoryGaugeNames.getMetrics().forEach((key, value) -> metrics.remove(key));
        gcMetricSet.getMetrics().forEach((key, value) -> metrics.remove(key));

        metrics.registerAll(new MemoryUsageGaugeSet());
        metrics.registerAll(new GarbageCollectorMetricSet());
    }

    @Synchronized
    @Override
    public void start() {
        init();
        log.info("Metrics prefix: {}", conf.getMetricsPrefix());

        if (conf.isEnableCSVReporter()) {
            // NOTE:  metrics output files are exclusive to a given process
            File outdir;
            if (!Strings.isNullOrEmpty(conf.getMetricsPrefix())) {
                outdir = new File(conf.getCsvEndpoint(), conf.getMetricsPrefix());
            } else {
                outdir = new File(conf.getCsvEndpoint());
            }
            outdir.mkdirs();
            log.info("Configuring stats with csv output to directory [{}]", outdir.getAbsolutePath());
            reporters.add(CsvReporter.forRegistry(getMetrics())
                          .convertRatesTo(TimeUnit.SECONDS)
                          .convertDurationsTo(TimeUnit.MILLISECONDS)
                          .build(outdir));
        }
        if (conf.isEnableStatsdReporter()) {
            log.info("Configuring stats with statsD at {}:{}", conf.getStatsDHost(), conf.getStatsDPort());
            reporters.add(StatsDReporter.forRegistry(getMetrics())
                          .build(conf.getStatsDHost(), conf.getStatsDPort()));
        }
        if (conf.isEnableGraphiteReporter()) {
            log.info("Configuring stats with graphite at {}:{}", conf.getGraphiteHost(), conf.getGraphitePort());
            final Graphite graphite = new Graphite(new InetSocketAddress(conf.getGraphiteHost(), conf.getGraphitePort()));
            reporters.add(GraphiteReporter.forRegistry(getMetrics())
                .prefixedWith(conf.getMetricsPrefix())
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(MetricFilter.ALL)
                .build(graphite));
        }
        if (conf.isEnableJMXReporter()) {
            log.info("Configuring stats with jmx {}", conf.getJmxDomain());
            final JmxReporter jmx = JmxReporter.forRegistry(getMetrics())
                .inDomain(conf.getJmxDomain())
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
            jmx.start();
        }
        if (conf.isEnableGangliaReporter()) {
            try {
                log.info("Configuring stats with ganglia at {}:{}", conf.getGangliaHost(), conf.getGangliaPort());
                final GMetric ganglia = new GMetric(conf.getGangliaHost(), conf.getGangliaPort(), GMetric.UDPAddressingMode.MULTICAST, 1);
                reporters.add(GangliaReporter.forRegistry(getMetrics())
                    .prefixedWith(conf.getMetricsPrefix())
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build(ganglia));
            } catch (IOException e) {
                log.warn("ganglia create failure: {}", e);
            }
        }
        if (conf.isEnableConsoleReporter()) {
            log.info("Configuring console reporter");
            reporters.add(ConsoleReporter.forRegistry(getMetrics())
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build());
        }
        for (ScheduledReporter r : reporters) {
            r.start(conf.getStatsOutputFrequencySeconds(), TimeUnit.SECONDS);
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

        metrics.removeMatching(MetricFilter.ALL);
    }

    @Override
    public StatsLogger createStatsLogger(String name) {
        init();
        return new StatsLoggerImpl(getMetrics(), "pravega." + name);
    }

    @Override
    public DynamicLogger createDynamicLogger() {
        init();
        return new DynamicLoggerImpl(conf, metrics, new StatsLoggerImpl(getMetrics(), "pravega"));
    }
}

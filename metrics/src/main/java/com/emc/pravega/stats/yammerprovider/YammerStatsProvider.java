/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.emc.pravega.metrics;

import java.net.InetSocketAddress;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import static com.codahale.metrics.MetricRegistry.name;

import com.readytalk.metrics.StatsDReporter;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.io.File;
import com.google.common.base.Strings;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class YammerStatsProvider implements StatsProvider {
    MetricRegistry metrics = null;
    List<ScheduledReporter> reporters = new ArrayList<ScheduledReporter>();

    synchronized void initIfNecessary() {
        if (metrics == null) {
            metrics = new MetricRegistry();
            metrics.registerAll(new MemoryUsageGaugeSet());
            metrics.registerAll(new GarbageCollectorMetricSet());
        }
    }

    public synchronized MetricRegistry getMetrics() {
        return metrics;
    }

    @Override
    public void start(MetricsConfig conf) {
        initIfNecessary();

        int metricsOutputFrequency = conf.getStatsOutputFrequency();
        String prefix = conf.getMetricsPrefix();
        String csvDir = conf.getCSVEndpoint();
        String statsDHost = conf.getStatsDHost();
        Integer statsDPort = conf.getStatsDPort();

        if (!Strings.isNullOrEmpty(csvDir)) {
            // NOTE: 1/ metrics output files are exclusive to a given process
            // 2/ the output directory must exist
            // 3/ if output files already exist they are not overwritten and there is no metrics output
            File outdir;
            if (!Strings.isNullOrEmpty(prefix)) {
                outdir = new File(csvDir, prefix);
            } else {
                outdir = new File(csvDir);
            }
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

    @Override
    public void stop() {
        for (ScheduledReporter r : reporters) {
            r.report();
            r.stop();
        }
    }

    @Override
    public StatsLogger getStatsLogger(String name) {
        initIfNecessary();
        return new YammerStatsLogger(getMetrics(), name);
    }
}

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

import com.google.common.base.Preconditions;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;

import java.time.Duration;
import java.util.EnumMap;
import java.util.concurrent.TimeUnit;

import static io.pravega.shared.MetricsNames.failMetricName;

class OpStatsLoggerImpl implements OpStatsLogger {
    //region Members

    private final Timer success;
    private final String successName;
    private final Timer fail;
    private final String failName;
    private final MeterRegistry metricRegistry;

    //endregion

    //region Constructor

    OpStatsLoggerImpl(MeterRegistry metricRegistry, String baseName, String statName, String ... tags) {
        this.metricRegistry = Preconditions.checkNotNull(metricRegistry, "metrics");
        this.successName = baseName + "." + statName;
        this.failName = baseName + "." + failMetricName(statName); //TODO: replace the original name() method
        this.success = this.metricRegistry.timer(this.successName, tags);
        this.fail = this.metricRegistry.timer(this.failName, tags);
    }

    //endregion

    //region AutoCloseable and Finalizer Implementation

    @Override
    public void close() {
        this.metricRegistry.remove(success);
        this.metricRegistry.remove(fail);
    }


    //endregion

    //region OpStatsLogger Implementation

    @Override
    public Meter.Id getId() {
        return this.success.getId();
    }

    @Override
    public void reportFailEvent(Duration duration) {
        fail.record(duration.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void reportSuccessEvent(Duration duration) {
        success.record(duration.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void reportSuccessValue(long value) {
        // Values are inserted as millis, which is the unit they will be presented, to maintain 1:1 scale
        success.record(value, TimeUnit.MILLISECONDS);
    }

    @Override
    public void reportFailValue(long value) {
        // Values are inserted as millis, which is the unit they will be presented, to maintain 1:1 scale
        fail.record(value, TimeUnit.MILLISECONDS);
    }

    @Override
    public void clear() {
        // can't clear a timer
    }

    @Override
    public OpStatsData toOpStatsData() {
        long numFailed = fail.count();
        long numSuccess = success.count();
        HistogramSnapshot snapshot = success.takeSnapshot();
        double avgLatencyMillis = success.mean(TimeUnit.SECONDS);

        EnumMap<OpStatsData.Percentile, Long> percentileLongMap  =
                new EnumMap<>(OpStatsData.Percentile.class);
        ValueAtPercentile[] values = snapshot.percentileValues();

        //TODO: refactor OpStatsData.Percentile based on micrometer implementation
        for (OpStatsData.Percentile percent : OpStatsData.PERCENTILESET) {
            for (ValueAtPercentile vp : snapshot.percentileValues()) {
                if (vp.percentile() == percent.getValue() / 100) {
                    percentileLongMap.put(percent, (long)vp.value()); //TODO: change O(N*N) to O(N) after refactoring
                }
            }
        }
        return new OpStatsData(numSuccess, numFailed, avgLatencyMillis, percentileLongMap);
    }

    //endregion
}

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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import java.time.Duration;
import java.util.EnumMap;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;

class OpStatsLoggerImpl implements OpStatsLogger {
    //region Members

    private final Timer success;
    private final String successName;
    private final Timer fail;
    private final String failName;
    private final MetricRegistry metricRegistry;

    //endregion

    //region Constructor

    OpStatsLoggerImpl(MetricRegistry metricRegistry, String basename, String statName) {
        this.metricRegistry = Preconditions.checkNotNull(metricRegistry, "metrics");
        this.successName = name(basename, statName);
        this.failName = name(basename, statName + "-fail");
        this.success = this.metricRegistry.timer(this.successName);
        this.fail = this.metricRegistry.timer(this.failName);
    }

    //endregion

    //region AutoCloseable and Finalizer Implementation

    @Override
    public void close() {
        this.metricRegistry.remove(this.successName);
        this.metricRegistry.remove(this.failName);
    }

    @Override
    @SuppressWarnings("deprecation") // deprecated since Java 10
    protected void finalize() {
        close();
    }

    //endregion

    //region OpStatsLogger Implementation

    @Override
    public String getName() {
        return this.successName;
    }

    @Override
    public void reportFailEvent(Duration duration) {
        fail.update(duration.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void reportSuccessEvent(Duration duration) {
        success.update(duration.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void reportSuccessValue(long value) {
        // Values are inserted as millis, which is the unit they will be presented, to maintain 1:1 scale
        success.update(value, TimeUnit.MILLISECONDS);
    }

    @Override
    public void reportFailValue(long value) {
        // Values are inserted as millis, which is the unit they will be presented, to maintain 1:1 scale
        fail.update(value, TimeUnit.MILLISECONDS);
    }

    @Override
    public void clear() {
        // can't clear a timer
    }

    @Override
    public OpStatsData toOpStatsData() {
        long numFailed = fail.getCount();
        long numSuccess = success.getCount();
        Snapshot s = success.getSnapshot();
        double avgLatencyMillis = s.getMean();

        EnumMap<OpStatsData.Percentile, Long> percentileLongMap  =
                new EnumMap<OpStatsData.Percentile, Long>(OpStatsData.Percentile.class);
        for (OpStatsData.Percentile percent : OpStatsData.PERCENTILESET) {
            percentileLongMap.put(percent, (long) s.getValue(percent.getValue() / 100));
        }
        return new OpStatsData(numSuccess, numFailed, avgLatencyMillis, percentileLongMap);
    }

    //endregion
}

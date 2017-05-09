/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.metrics;

import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;

import java.time.Duration;
import java.util.EnumMap;
import java.util.concurrent.TimeUnit;

class OpStatsLoggerImpl implements OpStatsLogger {
    private final Timer success;
    private final Timer fail;

    OpStatsLoggerImpl(Timer success, Timer fail) {
        Preconditions.checkNotNull(success, "success");
        Preconditions.checkNotNull(fail, "fail");
        this.success = success;
        this.fail = fail;
    }

    // OpStatsLogger functions
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
}

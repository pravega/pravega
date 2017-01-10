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
package com.emc.pravega.common.metrics;

import com.codahale.metrics.Timer;
import com.codahale.metrics.Snapshot;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

class YammerOpStatsLogger implements OpStatsLogger {
    final Timer success;
    final Timer fail;

    YammerOpStatsLogger(Timer success, Timer fail) {
        this.success = success;
        this.fail = fail;
    }

    // OpStatsLogger functions
    @Override
    public void registerFailedEvent(long eventLatency, TimeUnit unit) {
        fail.update(eventLatency, unit);
    }

    @Override
    public void registerSuccessfulEvent(long eventLatency, TimeUnit unit) {
        success.update(eventLatency, unit);
    }

    @Override
    public void registerSuccessfulValue(long value) {
        // Values are inserted as millis, which is the unit they will be presented, to maintain 1:1 scale
        success.update(value, TimeUnit.MILLISECONDS);
    }

    @Override
    public void registerFailedValue(long value) {
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

        EnumMap<OpStatsData.Percentile, Long> percentileLongMap  = new EnumMap<OpStatsData.Percentile, Long>(OpStatsData.Percentile.class);
        EnumSet<OpStatsData.Percentile> percentileSet = EnumSet.allOf(OpStatsData.Percentile.class);
        for (OpStatsData.Percentile percent : percentileSet) {
            percentileLongMap.put(percent, (long) s.getValue(percent.getValue() / 100));
        }
        return new OpStatsData(numSuccess, numFailed, avgLatencyMillis, percentileLongMap);
    }
}

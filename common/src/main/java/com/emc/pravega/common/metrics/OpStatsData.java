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

import java.util.EnumMap;
import java.util.EnumSet;
import com.codahale.metrics.Snapshot;

/**
 * This class provides a read view of operation specific stats.
 */
public class OpStatsData {
    private final long numSuccessfulEvents, numFailedEvents;
    // All latency values are in Milliseconds.
    private final double avgLatencyMillis;

    public enum Percentile {
        P10(10),
        P50(50),
        P90(90),
        P99(99),
        P999(99.9),
        P9999(99.99);

        private double numVal;

        Percentile(double numVal) {
            this.numVal = numVal;
        }

        public double getValue() {
            return numVal;
        }
    }

    private final EnumMap<Percentile, Long> percentileLongMap = new EnumMap<Percentile, Long>(Percentile.class);

    public OpStatsData(long numSuccessfulEvents, long numFailedEvents,
                       double avgLatencyMillis, Snapshot snapshot) {
        assert numSuccessfulEvents >= 0;
        assert numFailedEvents >= 0;
        assert avgLatencyMillis >= 0;
        this.numSuccessfulEvents = numSuccessfulEvents;
        this.numFailedEvents = numFailedEvents;
        this.avgLatencyMillis = avgLatencyMillis;

        EnumSet<Percentile> percentileSet = EnumSet.allOf(Percentile.class);
        for (Percentile percent : percentileSet) {
            percentileLongMap.put(percent, (long) snapshot.getValue(percent.getValue() / 100));
        }
    }

    public long getPercentile(Percentile percentile) {
        return  percentileLongMap.get(percentile);
    }

    public long getNumSuccessfulEvents() {
        return this.numSuccessfulEvents;
    }

    public long getNumFailedEvents() {
        return this.numFailedEvents;
    }

    public double getAvgLatencyMillis() {
        return this.avgLatencyMillis;
    }
}

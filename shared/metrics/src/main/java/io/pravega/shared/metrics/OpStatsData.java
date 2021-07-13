/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.shared.metrics;

import com.google.common.base.Preconditions;

import java.util.EnumMap;
import java.util.EnumSet;

/**
 * This class provides a read view of operation specific stats.
 */
public class OpStatsData {
    static final EnumSet<Percentile> PERCENTILE_SET = EnumSet.allOf(Percentile.class);
    // Added for Micrometer's implementation, PERCENTILE_ARRAY is based on PERCENTILE_SET without scale 100.
    static final double[] PERCENTILE_ARRAY = {0.1, 0.5, 0.9, 0.99, 0.999, 0.9999};
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

        private final double numVal;

        Percentile(double numVal) {
            this.numVal = numVal;
        }

        public double getValue() {
            return numVal;
        }
    }

    private final EnumMap<Percentile, Long> percentileLongMap;

    public OpStatsData(long numSuccessfulEvents, long numFailedEvents,
                       double avgLatencyMillis, EnumMap<Percentile, Long> percentileLongMap) {
        Preconditions.checkArgument(numSuccessfulEvents >= 0, "numSuccessfulEvents must be non-negative number.");
        Preconditions.checkArgument(numFailedEvents >= 0, "numFailedEvents must be non-negative number.");
        Preconditions.checkArgument(avgLatencyMillis >= 0, "avgLatencyMillis must be non-negative number.");

        this.numSuccessfulEvents = numSuccessfulEvents;
        this.numFailedEvents = numFailedEvents;
        this.avgLatencyMillis = avgLatencyMillis;
        this.percentileLongMap = percentileLongMap;
    }

    public long getPercentile(Percentile percentile) {
        return percentileLongMap.getOrDefault(percentile, -1L);
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

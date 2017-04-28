/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.shared.metrics;

import com.google.common.base.Preconditions;

import java.util.EnumMap;
import java.util.EnumSet;

/**
 * This class provides a read view of operation specific stats.
 */
public class OpStatsData {
    static final EnumSet<Percentile> PERCENTILESET = EnumSet.allOf(Percentile.class);
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

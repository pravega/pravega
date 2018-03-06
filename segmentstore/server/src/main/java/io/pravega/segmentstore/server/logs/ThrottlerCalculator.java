/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.logs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.MathHelpers;
import io.pravega.segmentstore.storage.QueueStats;
import java.util.function.Supplier;

/**
 * Helper class that provides methods for calculating various OperationProcessor delays, to be used for batching operations
 * or throttling requests.
 */
class ThrottlerCalculator {
    @VisibleForTesting
    static final int MAX_BATCHING_DELAY_MILLIS = 50;
    @VisibleForTesting
    static final int MAX_THROTTLING_DELAY_MILLIS = 10000;
    @VisibleForTesting
    static final double MAX_CACHE_UTILIZATION = 1.25; // 125% of the cache capacity.

    private final Supplier<QueueStats> getQueueStats;
    private final Supplier<Double> getCacheUtilization;

    /**
     * Creates a new instance of the ThrottlerCalculator class.
     *
     * @param getQueueStats       A Supplier that, when invoked, returns a QueueStats object representing the most recent
     *                            statistics about the DurableDataLog write queue.
     * @param getCacheUtilization A Supplier that, when invoked, returns a non-negative number representing the cache
     *                            utilization (calculated as the ratio of used cache to total cache available). May be
     *                            greater than 1 if too much cache is used (and thus throttling is required).
     */
    ThrottlerCalculator(Supplier<QueueStats> getQueueStats, Supplier<Double> getCacheUtilization) {
        this.getQueueStats = Preconditions.checkNotNull(getQueueStats, "getQueueStats");
        this.getCacheUtilization = Preconditions.checkNotNull(getCacheUtilization, "getCacheUtilization");
    }

    /**
     * Determines whether there is a need to introduce a throttling delay. If this method returns false, then
     * getThrottlingDelayMillis() will return 0.
     *
     * @return True if throttling is required, false otherwise.
     */
    boolean isThrottlingRequired() {
        return this.getCacheUtilization.get() > 1;
    }

    /**
     * Calculates the amount of time to wait before processing more operations from the queue in order to relieve pressure
     * on downstream components. This is based on ReadIndex statistics, mainly cache utilization ratio.
     *
     * @return The amount of time, in milliseconds. If isThrottlingRequired() returns false, this will return 0, otherwise
     * it will return a positive value.
     */
    int getThrottlingDelayMillis() {
        double cacheUtilization = this.getCacheUtilization.get();
        if (cacheUtilization <= 1) {
            // Cache utilization is below 100%. No need for throttling.
            return 0;
        } else {
            // We are exceeding the cache capacity. Start throttling proportionally to the amount of excess, but make sure
            // we don't exceed the max allowed.
            return (int) (Math.min(1.0, (cacheUtilization - 1) / (MAX_CACHE_UTILIZATION - 1)) * MAX_THROTTLING_DELAY_MILLIS);
        }
    }

    /**
     * Calculates the amount of time to wait before processing more operations from the queue in order to aggregate them
     * into larger writes. This is based on statistics from the DurableDataLog.
     *
     * @return The amount of time, in milliseconds.
     */
    int getBatchingDelayMillis() {
        QueueStats stats = this.getQueueStats.get();

        // The higher the average fill rate, the more efficient use we make of the available capacity. As such, for high
        // fill ratios we don't want to wait too long.
        double fillRatioAdj = MathHelpers.minMax(1 - stats.getAverageItemFillRatio(), 0, 1);

        // Finally, we use the the ExpectedProcessingTime to give us a baseline as to how long items usually take to process.
        int delayMillis = (int) Math.round(stats.getExpectedProcessingTimeMillis() * fillRatioAdj);
        return Math.min(delayMillis, MAX_BATCHING_DELAY_MILLIS);
    }
}

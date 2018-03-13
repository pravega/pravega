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
import java.util.List;
import java.util.function.Supplier;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Singular;

/**
 * Helper class that provides methods for calculating various OperationProcessor delays, to be used for batching operations
 * or throttling requests.
 */
@Builder
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
class ThrottlerCalculator {
    //region Members

    /**
     * Maximum delay (millis) we are willing to introduce in order to perform batching.
     */
    @VisibleForTesting
    static final int MAX_BATCHING_DELAY_MILLIS = 50;
    /**
     * Maximum delay (millis) we are willing to introduce in order to throttle the incoming operations.
     */
    @VisibleForTesting
    static final int MAX_DELAY_MILLIS = 10000;
    /**
     * Amount of time (millis) to increase throttling by for each percentage point increase in the cache utilization (above 100%).
     */
    @VisibleForTesting
    static final double THROTTLING_MILLIS_PER_PERCENT_OVER_LIMIT = 100;

    @Singular
    private final List<Throttler> throttlers;

    //endregion

    //region Methods

    /**
     * Determines whether there is an immediate need to introduce a throttling delay.
     *
     * @return True if throttling is required, false otherwise.
     */
    boolean isThrottlingRequired() {
        for (Throttler t : this.throttlers) {
            if (t.isThrottlingRequired()) {
                return true;
            }
        }

        return false;
    }

    /**
     * Calculates the amount of time needed to delay based on the configured throttlers. The computed result is not additive,
     * as there is no benefit to adding delays from various Throttle Calculators together. For example, a cache throttling
     * delay will have increased batching as a side effect, so there's no need to include the batching one as well.
     *
     * @return A DelayResult representing the computed delay. This delay is the maximum delay as calculated by the internal
     * throttlers.
     */
    DelayResult getThrottlingDelay() {
        // These delays are not additive. There's no benefit to adding a batching delay on top of a throttling delay, since
        // a throttling delay will have increased batching as a side effect.
        int maxDelay = 0;
        boolean maximum = false;
        for (Throttler t : this.throttlers) {
            DelayResult r = t.getDelay();
            maxDelay = Math.max(maxDelay, r.durationMillis);
            maximum |= r.maximum;
        }

        return new DelayResult(maxDelay, maximum);
    }

    //endregion

    //region Throttlers

    /**
     * Defines the structure of a Throttle Calculator.
     */
    static abstract class Throttler {
        /**
         * Determines whether throttling based on this calculator is absolutely required at this moment.
         */
        abstract boolean isThrottlingRequired();

        /**
         * Calculates a throttling delay based on information available at the moment.
         */
        abstract DelayResult getDelay();
    }

    /**
     * Calculates the amount of time to wait before processing more operations from the queue in order to relieve pressure
     * on the cache. This is based on ReadIndex statistics, mainly cache utilization ratio.
     */
    @RequiredArgsConstructor
    private static class CacheThrottler extends Throttler {
        private final Supplier<Double> getCacheUtilization;

        @Override
        boolean isThrottlingRequired() {
            return this.getCacheUtilization.get() > 1;
        }

        @Override
        DelayResult getDelay() {
            // We only throttle if we exceed the cache capacity. We increase the throttling amount in a linear fashion.
            double cacheUtilization = this.getCacheUtilization.get();
            int delay = (int) MathHelpers.minMax((cacheUtilization - 1.0) * 100 * THROTTLING_MILLIS_PER_PERCENT_OVER_LIMIT, 0, MAX_DELAY_MILLIS);
            return new DelayResult(delay, delay >= MAX_DELAY_MILLIS);
        }
    }

    /**
     * Calculates the amount of time to wait before processing more operations from the queue in order to aggregate them
     * into larger writes. This is based on statistics from the DurableDataLog.
     */
    @RequiredArgsConstructor
    private static class BatchingThrottler extends Throttler {
        private final Supplier<QueueStats> getQueueStats;

        @Override
        boolean isThrottlingRequired() {
            // Regardless of what value we get from getDelay(), there is never an immediate need for throttling due to this reason.
            return false;
        }

        @Override
        DelayResult getDelay() {
            QueueStats stats = this.getQueueStats.get();

            // The higher the average fill rate, the more efficient use we make of the available capacity. As such, for high
            // fill ratios we don't want to wait too long.
            double fillRatioAdj = MathHelpers.minMax(1 - stats.getAverageItemFillRatio(), 0, 1);

            // Finally, we use the the ExpectedProcessingTime to give us a baseline as to how long items usually take to process.
            int delayMillis = (int) Math.round(stats.getExpectedProcessingTimeMillis() * fillRatioAdj);
            return new DelayResult(Math.min(delayMillis, MAX_BATCHING_DELAY_MILLIS), false);
        }
    }

    //endregion

    //region Builder

    /**
     * Builder for the ThrottlerCalculator class.
     */
    static class ThrottlerCalculatorBuilder {
        /**
         * Includes a Cache Throttler.
         *
         * @param getCacheUtilization A Supplier that, when invoked, returns a non-negative number representing the cache
         *                            utilization (calculated as the ratio of used cache to total cache available). May be
         *                            greater than 1 if too much cache is used (and thus throttling is required).
         * @return This builder.
         */
        ThrottlerCalculatorBuilder cacheThrottler(Supplier<Double> getCacheUtilization) {
            return throttler(new CacheThrottler(Preconditions.checkNotNull(getCacheUtilization, "getCacheUtilization")));
        }

        /**
         * Includes a Batching Throttler.
         *
         * @param getQueueStats A Supplier that, when invoked, returns a QueueStats object representing the most recent
         *                      statistics about the DurableDataLog write queue.
         * @return This builder.
         */
        ThrottlerCalculatorBuilder batchingThrottler(Supplier<QueueStats> getQueueStats) {
            return throttler(new BatchingThrottler(Preconditions.checkNotNull(getQueueStats, "getQueueStats")));
        }
    }

    //endregion

    //region DelayResult

    /**
     * The result returned by the ThrottlerCalculator.
     */
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    static class DelayResult {
        /**
         * The suggested delay, in millis.
         */
        @Getter
        private final int durationMillis;
        /**
         * Whether the suggested delay equals or exceeds the maximum defined delay (which usually means it would have been
         * higher if not capped at this value).
         */
        @Getter
        private final boolean maximum;
    }

    //endregion
}

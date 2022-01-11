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
package io.pravega.segmentstore.server.logs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.MathHelpers;
import io.pravega.segmentstore.storage.QueueStats;
import io.pravega.segmentstore.storage.WriteSettings;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
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

    @VisibleForTesting
    final int maxDelayMillis;

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
        Preconditions.checkState(this.maxDelayMillis > 0, "Max delay cannot be =< 0");
        // These delays are not additive. There's no benefit to adding a batching delay on top of a throttling delay, since
        // a throttling delay will have increased batching as a side effect.
        int maxDelay = 0;
        boolean maximum = false;
        ThrottlerName throttlerName = null;
        for (Throttler t : this.throttlers) {
            int delay = t.getDelayMillis();
            if (delay >= this.maxDelayMillis) {
                // This throttler introduced the maximum delay. No need to search more.
                maxDelay = this.maxDelayMillis;
                maximum = true;
                throttlerName = t.getName();
                break;
            }

            if (delay > maxDelay) {
                maxDelay = delay;
                throttlerName = t.getName();
            }
        }

        return new DelayResult(throttlerName, maxDelay, maximum);
    }

    private static <T, V extends Number> int calculateBaseDelay(T fullThrottleThreshold, Function<T, V> calculator, int maxDelayMillis) {
        return (int) Math.ceil(maxDelayMillis / calculator.apply(fullThrottleThreshold).doubleValue());
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
        abstract int getDelayMillis();

        /**
         * Gets a log-friendly name for this Throttle instance.
         *
         * @return Throttler name.
         */
        abstract ThrottlerName getName();
    }

    /**
     * Calculates the amount of time to wait before processing more operations from the queue in order to relieve pressure
     * on the cache. This is based on ReadIndex statistics, mainly cache utilization ratio.
     */
    private static class CacheThrottler extends Throttler {
        private final double targetCacheUtilization;
        private final int baseDelay;
        @NonNull
        private final Supplier<Double> getCacheUtilization;

        CacheThrottler(Supplier<Double> getCacheUtilization, double targetCacheUtilization, double maxCacheUtilization, int maxDelayMillis) {
            this.targetCacheUtilization = targetCacheUtilization + ThrottlerPolicy.CACHE_TARGET_UTILIZATION_THRESHOLD_ADJUSTMENT;
            this.getCacheUtilization = getCacheUtilization;
            if (this.targetCacheUtilization >= maxCacheUtilization) {
                // Since this is externally provided, we need to be able to handle invalid values. If we get too small of
                // a utilization, then we will apply maximum throttle as soon as we reach the min utilization threshold.
                this.baseDelay = maxDelayMillis;
            } else {
                this.baseDelay = calculateBaseDelay(maxCacheUtilization, this::getDelayMultiplier, maxDelayMillis);
            }
        }

        @Override
        boolean isThrottlingRequired() {
            return this.getCacheUtilization.get() > this.targetCacheUtilization;
        }

        @Override
        int getDelayMillis() {
            // We only throttle if we exceed the cache capacity. We increase the throttling amount in a linear fashion.
            return (int) (getDelayMultiplier(this.getCacheUtilization.get()) * this.baseDelay);
        }

        @Override
        ThrottlerName getName() {
            return ThrottlerName.Cache;
        }

        private double getDelayMultiplier(double utilization) {
            return 100 * (utilization - this.targetCacheUtilization);
        }
    }

    /**
     * Calculates the amount of time to wait before processing more operations from the queue in order to aggregate them
     * into larger writes. This is based on statistics from the DurableDataLog.
     */
    @RequiredArgsConstructor
    private static class BatchingThrottler extends Throttler {
        @NonNull
        private final Supplier<QueueStats> getQueueStats;
        private final int maxBatchingDelayMillis;

        @Override
        boolean isThrottlingRequired() {
            // Regardless of what value we get from getDelay(), there is never an immediate need for throttling due to this reason.
            return false;
        }

        @Override
        int getDelayMillis() {
            QueueStats stats = this.getQueueStats.get();

            // The higher the average fill rate, the more efficient use we make of the available capacity. As such, for high
            // fill ratios we don't want to wait too long.
            double fillRatioAdj = MathHelpers.minMax(1 - stats.getAverageItemFillRatio(), 0, 1);

            // Finally, we use the ExpectedProcessingTime to give us a baseline as to how long items usually take to process.
            int delayMillis = (int) Math.round(stats.getExpectedProcessingTimeMillis() * fillRatioAdj);
            return Math.min(delayMillis, this.maxBatchingDelayMillis);
        }

        @Override
        ThrottlerName getName() {
            return ThrottlerName.Batching;
        }
    }

    /**
     * Calculates the amount of time to wait before processing more operations from the queue in order to relieve pressure
     * from the DurableDataLog. This is based on static information from the DurableDataLog's {@link WriteSettings} and dynamic
     * information from its {@link QueueStats}.
     *
     * In order to perform efficient comparisons, the {@link WriteSettings} information (reported in bytes) needs to be
     * converted into the same unit as what's reported by {@link QueueStats} (number of items in the queue and average fill ratio):
     * - The max throttling threshold is obtained by dividing the {@link WriteSettings#getMaxOutstandingBytes()} by {@link WriteSettings#getMaxWriteLength()}.
     * - The min throttling threshold is a fraction of the max threshold ({@link ThrottlerPolicy#DURABLE_DATALOG_THROTTLE_THRESHOLD_FRACTION}).
     * - The adjusted queue size is obtained by multiplying {@link QueueStats#getSize()} by {@link QueueStats#getAverageItemFillRatio()}.
     *
     * This allows us to compare adjusted queue size directly with min-max throttling thresholds.
     */
    private static class DurableDataLogThrottler extends Throttler {
        private final int thresholdMillis;
        private final int baseDelay;
        private final int minThrottleThreshold;
        private final Supplier<QueueStats> getQueueStats;

        DurableDataLogThrottler(@NonNull WriteSettings writeSettings, @NonNull Supplier<QueueStats> getQueueStats, int maxDelayMillis) {
            // Calculate the latency threshold as a fraction of the WriteSettings' Max Write Timeout.
            this.thresholdMillis = (int) Math.floor(writeSettings.getMaxWriteTimeout().toMillis() * ThrottlerPolicy.DURABLE_DATALOG_THROTTLE_THRESHOLD_FRACTION);

            // Calculate max and min throttling thresholds (see Javadoc above for explanation).
            int maxThrottleThreshold = writeSettings.getMaxOutstandingBytes() / writeSettings.getMaxWriteLength();
            this.minThrottleThreshold = (int) Math.floor(maxThrottleThreshold * ThrottlerPolicy.DURABLE_DATALOG_THROTTLE_THRESHOLD_FRACTION);
            this.baseDelay = calculateBaseDelay(maxThrottleThreshold, this::getDelayMultiplier, maxDelayMillis);
            this.getQueueStats = getQueueStats;
        }

        @Override
        boolean isThrottlingRequired() {
            return isThrottlingRequired(this.getQueueStats.get());
        }

        private boolean isThrottlingRequired(QueueStats stats) {
            return stats.getExpectedProcessingTimeMillis() > this.thresholdMillis;
        }

        @Override
        int getDelayMillis() {
            QueueStats stats = this.getQueueStats.get();
            if (isThrottlingRequired(stats)) {
                // Calculate the adjusted queue size (see Javadoc above for explanation).
                int adjustedQueueSize = (int) (stats.getSize() * stats.getAverageItemFillRatio());
                return getDelayMultiplier(adjustedQueueSize) * this.baseDelay;
            }
            return 0;
        }

        private int getDelayMultiplier(int adjustedQueueSize) {
            return adjustedQueueSize - this.minThrottleThreshold;
        }

        @Override
        ThrottlerName getName() {
            return ThrottlerName.DurableDataLog;
        }
    }

    /**
     * Calculates the amount of time to wait before processing more operations from the queue in order to relieve pressure
     * from the OperationLog. This is based solely on the number of operations accumulated in the OperationLog.
     */
    @RequiredArgsConstructor
    private static class OperationLogThrottler extends Throttler {
        @NonNull
        private final Supplier<Integer> getOperationLogSize;
        private final int maxDelayMillis;
        private final int operationLogMaxSize;
        private final int operationLogTargetSize;

        @Override
        boolean isThrottlingRequired() {
            return this.getOperationLogSize.get() > this.operationLogTargetSize;
        }

        @Override
        int getDelayMillis() {
            // We only throttle if we exceed the target log size. We increase the throttling amount in a linear fashion.
            int size = this.getOperationLogSize.get();
            if (size <= this.operationLogTargetSize) {
                return 0;
            } else if (size >= this.operationLogMaxSize) {
                return this.maxDelayMillis;
            } else {
                final double sizeSpan = this.operationLogMaxSize - this.operationLogTargetSize;
                return (int) (this.maxDelayMillis * (this.getOperationLogSize.get() - this.operationLogTargetSize) / sizeSpan);
            }
        }

        @Override
        ThrottlerName getName() {
            return ThrottlerName.OperationLog;
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
         * @param getCacheUtilization    A Supplier that, when invoked, returns a non-negative number representing the cache
         *                               utilization (calculated as the ratio of used cache to total cache available).
         * @param targetCacheUtilization Target cache utilization, at or above which throttling will gradually begin to apply.
         * @param maxCacheUtilization    Maximum allowed cache utilization, at or above which full throttling will apply.
         * @return This builder.
         */
        ThrottlerCalculatorBuilder cacheThrottler(Supplier<Double> getCacheUtilization, double targetCacheUtilization,
                                                  double maxCacheUtilization, int maxDelayMillis) {
            return throttler(new CacheThrottler(getCacheUtilization, targetCacheUtilization, maxCacheUtilization, maxDelayMillis));
        }

        /**
         * Includes a Batching Throttler.
         *
         * @param getQueueStats A Supplier that, when invoked, returns a QueueStats object representing the most recent
         *                      statistics about the DurableDataLog write queue.
         * @return This builder.
         */
        ThrottlerCalculatorBuilder batchingThrottler(Supplier<QueueStats> getQueueStats, int maxBatchingDelayMillis) {
            return throttler(new BatchingThrottler(getQueueStats, maxBatchingDelayMillis));
        }

        ThrottlerCalculatorBuilder durableDataLogThrottler(WriteSettings writeSettings, Supplier<QueueStats> getQueueStats,
                                                           int maxDelayMillis) {
            return throttler(new DurableDataLogThrottler(writeSettings, getQueueStats, maxDelayMillis));
        }

        ThrottlerCalculatorBuilder operationLogThrottler(Supplier<Integer> getDurableLogSize, int maxDelayMillis,
                                                         int operationLogMaxSize, int operationLogTargetSize) {
            return throttler(new OperationLogThrottler(getDurableLogSize, maxDelayMillis, operationLogMaxSize, operationLogTargetSize));
        }
    }

    //endregion

    //region DelayResult

    /**
     * The result returned by the ThrottlerCalculator.
     */
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    @Getter
    static class DelayResult {
        /**
         * The name of the throttler inducing this delay.
         */
        private final ThrottlerName throttlerName;
        /**
         * The suggested delay, in millis.
         */
        private final int durationMillis;
        /**
         * Whether the suggested delay equals or exceeds the maximum defined delay (which usually means it would have been
         * higher if not capped at this value).
         */
        private final boolean maximum;

        /**
         * Creates a new {@link DelayResult} with the same information as this instance but the new given value as duration.
         *
         * @param durationMillis The new duration value, in milliseconds.
         * @return A new {@link DelayResult} object.
         */
        DelayResult withNewDelay(int durationMillis) {
            return new DelayResult(this.throttlerName, durationMillis, false);
        }

        @Override
        public String toString() {
            return String.format("%dms (Max=%s, Reason=%s)", this.durationMillis, this.maximum, this.throttlerName);
        }
    }

    //endregion

    //region ThrottlerName

    /**
     * Defines Throttler Names.
     */
    @Getter
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    enum ThrottlerName {
        /**
         * Throttling is required in order to aggregate multiple operations together in a single write.
         */
        Batching(false),
        /**
         * Throttling is required due to excessive Cache utilization.
         */
        Cache(true),
        /**
         * Throttling is required due to excessive size of DurableDataLog's in-flight queue.
         */
        DurableDataLog(true),
        /**
         * Throttling is required due to excessive accumulated Operations in OperationLog (not yet truncated).
         */
        OperationLog(true);

        @Getter
        private final boolean interruptible;
    }

    //endregion
}

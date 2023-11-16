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
package io.pravega.segmentstore.storage.chunklayer;

import com.google.common.util.concurrent.AtomicDouble;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.pravega.shared.MetricsNames.SLTS_HEALTH_SLOW_PERCENTAGE;
import static io.pravega.shared.MetricsNames.SLTS_STORAGE_USED_BYTES;
import static io.pravega.shared.MetricsNames.SLTS_STORAGE_USED_PERCENTAGE;
import static io.pravega.shared.NameUtils.isSegmentInSystemScope;

/**
 * Tracks the health status of the {@link ChunkedSegmentStorage}.
 */
@Slf4j
@RequiredArgsConstructor
class StorageHealthTracker implements  StatsReporter {
    private static final int PARALLEL_OP_MULTIPLIER = 1;
    private static final int EXCLUSIVE_OP_MULTIPLIER = 1;
    private static final int GC_OP_MULTIPLIER = 1;
    /**
     * Tracks whether storage is slow.
     */
    private final AtomicBoolean isStorageDegraded = new AtomicBoolean(false);

    /**
     * Tracks whether storage should be throttled.
     */
    private final AtomicBoolean shouldThrottle = new AtomicBoolean(false);

    /**
     * Tracks whether storage is unavailable.
     */
    private final AtomicBoolean isStorageUnavailable = new AtomicBoolean(false);

    /**
     * Tracks whether storage is full.
     */
    private final AtomicBoolean isStorageFull = new AtomicBoolean(false);

    /**
     * Tracks percentage late requests in last iteration.
     */
    private final AtomicDouble percentageLate = new AtomicDouble(0);

    /**
     * Tracks storage used in bytes.
     */
    private final AtomicLong storageUsed = new AtomicLong(0);

    /**
     * Tracks number of late requests in current iteration.
     */
    private final AtomicLong lateRequestCount = new AtomicLong();

    /**
     * Tracks number of late requests in current iteration.
     */
    private final AtomicLong completedRequestCount = new AtomicLong();

    /**
     * Tracks number of late requests in current iteration.
     */
    private final AtomicLong pendingRequestCount = new AtomicLong();

    /**
     * Tracks number of unavailable requests in current iteration.
     */
    private final AtomicLong unavailableRequestCount = new AtomicLong();

    /**
     * Tracks number of iterations experiencing unavailable requests .
     */
    private final AtomicInteger unavailableIterationCount = new AtomicInteger();

    /**
     * Container Id.
     */
    private final long containerId;

    /**
     * {@link ChunkedSegmentStorageConfig} instance to use.
     */
    @NonNull
    private final ChunkedSegmentStorageConfig config;

    /**
     * Function that supplies current time.
     */
    @NonNull
    private final Supplier<Long> currentTimeSupplier;

    /**
     * Function that supplies delay future.
     */
    @NonNull
    private final Function<Duration, CompletableFuture<Void>> delaySupplier;

    /**
     *  Calculates Health Stats.
     */
    void calculateHealthStats() {
        // Set the new percentage
        if (completedRequestCount.get() == 0) {
            percentageLate.set(0);
        } else {
            percentageLate.set((100.0 * lateRequestCount.get()) / completedRequestCount.get());
        }

        log.debug("StorageHealthTracker[{}]: Calculating Health Stats. Completed={} Pending={} late={} unavailable={} unavailableIterations={}",
                containerId, completedRequestCount.get(), pendingRequestCount.get(), lateRequestCount.get(),
                unavailableRequestCount.get(), unavailableIterationCount.get());

        // set degraded status.
        if (percentageLate.intValue() >= config.getMaxLateThrottlePercentage()) {
            if (!isStorageDegraded.get()) {
                log.info("StorageHealthTracker[{}]: Storage is slow. {}% Requests are slow. Max {}% Min {}% allowed.",
                        containerId, percentageLate.intValue(),
                        config.getMaxLateThrottlePercentage(), config.getMinLateThrottlePercentage());
            }
            isStorageDegraded.set(true);
        } else {
            if (isStorageDegraded.get()) {
                log.info("StorageHealthTracker[{}]: Storage is not slow anymore. {}% Requests are slow. Max {}% Min {}% allowed.",
                    containerId, percentageLate.intValue(),
                    config.getMaxLateThrottlePercentage(), config.getMinLateThrottlePercentage());
            }
            isStorageDegraded.set(false);
        }

        // Set unavailable status
        if (unavailableRequestCount.get() > 0) {
            if (unavailableIterationCount.get() == 0) {
                log.info("StorageHealthTracker[{}]: Storage is unavailable.", containerId);
            }
            unavailableIterationCount.incrementAndGet();
            isStorageUnavailable.set(true);
        } else {
            if (isStorageUnavailable.get()) {
                log.info("StorageHealthTracker[{}]: Storage is available again.", containerId);
            }
            unavailableIterationCount.set(0);
            isStorageUnavailable.set(false);
        }

        // Finally, set whether we should throttle
        if (percentageLate.intValue() > config.getMinLateThrottlePercentage() || isStorageUnavailable.get()) {
            if (!shouldThrottle.get()) {
                log.info("StorageHealthTracker[{}]: Throttle is enabled. {}% Requests are slow.",
                        containerId, percentageLate.intValue());
            }
            shouldThrottle.set(true);
        } else {
            if (shouldThrottle.get()) {
                log.info("StorageHealthTracker[{}]: Throttle is disabled. {}% Requests are slow.",
                        containerId, percentageLate.intValue());
            }
            shouldThrottle.set(false);
        }

        //Finally, clear state
        lateRequestCount.set(0);
        completedRequestCount.set(0);
        unavailableRequestCount.set(0);
        lateRequestCount.set(0);
    }

    @Override
    public void report() {
        // Report storage size.
        ChunkStorageMetrics.DYNAMIC_LOGGER.reportGaugeValue(SLTS_STORAGE_USED_BYTES, getStorageUsed());
        ChunkStorageMetrics.DYNAMIC_LOGGER.reportGaugeValue(SLTS_STORAGE_USED_PERCENTAGE, getStorageUsedPercentage());
        // Report slowness percentage
        ChunkStorageMetrics.DYNAMIC_LOGGER.reportGaugeValue(SLTS_HEALTH_SLOW_PERCENTAGE, getLatePercentage());
    }

    /**
     * Method used to report a late request. Late is defined as by "storage.self.check.late" property.
     * @param latency latency to report.
     */
    void reportLate(long latency) {
        lateRequestCount.accumulateAndGet(1L, this::addValue);
    }

    /**
     * Method used to report request did not complete because storage was unavailable.
     */
    void reportUnavailable() {
        isStorageUnavailable.set(true);
        unavailableRequestCount.accumulateAndGet(1L, this::addValue);
    }

    /**
     * Method used to report request completion
     */
    void reportCompleted() {
        completedRequestCount.accumulateAndGet(1L, this::addValue);
        pendingRequestCount.accumulateAndGet(-1L, this::addValue);
    }

    /**
     * Gets the percentage of late requests in last iteration.
     * @return long between 0 and 100.
     */
    long getLatePercentage() {
        return percentageLate.intValue();
    }

    /**
     * Method used to report beginning of request.
     */
    void reportStarted() {
        pendingRequestCount.accumulateAndGet(1L, this::addValue);
    }

    /**
     * Indicates whether storage is full.
     */
    boolean isStorageFull() {
        return config.isSafeStorageSizeCheckEnabled() && isStorageFull.get();
    }

    /**
     * Sets storage full.
     */
    void setStorageFull(boolean isFull) {
        if (config.isSafeStorageSizeCheckEnabled()) {
            isStorageFull.set(isFull);
        }
    }

    /**
     * Gets value set of storage usage in bytes.
     * @return value set of storage usage in bytes.
     */
    long getStorageUsed() {
        return storageUsed.get();
    }

    /**
     * Sets value set of storage usage in bytes.
     */
    void setStorageUsed(long used) {
        storageUsed.set(used);
    }

    /**
     * Gets storage percentage used.
     * @return double between 0 and 100.
     */
    double getStorageUsedPercentage() {
        return (100.0 * storageUsed.get()) / config.getMaxSafeStorageSize();
    }

    /**
     * Gets whether storage is unavailable.
     */
    boolean isStorageUnavailable() {
        return isStorageUnavailable.get();
    }


    /**
     * Gets whether storage is degraded.
     */
    boolean isStorageDegraded() {
        return isStorageDegraded.get();
    }

    /**
     * Whether this instance is running under the safe mode or not.
     *
     * @return True if safe mode, False otherwise.
     */
    boolean isSafeMode() {
        return isStorageFull.get();
    }

    /**
     * Throttles a parallel operation if required.
     * @return A CompletableFuture that will complete after the throttle is applied.
     * @param segmentNames The names of the Segments involved in this operation.
     */
    CompletableFuture<Void> throttleParallelOperation(String... segmentNames) {
        if (!isSystemOperation(segmentNames) && (shouldThrottle.get() || isStorageUnavailable.get())) {
            long delay = 0;
            if (isStorageUnavailable.get()) {
                delay = getUnavailableThrottleDelay();
            } else if (isStorageDegraded.get()) {
                delay = config.getMaxLateThrottleDurationInMillis();
            } else if (shouldThrottle.get()) {
                delay = getParallelThrottleDelay();
            }
            ChunkStorageMetrics.SLTS_HEALTH_PARALLEL_THROTTLE.reportSuccessValue(delay);
            log.debug("StorageHealthTracker[{}]: Throttling parallel operation. Delay={}", containerId, delay);
            return delaySupplier.apply(Duration.ofMillis(delay));
        }
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Throttles an exclusive operation if required.
     * @param segmentNames The names of the Segments involved in this operation.
     * @return A CompletableFuture that will complete after the throttle is applied.
     */
    CompletableFuture<Void> throttleExclusiveOperation(String... segmentNames) {
        if (!isSystemOperation(segmentNames) && (shouldThrottle.get() || isStorageUnavailable.get())) {
            long delay = 0;
            if (isStorageUnavailable.get()) {
                delay = getUnavailableThrottleDelay();
            } else if (isStorageDegraded.get()) {
                delay = config.getMaxLateThrottleDurationInMillis();
            } else if (shouldThrottle.get()) {
                delay = getExclusiveThrottleDelay();
            }
            ChunkStorageMetrics.SLTS_HEALTH_EXCLUSIVE_THROTTLE.reportSuccessValue(delay);
            log.debug("StorageHealthTracker[{}]: Throttling exclusive operation. Delay={}", containerId, delay);
            return delaySupplier.apply(Duration.ofMillis(delay));
        }
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Throttles garbage collection batch if required.
     * @return A CompletableFuture that will complete after the throttle is applied.
     */
    CompletableFuture<Void> throttleGarbageCollectionBatch() {
        if (shouldThrottle.get() || isStorageUnavailable.get()) {
            long delay = 0;
            if (isStorageUnavailable.get()) {
                delay = getUnavailableThrottleDelay();
            } else if (isStorageDegraded.get()) {
                delay = config.getMaxLateThrottleDurationInMillis();
            } else if (shouldThrottle.get()) {
                delay = getGarbageCollectionThrottleDelay();
            }
            ChunkStorageMetrics.SLTS_HEALTH_GC_THROTTLE.reportSuccessValue(delay);
            log.debug("StorageHealthTracker[{}]: Throttling GC batch. Delay={}", containerId, delay);
            return delaySupplier.apply(Duration.ofMillis(delay));
        }
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Calculate throttle for parallel operations.
     *
     * @return Duration for throttle.
     */
    private long getParallelThrottleDelay() {
        return PARALLEL_OP_MULTIPLIER * getLinearlyProportionalDelay();
    }

    /**
     *  Calculate delay for exclusive operations.
     *
     * @return long Duration in milliseconds for throttle.
     */
    private long getExclusiveThrottleDelay() {
        return EXCLUSIVE_OP_MULTIPLIER * getLinearlyProportionalDelay();
    }

    /**
     *  Calculate throttle for garbage collection operations.
     *
     * @return long Duration in milliseconds for throttle.
     */
    private long getGarbageCollectionThrottleDelay() {
        return GC_OP_MULTIPLIER * getLinearlyProportionalDelay();
    }

    /**
     *  Calculate throttle when storage is unavailable.
     *
     * @return long Duration in milliseconds for throttle.
     */
    long getUnavailableThrottleDelay() {
        val multiplier = Math.max(1, unavailableIterationCount.get());
        return  multiplier * config.getMaxLateThrottleDurationInMillis();
    }

    /**
     * Get linearly proportional delay.
     * Duration is between min and max throttle delay and value is proportional to percentage late requests in most recent iterations.
     *
     * @return long linearly proportional delay for throttle
     */
    private long getLinearlyProportionalDelay() {
        val maxRange = config.getMaxLateThrottleDurationInMillis() - config.getMinLateThrottleDurationInMillis();
        val millis = config.getMinLateThrottleDurationInMillis() + (percentageLate.doubleValue() * maxRange / 100);
        return Math.round(millis);
    }

    private long addValue(long incValueParam, long value) {
        return incValueParam + value;
    }

    private boolean isSystemOperation(String... segmentNames) {
        return segmentNames.length >= 1 && isSegmentInSystemScope(segmentNames[0]);
    }
}

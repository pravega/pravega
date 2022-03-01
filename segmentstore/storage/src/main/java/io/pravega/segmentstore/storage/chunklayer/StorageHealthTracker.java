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
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.pravega.shared.NameUtils.isSegmentInSystemScope;

/**
 * Tracks the health status of the {@link ChunkedSegmentStorage}.
 */
@Slf4j
@RequiredArgsConstructor
class StorageHealthTracker {
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
     * Container Id.
     */
    private final long containerId;

    /**
     * {@link ChunkedSegmentStorageConfig} instance to use.
     */
    @NonNull
    @Getter
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
     * Indicates a beginning of the iteration or batch of operations.
     *
     * @param iterationId identifier of iteration.
     */
    public void beginIteration(long iterationId) {
        // Clear
        lateRequestCount.set(0);
        completedRequestCount.set(0);
        unavailableRequestCount.set(0);
        lateRequestCount.set(0);
    }

    /**
     * Indicates an end of the iteration or batch of operations.
     *
     * @param iterationId identifier of iteration.
     */
    public void endIteration(long iterationId) {
        // Set the new percentage
        if (completedRequestCount.get() == 0) {
            percentageLate.set(0);
        } else {
            percentageLate.set((100.0 * lateRequestCount.get()) / completedRequestCount.get());
        }

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

        if (unavailableRequestCount.get() > 0) {
            if (!isStorageUnavailable.get()) {
                log.info("StorageHealthTracker[{}]: Storage is unavailable.");
            }
            isStorageUnavailable.set(true);
        } else {
            if (isStorageUnavailable.get()) {
                log.info("StorageHealthTracker[{}]: Storage is available again.");
            }
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
    long getPercentLate() {
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
        if (!isSystemOperation(segmentNames)) {
            if (isStorageUnavailable.get()) {
                return delaySupplier.apply(Duration.ofMillis(config.getMaxLateThrottleDurationInMillis()));
            }
            if (isStorageDegraded.get()) {
                return delaySupplier.apply(Duration.ofMillis(config.getMaxLateThrottleDurationInMillis()));
            }
            if (shouldThrottle.get()) {
                return delaySupplier.apply(getParallelThrottleDelay());
            }
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
            if (isStorageUnavailable.get() || isStorageDegraded.get()) {
                return delaySupplier.apply(Duration.ofMillis(config.getMaxLateThrottleDurationInMillis()));
            }
            if (shouldThrottle.get()) {
                return delaySupplier.apply(getExclusiveThrottleDelay());
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Throttles garbage collection batch if required.
     * @return A CompletableFuture that will complete after the throttle is applied.
     */
    CompletableFuture<Void> throttleGarbageCollectionBatch() {
        if (isStorageUnavailable.get() || isStorageDegraded.get()) {
            return delaySupplier.apply(Duration.ofMillis(config.getMaxLateThrottleDurationInMillis()));
        }
        if (shouldThrottle.get()) {
            return delaySupplier.apply(getGarbageCollectionThrottleDelay());
        }
        return CompletableFuture.completedFuture(null);
    }

    Duration getParallelThrottleDelay() {
        var millis = config.getMinLateThrottleDurationInMillis() + (percentageLate.doubleValue() * (config.getMaxLateThrottleDurationInMillis() - config.getMinLateThrottleDurationInMillis())) / 100;
        return Duration.ofMillis(Math.round(millis));
    }

    Duration getExclusiveThrottleDelay() {
        var millis = config.getMinLateThrottleDurationInMillis() + (percentageLate.doubleValue() * (config.getMaxLateThrottleDurationInMillis() - config.getMinLateThrottleDurationInMillis())) / 100;
        return Duration.ofMillis(Math.round(millis));
    }

    Duration getGarbageCollectionThrottleDelay() {
        var millis = config.getMinLateThrottleDurationInMillis() + (percentageLate.doubleValue() * (config.getMaxLateThrottleDurationInMillis() - config.getMinLateThrottleDurationInMillis())) / 100;
        return Duration.ofMillis(Math.round(millis));
    }

    private long addValue(long incValueParam, long value) {
        return incValueParam + value;
    }

    private boolean isSystemOperation(String... segmentNames) {
        return segmentNames.length >= 1 && isSegmentInSystemScope(segmentNames[0]);
    }
}

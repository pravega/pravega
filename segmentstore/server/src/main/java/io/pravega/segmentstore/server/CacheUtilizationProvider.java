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
package io.pravega.segmentstore.server;

import io.pravega.segmentstore.storage.ThrottleSourceListener;
import io.pravega.segmentstore.storage.ThrottlerSourceListenerCollection;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Defines an object that can provide information about the Cache utilization, as well as keep track of pending insertion bytes.
 */
@Slf4j
@ThreadSafe
public class CacheUtilizationProvider {
    //region Members

    private final CachePolicy policy;
    private final Supplier<Long> getCacheStoredBytes;
    private final ThrottlerSourceListenerCollection cleanupListeners;
    private final AtomicLong pendingBytes;
    private final double utilizationSpread;
    private final double maxInsertCapacityThreshold;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the {@link CacheUtilizationProvider} class.
     *
     * @param policy              The {@link CachePolicy} to use.
     * @param getCacheStoredBytes A {@link Supplier} that, when invoked, returns the number of stored bytes in the cache.
     */
    CacheUtilizationProvider(@NonNull CachePolicy policy, @NonNull Supplier<Long> getCacheStoredBytes) {
        this.policy = policy;
        this.getCacheStoredBytes = getCacheStoredBytes;
        this.cleanupListeners = new ThrottlerSourceListenerCollection();
        this.pendingBytes = new AtomicLong();
        this.utilizationSpread = 2 * (policy.getMaxUtilization() - policy.getTargetUtilization());
        this.maxInsertCapacityThreshold = policy.getMaxUtilization() - this.utilizationSpread;
    }

    //endregion

    //region Operations

    /**
     * Updates the pending byte counter by the given delta amount.
     *
     * @param delta The amount of bytes (positive or negative) to adjust by.
     */
    public void adjustPendingBytes(long delta) {
        if (delta < 0) {
            this.pendingBytes.updateAndGet(p -> Math.max(0, p + delta));
        } else if (delta > 0) {
            this.pendingBytes.addAndGet(delta);
        }
    }

    /**
     * Gets the number of pending bytes (that are about to be inserted into the cache, but not part of it yet).
     *
     * @return The number of pending bytes.
     */
    long getPendingBytes() {
        return this.pendingBytes.get();
    }

    /**
     * Gets a value representing the current cache utilization, as a ratio of cache used to cache max size.
     * * A value of 0 indicates that the cache is empty (or almost empty).
     * * A value in the interval (0,1) indicates the cache is used, but not full.
     * * A value of 1 indicates that the cache is used at 100% capacity.
     * * A value greater than 1 indicates the cache exceeds it allocated capacity.
     *
     * @return The cache utilization.
     */
    public double getCacheUtilization() {
        // We use the total number of used bytes, which includes any overhead. This will provide a more accurate
        // representation of the utilization than just the Stored Bytes.
        return (double) (this.getCacheStoredBytes.get() + this.pendingBytes.get()) / this.policy.getMaxSize();
    }

    /**
     * Gets a value representing the target utilization of the cache, as a ratio of cache used to cache max size.
     * The cache should be kept at or below this level. Any utilization above this limit should cause throttling and/or
     * cache eviction to occur.
     *
     * See {@link #getCacheUtilization()} for more details.
     *
     * @return The maximum cache utilization.
     */
    public double getCacheTargetUtilization() {
        return this.policy.getTargetUtilization();
    }

    /**
     * Gets a value representing the maximum allowed utilization of the cache, as a ratio of cache used to cache max size.
     * Any utilization above this limit should cause both full throttling and cache eviction to occur.
     *
     * See {@link #getCacheUtilization()} for more details.
     *
     * @return The maximum cache utilization.
     */
    public double getCacheMaxUtilization() {
        return this.policy.getMaxUtilization();
    }

    /**
     * Calculates a value that indicates the capacity of the cache for accepting new insertions. To determine this, both
     * the actual Cache capacity and the number of pending bytes ({@link #getPendingBytes()} are included.
     *
     * How to interpret the result:
     * - A smaller capacity value indicates the cache is closer to its max capacity and inserting a new entry is more
     * likely to result in CacheFullException immediately or shortly in the future.
     * - A value of 0 indicates the cache is at or beyond its max configured capacity so inserting a new entry is very
     * likely to result in a CacheFullException immediately or shortly in the future.
     * - A value of 1 indicates the cache has sufficient capacity to handle a large insertion without the risk of filling
     * it up.
     *
     * @return A value in the interval [0, 1] that indicates the capacity of the cache to accept new insertions.
     */
    public double getCacheInsertionCapacity() {
        double capacity = getCacheUtilization();
        if (capacity > this.policy.getMaxUtilization()) {
            // No capacity for further insertion.
            return 0;
        } else if (capacity > this.maxInsertCapacityThreshold) {
            // Remap on the interval [maxInsertCapacityThreshold, MaxUtilization].
            capacity = (capacity - this.maxInsertCapacityThreshold) / this.utilizationSpread;

            // Invert: the closer to MaxUtilization, the less capacity we have.
            capacity = 1 - capacity;
            return capacity;
        } else {
            return 1;
        }
    }

    /**
     * Registers the given {@link ThrottleSourceListener}, which will be notified of all subsequent Cache Cleanup events that
     * result in at least one entry being evicted from the cache.
     *
     * @param listener The {@link ThrottleSourceListener} to register. This will be auto-unregistered on the first Cache Cleanup
     *                 run that detects {@link ThrottleSourceListener#isClosed()} to be true.
     */
    public void registerCleanupListener(@NonNull ThrottleSourceListener listener) {
        this.cleanupListeners.register(listener);
    }

    /**
     * Notifies any registered {@link ThrottleSourceListener} (via {@link #registerCleanupListener}) that a Cache Cleanup
     * event has just completed.
     */
    void notifyCleanupListeners() {
        this.cleanupListeners.notifySourceChanged();
    }

    //endregion
}

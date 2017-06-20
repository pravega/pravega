/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.bookkeeper;

import com.google.common.base.Preconditions;
import io.pravega.common.AbstractTimer;
import io.pravega.common.MathHelpers;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.RequiredArgsConstructor;

/**
 * Manages the Write Queue Concurrency.
 */
@NotThreadSafe
class ConcurrencyManager {
    //region Members

    private static final double SIGNIFICANT_DIFFERENCE = 0.1;
    private static final long MIN_FREQUENCY_MILLIS = 1000;
    private static final long STALE_MILLIS = MIN_FREQUENCY_MILLIS * 4;
    private final int minParallelism;
    private final int maxParallelism;
    private final WriteQueue queue;
    private Snapshot lastSnapshot;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ConcurrencyManager class.
     *
     * @param queue          The WriteQueue to attach to.
     * @param minParallelism The minimum degree of parallelism desired.
     * @param maxParallelism The maximum degree of parallelism desired.
     */
    ConcurrencyManager(WriteQueue queue, int minParallelism, int maxParallelism) {
        this.queue = Preconditions.checkNotNull(queue, "queue");
        Preconditions.checkArgument(minParallelism <= maxParallelism, "minParallelism must be <= maxParallelism.");
        this.minParallelism = minParallelism;
        this.maxParallelism = maxParallelism;
        resetSnapshot();
    }

    //endregion

    //region Operations

    /**
     * Gets a value indicating the current (most recent) degree of parallelism.
     *
     * @return The result
     */
    int getCurrentParallelism() {
        return this.lastSnapshot.parallelism;
    }

    /**
     * Updates the degree of parallelism (if needed), based on information collected since the last time this method was called.
     * Rules:
     * * Only changes something if throughput changed significantly since the last time.
     * * Changes the degree of parallelism in the same direction as throughput.
     * ** Exception is if both throughput and the fill ratio are declining, in which case nothing is done.
     *
     * @return The degree of parallelism, whether it was updated or not.
     */
    int updateParallelism() {
        // Calculate the most recent throughput and Fill Ratio (of those items that have just been written).
        final long time = this.queue.getTimeSupplier().get();
        final long elapsedMillis = (time - this.lastSnapshot.timeStamp) / AbstractTimer.NANOS_TO_MILLIS;
        if (elapsedMillis < MIN_FREQUENCY_MILLIS) {
            // No need to do any change.
            return this.lastSnapshot.parallelism;
        } else if (elapsedMillis >= STALE_MILLIS) {
            // Last snapshot is too old. We can't make any good determination.
            resetSnapshot();
            return this.lastSnapshot.parallelism;
        }

        final WriteQueue.WriteStats recentStats = this.queue.fetchRecentWriteStats();
        final double recentThroughput = (double) recentStats.totalLength / elapsedMillis;
        final double recentFillRatio = WriteQueue.calculateFillRatio(recentStats.totalLength, recentStats.count);
        final double throughputDifference = recentThroughput - this.lastSnapshot.throughput;

        int parallelism = this.lastSnapshot.parallelism;
        if (Math.abs(throughputDifference / this.lastSnapshot.throughput) >= SIGNIFICANT_DIFFERENCE) {
            // Throughput changed significantly.
            // But first, check for a special case. If both Throughput and Fill Ratio are decreasing, then most likely the
            // former is a result of the latter. As such, don't do anything, in that case.
            boolean fillRatioDecreased = (recentFillRatio - this.lastSnapshot.fillRatio) <= -SIGNIFICANT_DIFFERENCE;
            if (throughputDifference > 0 || !fillRatioDecreased) {
                // Either the throughput increased or the Fill Ratio did not decrease significantly.
                // Change the parallelism in the same direction as Throughput.
                // Update the degree of parallelism, but make sure we don't exceed the given bounds.
                parallelism = MathHelpers.minMax(parallelism + (int) Math.signum(throughputDifference), this.minParallelism, this.maxParallelism);
            }
        }

        // Update stats and reset counters.
        this.lastSnapshot = new Snapshot(recentFillRatio, recentThroughput, time, parallelism);
        //TODO: log with context.
        System.err.println(this.lastSnapshot);
        return parallelism;
    }

    private void resetSnapshot() {
        this.lastSnapshot = new Snapshot(0, 0, this.queue.getTimeSupplier().get(), this.minParallelism * 2);
    }

    //endregion

    //region Snapshot

    @RequiredArgsConstructor
    private static class Snapshot {
        final double fillRatio;
        final double throughput;
        final long timeStamp;
        final int parallelism;

        @Override
        public String toString() {
            return String.format("Throughput = %.1f B/ms, FillRatio = %.2f, Parallelism = %d", this.throughput, this.fillRatio, this.parallelism);
        }
    }

    //endregion
}

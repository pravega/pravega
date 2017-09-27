/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.bookkeeper;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.AbstractTimer;
import io.pravega.common.MathHelpers;
import java.util.function.Supplier;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.RequiredArgsConstructor;

/**
 * Manages the Write Queue Concurrency.
 */
@ThreadSafe
class ConcurrencyManager {
    //region Members

    @VisibleForTesting
    static final double SIGNIFICANT_DIFFERENCE = 0.1;
    @VisibleForTesting
    static final long UPDATE_PERIOD_MILLIS = 5000; // At most once every 5s.
    @VisibleForTesting
    static final long STALE_MILLIS = UPDATE_PERIOD_MILLIS * 12; // After 60s of no activity, reset all stats.
    private final int minParallelism;
    private final int maxParallelism;
    private final Supplier<Long> timeSupplier;
    private final Object snapshotLock = new Object();
    @GuardedBy("this")
    private long recentTotalWrittenLength;
    @GuardedBy("this")
    private int recentWriteCount;
    @GuardedBy("snapshotLock")
    private Snapshot lastSnapshot;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ConcurrencyManager class.
     *
     * @param minParallelism The minimum degree of parallelism desired.
     * @param maxParallelism The maximum degree of parallelism desired.
     */
    ConcurrencyManager(int minParallelism, int maxParallelism) {
        this(minParallelism, maxParallelism, System::nanoTime);
    }

    /**
     * Creates a new instance of the ConcurrencyManager class.
     *
     * @param minParallelism The minimum degree of parallelism desired.
     * @param maxParallelism The maximum degree of parallelism desired.
     * @param timeSupplier   A Supplier that returns the current time, in nanoseconds.
     */
    @VisibleForTesting
    ConcurrencyManager(int minParallelism, int maxParallelism, Supplier<Long> timeSupplier) {
        this.timeSupplier = Preconditions.checkNotNull(timeSupplier, "timeSupplier");
        Preconditions.checkArgument(minParallelism <= maxParallelism, "minParallelism must be <= maxParallelism.");
        this.minParallelism = minParallelism;
        this.maxParallelism = maxParallelism;
        this.recentTotalWrittenLength = 0;
        this.recentWriteCount = 0;
        resetSnapshot(this.timeSupplier.get());
    }

    //endregion

    //region Operations

    /**
     * Records the fact that a write with the given length has just completed successfully.
     *
     * @param writeLength The length of the write that completed.
     */
    void writeCompleted(int writeLength) {
        synchronized (this) {
            this.recentWriteCount++;
            this.recentTotalWrittenLength += writeLength;
        }
    }

    /**
     * Gets a value indicating the current (most recent) degree of parallelism.
     *
     * @return The current degree of parallelism.
     */
    int getCurrentParallelism() {
        synchronized (this.snapshotLock) {
            return this.lastSnapshot.parallelism;
        }
    }

    /**
     * Updates the degree of parallelism (if needed), based on information collected since the last time this method was called.
     * Notes:
     * * Only changes something if throughput changed significantly since the last time.
     * * Changes the degree of parallelism in the same direction as throughput.
     * ** Exception is if both throughput and the fill ratio are declining, in which case nothing is done.
     *
     * @return The degree of parallelism, whether it was updated or not.
     */
    int getOrUpdateParallelism() {
        if (this.minParallelism == this.maxParallelism) {
            // Min == Max, so the parallelism will never change.
            return getCurrentParallelism();
        }

        final long time = this.timeSupplier.get();
        synchronized (this.snapshotLock) {
            // Calculate the most recent throughput and Fill Ratio (of those items that have just been written).
            final long elapsedMillis = (time - this.lastSnapshot.timeStamp) / AbstractTimer.NANOS_TO_MILLIS;
            if (elapsedMillis >= STALE_MILLIS) {
                // Too long since the last update; the data we have is no longer relevant. Reset.
                resetSnapshot(time);
            } else if (elapsedMillis >= UPDATE_PERIOD_MILLIS) {
                // Enough time elapsed since the last update; update the snapshot.
                updateSnapshot(time, elapsedMillis);
            }

            return this.lastSnapshot.parallelism;
        }
    }

    /**
     * Updates the current snapshot based on the current data.
     *
     * @param time          The current time, in millis.
     * @param elapsedMillis The elapsed time, in millis, since the last time this method ran.
     */
    @GuardedBy("snapshotLock")
    private void updateSnapshot(long time, long elapsedMillis) {
        final int recentCount;
        final long recentLength;
        synchronized (this) {
            recentCount = this.recentWriteCount;
            recentLength = this.recentTotalWrittenLength;
            this.recentWriteCount = 0;
            this.recentTotalWrittenLength = 0;
        }

        final double recentThroughput = (double) recentLength / elapsedMillis;
        final double recentFillRatio = WriteQueue.calculateFillRatio(recentLength, recentCount);
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
        } else if (parallelism == this.minParallelism) {
            parallelism = Math.min(this.maxParallelism, parallelism + 1);
        }

        // Update snapshot with the latest stats.
        this.lastSnapshot = new Snapshot(recentFillRatio, recentThroughput, time, parallelism);
    }

    @GuardedBy("snapshotLock")
    private void resetSnapshot(long time) {
        this.lastSnapshot = new Snapshot(0, 0, time, Math.min(this.minParallelism * 4, this.maxParallelism));
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
            return String.format("Throughput = %.1f B/ms, FillRatio = %.2f, Parallelism = %d",
                    this.throughput, this.fillRatio, this.parallelism);
        }
    }

    //endregion
}

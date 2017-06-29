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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.AbstractTimer;
import io.pravega.common.MathHelpers;
import java.util.concurrent.atomic.AtomicReference;
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
    static final double SIGNIFICANT_DIFFERENCE = 0.25;
    @VisibleForTesting
    static final long MIN_FREQUENCY_MILLIS = 1000;
    @VisibleForTesting
    static final long STALE_MILLIS = MIN_FREQUENCY_MILLIS * 10;
    @VisibleForTesting
    static final int MAX_STAGNATION_AGE = 20;
    private final int minParallelism;
    private final int maxParallelism;
    private final Supplier<Long> timeSupplier;
    @GuardedBy("this")
    private long recentTotalWrittenLength;
    @GuardedBy("this")
    private int recentWriteCount;
    private final AtomicReference<Snapshot> lastSnapshot;

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
        this.lastSnapshot = new AtomicReference<>();
        resetSnapshot();
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
     * @return The result
     */
    int getCurrentParallelism() {
        return this.lastSnapshot.get().parallelism;
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
        final long time = this.timeSupplier.get();
        final Snapshot lastSnapshot = this.lastSnapshot.get();
        final long elapsedMillis = (time - lastSnapshot.timeStamp) / AbstractTimer.NANOS_TO_MILLIS;
        if (elapsedMillis < MIN_FREQUENCY_MILLIS) {
            // No need to do any change.
            return lastSnapshot.parallelism;
        } else if (elapsedMillis >= STALE_MILLIS) {
            // Last snapshot is too old. We can't make any good determination.
            resetSnapshot();
            return this.lastSnapshot.get().parallelism;
        }

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
        final double throughputDifference = recentThroughput - lastSnapshot.throughput;

        int parallelism = lastSnapshot.parallelism;
        if (Math.abs(throughputDifference / lastSnapshot.throughput) >= SIGNIFICANT_DIFFERENCE) {
            // Throughput changed significantly.
            // But first, check for a special case. If both Throughput and Fill Ratio are decreasing, then most likely the
            // former is a result of the latter. As such, don't do anything, in that case.
            boolean fillRatioDecreased = (recentFillRatio - lastSnapshot.fillRatio) <= -SIGNIFICANT_DIFFERENCE;
            if (throughputDifference > 0 || !fillRatioDecreased) {
                // Either the throughput increased or the Fill Ratio did not decrease significantly.
                // Change the parallelism in the same direction as Throughput.
                // Update the degree of parallelism, but make sure we don't exceed the given bounds.
                parallelism = MathHelpers.minMax(parallelism + (int) Math.signum(throughputDifference), this.minParallelism, this.maxParallelism);
            }
        } else if (parallelism == this.minParallelism) {
            parallelism = Math.min(this.maxParallelism, parallelism + 1);
        }

        int age = lastSnapshot.age;
        if (parallelism == lastSnapshot.parallelism && age >= MAX_STAGNATION_AGE) {
            // If we have been stuck at this degree of parallelism for too long, nudge the parallelism up or down by a bit.
            parallelism = MathHelpers.minMax(parallelism + (parallelism == this.maxParallelism ? -1 : 1), this.minParallelism, this.maxParallelism);
        }

        if (parallelism != lastSnapshot.parallelism) {
            // Degree of parallelism changed - reset age.
            age = 0;
        }

        // Update stats and reset counters.
        this.lastSnapshot.set(new Snapshot(recentFillRatio, recentThroughput, time, parallelism, age + 1));
        return parallelism;
    }

    private void resetSnapshot() {
        this.lastSnapshot.set(new Snapshot(0, 0, this.timeSupplier.get(), Math.min(this.minParallelism * 2, this.maxParallelism), 1));
    }

    //endregion

    //region Snapshot

    @RequiredArgsConstructor
    private static class Snapshot {
        final double fillRatio;
        final double throughput;
        final long timeStamp;
        final int parallelism;
        final int age;

        @Override
        public String toString() {
            return String.format("Throughput = %.1f B/ms, FillRatio = %.2f, Parallelism = %d, Age = %d",
                    this.throughput, this.fillRatio, this.parallelism, this.age);
        }
    }

    //endregion
}

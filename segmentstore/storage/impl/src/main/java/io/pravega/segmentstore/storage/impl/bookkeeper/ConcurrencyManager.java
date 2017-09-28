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
    static final double SIGNIFICANT_DIFFERENCE = 0.25; // Changes below this will be ignored.
    @VisibleForTesting
    static final long UPDATE_PERIOD_MILLIS = 1000; // Min interval for updates.
    @VisibleForTesting
    static final long STALE_MILLIS = UPDATE_PERIOD_MILLIS * 15; // Reset all stats if no activity in this interval.
    @VisibleForTesting
    static final int MAX_STAGNATION_AGE = 10;
    private final int minParallelism;
    private final int maxParallelism;
    private final Supplier<Long> timeSupplier;
    private final Object snapshotLock = new Object();
    @GuardedBy("this")
    private long recentTotalWrittenLength;
    @GuardedBy("this")
    private long recentTotalLatencyMillis;
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
     * @param writeLength   The length of the write that completed.
     * @param elapsedMillis The amount of time, in millis, that this write took to complete.
     */
    void writeCompleted(int writeLength, long elapsedMillis) {
        synchronized (this) {
            this.recentWriteCount++;
            this.recentTotalWrittenLength += writeLength;
            this.recentTotalLatencyMillis += elapsedMillis;
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
     * General Notes:
     * * If Min and Max Parallelism are the same, this method doesn't change anything.
     * * Only attempts to change something if enough time passed since the last call to this method that made a change.
     * * Any change to Parallelism will be done in increments of 1 (either up or down).
     *
     * Important Observations:
     * * BookKeeper (BK) write throughput and latency, for a constant write size, will increase with the degree of parallelism
     * until a certain point, after which throughput plateaus and latency starts to rise at a higher rate.
     * * The optimal degree of parallelism varies based on a number of factors, such as write size, cluster configuration
     * and cluster load/network capacity.
     *
     * How it works:
     * * Compares Throughput and Latency in the period that just ended with the period before (normalizes based on interval duration).
     * * If Throughput increases, the Parallelism is increased.
     * * If Throughput decreases, the Parallelism is decreased. An exception is if the last action we took prior to this
     * was to decrease Parallelism, in which case the Parallelism is increased.
     * * If Throughput does not change (significantly), then the Parallelism is changed in the opposite direction
     * in which Latency changed.
     * * If we were unable to change Parallelism for a long time, it will be automatically decreased in an effort
     * to get out of a local optimum where it may have been stuck.
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
     * Updates the current snapshot based on the current data. See getOrUpdateParallelism() documentation for how this works.
     *
     * @param time          The current time, in millis.
     * @param elapsedMillis The elapsed time, in millis, since the last time this method ran.
     */
    @GuardedBy("snapshotLock")
    private void updateSnapshot(long time, long elapsedMillis) {
        final double recentThroughput;
        final long recentLatency;
        synchronized (this) {
            recentThroughput = (double) this.recentTotalWrittenLength / elapsedMillis;
            recentLatency = this.recentWriteCount == 0 ? 0 : this.recentTotalLatencyMillis / this.recentWriteCount;
            this.recentWriteCount = 0;
            this.recentTotalWrittenLength = 0;
            this.recentTotalLatencyMillis = 0;
        }

        final double throughputDifference = recentThroughput - this.lastSnapshot.throughput;
        final long latencyDifference = recentLatency - this.lastSnapshot.latency;
        boolean significantThroughputChange = Math.abs(throughputDifference / this.lastSnapshot.throughput) >= SIGNIFICANT_DIFFERENCE;
        boolean significantLatencyChange = this.lastSnapshot.latency == 0
                ? latencyDifference != 0
                : Math.abs((double) latencyDifference / this.lastSnapshot.latency) >= SIGNIFICANT_DIFFERENCE;

        int delta = 0;
        if (significantThroughputChange) {
            // Throughput changed significantly. Adjust parallelism in the same direction as throughput change.
            delta = (int) Math.signum(throughputDifference);
            if (throughputDifference < 0 && this.lastSnapshot.delta < 0) {
                // Special case: If throughput decreased, and the last action we took was a decrease, then we should
                // revert that action and increase again. It is very likely the decrease in throughput was a result of
                // us lowering the parallelism.
                delta = 1;
            }
        } else if (significantLatencyChange) {
            // Throughput did not change significantly; if Latency did, then adjust in the opposite direction of the latency change.
            delta = -(int) Math.signum(latencyDifference);
        }

        int age = this.lastSnapshot.age;
        if (delta == 0 && age >= MAX_STAGNATION_AGE) {
            // If we have been stuck at this degree of parallelism for too long, nudge the parallelism down to trigger a
            // change, except if we have been stuck at the min parallelism, in which case we want to nudge up.
            delta = this.lastSnapshot.parallelism == this.minParallelism ? 1 : -1;
        }

        if (delta != 0) {
            // Degree of parallelism changed - reset age.
            age = 0;
        }

        // Update snapshot with the latest stats.
        int parallelism = MathHelpers.minMax(this.lastSnapshot.parallelism + delta, this.minParallelism, this.maxParallelism);
        delta = parallelism - this.lastSnapshot.parallelism; // Recalculate actual change.
        this.lastSnapshot = new Snapshot(recentThroughput, recentLatency, time, parallelism, delta, age + 1);
    }

    @GuardedBy("snapshotLock")
    private void resetSnapshot(long time) {
        this.lastSnapshot = new Snapshot(0, 0, time, Math.min(this.minParallelism * 2, this.maxParallelism), 0, 1);
    }

    //endregion

    //region Snapshot

    @RequiredArgsConstructor
    private static class Snapshot {
        final double throughput;
        final long latency;
        final long timeStamp;
        final int parallelism;
        final int delta;
        final int age;

        @Override
        public String toString() {
            return String.format("TPut=%.1fBps, Latency=%dms, Parallelism=%d, Age=%d",
                    this.throughput, this.latency, this.parallelism, this.age);
        }
    }

    //endregion
}

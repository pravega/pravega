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
import io.pravega.common.Exceptions;
import io.pravega.common.MathHelpers;
import io.pravega.segmentstore.storage.QueueStats;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.EnumSet;
import java.util.List;
import java.util.function.Supplier;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.RequiredArgsConstructor;

/**
 * A specialized queue for BookKeeper writes. Provides methods for adding new items, determining the next items to execute,
 * as well as cleaning up completed writes.
 */
@ThreadSafe
class WriteQueue {
    //region Members

    private final Supplier<Long> timeSupplier;
    private final int baseParallelism;
    private final int parallelismSpan;
    @GuardedBy("this")
    private final Deque<Write> writes;
    @GuardedBy("this")
    private long totalLength;
    @GuardedBy("this")
    private int lastDurationMillis;
    @GuardedBy("this")
    private boolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the WriteQueue class.
     *
     * @param minParallelism The minimum number of items that can be processed at once.
     * @param maxParallelism The maximum number of items that can be processed at once.
     */
    WriteQueue(int minParallelism, int maxParallelism) {
        this(minParallelism, maxParallelism, System::nanoTime);
    }

    /**
     * Creates a new instance of the WriteQueue class.
     *
     * @param minParallelism The minimum number of items that can be processed at once.
     * @param maxParallelism The maximum number of items that can be processed at once.
     * @param timeSupplier   A Supplier that returns the current time, in nanoseconds.
     */
    @VisibleForTesting
    WriteQueue(int minParallelism, int maxParallelism, Supplier<Long> timeSupplier) {
        this.timeSupplier = Preconditions.checkNotNull(timeSupplier, "timeSupplier");
        Preconditions.checkArgument(minParallelism <= maxParallelism, "minParallelism must be <= maxParallelism.");
        this.baseParallelism = minParallelism;
        this.parallelismSpan = maxParallelism - minParallelism;
        this.writes = new ArrayDeque<>();
        this.lastParallelismUpdate = this.timeSupplier.get() / AbstractTimer.NANOS_TO_MILLIS;
        this.currentParallelism = minParallelism;
    }

    //endregion

    //region Queue Operations

    /**
     * Gets a snapshot of the queue internals.
     *
     * @return The snapshot, including Queue Size, Item Fill Rate and elapsed time of the oldest item.
     */
    synchronized QueueStats getStatistics() {
        int size = this.writes.size();
        double fillRatio = getFillRatio();
        int processingTime = this.lastDurationMillis;
        if (processingTime == 0 && size > 0) {
            // We get in here when this method is invoked prior to any operation being completed. Since lastDurationMillis
            // is only set when an item is completed, in this special case we just estimate based on the amount of time
            // the first item in the queue has been added.
            processingTime = (int) ((this.timeSupplier.get() - this.writes.peekFirst().getTimestamp()) / AbstractTimer.NANOS_TO_MILLIS);
        }

        int parallelism = calculateParallelism(getFillRatio(), this.baseParallelism, this.parallelismSpan);
        return new QueueStats(parallelism, size, fillRatio, processingTime);
    }

    /**
     * Adds a new Write to the end of the queue.
     *
     * @param write The write to add.
     */
    synchronized void add(Write write) {
        Exceptions.checkNotClosed(this.closed, this);
        this.writes.addLast(write);
        this.totalLength += write.data.getLength();
        write.setTimestamp(this.timeSupplier.get());
    }

    /**
     * Clears the queue of all the items and closes it, preventing any new writes from being added.
     *
     * @return A new List with the contents of the queue (prior to cleanup), in the same order.
     */
    synchronized List<Write> close() {
        List<Write> items = new ArrayList<>(this.writes);
        this.writes.clear();
        this.totalLength = 0;
        this.closed = true;
        return items;
    }

    /**
     * Gets an ordered List of Writes that are ready to be executed. The returned writes are not removed from the queue
     * and are in the same order they are in the queue. They are not necessarily the first items in the queue (if, for
     * example, the head of the queue has a bunch of completed Writes).
     * This method will return writes as long as:
     * * Neither of the MaxSize or MaxCount limits are reached
     * * The writes to return have the same Ledger Id assigned as the first write in the queue.
     *
     * @param maximumAccumulatedSize The maximum total accumulated size of the items to return. Once this value is exceeded,
     *                               no further writes are returned.
     * @return The result.
     */
    synchronized List<Write> getWritesToExecute(long maximumAccumulatedSize) {
        Exceptions.checkNotClosed(this.closed, this);
        long accumulatedSize = 0;
        int activeWriteCount = 0;

        // Collect all remaining writes, as long as they are not currently in-progress and have the same ledger id
        // as the first item in the ledger.
        long firstLedgerId = this.writes.peekFirst().getWriteLedger().metadata.getLedgerId();
        boolean canSkip = true;

        List<Write> result = new ArrayList<>();
        final int maxParallelism = calculateParallelism(getFillRatio(), this.baseParallelism, this.parallelismSpan);
        for (Write write : this.writes) {
            if (accumulatedSize >= maximumAccumulatedSize || activeWriteCount >= maxParallelism) {
                // Either reached the throttling limit or ledger max size limit.
                // If we try to send too many writes to this ledger, the writes are likely to be rejected with
                // LedgerClosedException and simply be retried again.
                break;
            }

            // Account for this write's size, even if it's complete or in progress.
            accumulatedSize += write.data.getLength();
            if (write.isInProgress()) {
                activeWriteCount++;
                if (!canSkip) {
                    // We stumbled across an in-progress write after a not-in-progress write. We can't retry now.
                    // This is likely due to a bunch of writes failing (i.e. due to a LedgerClosedEx), but we overlapped
                    // with their updating their status. Try again next time (when that write completes).
                    return Collections.emptyList();
                }
            } else if (write.getWriteLedger().metadata.getLedgerId() != firstLedgerId) {
                // We cannot initiate writes in a new ledger until all writes in the previous ledger completed.
                break;
            } else if (!write.isDone()) {
                canSkip = false;
                result.add(write);
                activeWriteCount++;
            }
        }

        return result;
    }

    /**
     * Removes all the completed writes (whether successful or failed) from the beginning of the queue, until the first
     * non-completed item is encountered or the queue is empty.
     *
     * @return True if there are items left in the queue, false otherwise.
     */
    synchronized EnumSet<CleanupStatus> removeFinishedWrites() {
        Exceptions.checkNotClosed(this.closed, this);
        long currentTime = this.timeSupplier.get();
        long totalElapsed = 0;
        int removedCount = 0;
        boolean failedWrite = false;
        while (!this.writes.isEmpty() && this.writes.peekFirst().isDone()) {
            Write w = this.writes.removeFirst();
            this.totalLength = Math.max(0, this.totalLength - w.data.getLength());
            removedCount++;
            this.completedWritesLength += w.data.getLength();
            totalElapsed += currentTime - w.getTimestamp();
            failedWrite |= w.getFailureCause() != null;
        }

        if (removedCount > 0) {
            this.lastDurationMillis = (int) (totalElapsed / removedCount / AbstractTimer.NANOS_TO_MILLIS);
        }

        CleanupStatus empty = this.writes.isEmpty() ? CleanupStatus.QueueEmpty : CleanupStatus.QueueNotEmpty;
        return failedWrite ? EnumSet.of(CleanupStatus.WriteFailed, empty) : EnumSet.of(empty);
    }

    @Override
    public String toString() {
        return getStatistics().toString();
    }

    /**
     * Calculates the WriteQueue's FillRatio, which is a number between [0, 1] that represents the average fill of each
     * write with respect to the maximum BookKeeper write allowance.
     */
    @GuardedBy("this")
    private double getFillRatio() {
        int size = this.writes.size();
        if (size > 0) {
            return Math.min(1, (double) this.totalLength / size / BookKeeperConfig.MAX_APPEND_LENGTH);
        } else {
            return 0;
        }
    }

    /**
     * Calculates the degree of parallelism.
     *
     * @param fillRatio       The Queue Fill Ratio.
     * @param baseParallelism The minimum degree of parallelism.
     * @param parallelismSpan The difference between the maximum degree of parallelism and minimum degree of parallelism.
     */
    @VisibleForTesting
    static int calculateParallelism(double fillRatio, int baseParallelism, int parallelismSpan) {
        double parallelism;
        if (fillRatio < 0.01) {
            parallelism = parallelismSpan;
        } else {
            parallelism = Math.min(parallelismSpan, -1 / Math.log(1 - fillRatio));
        }
        return (int) parallelism + baseParallelism;
    }

    @GuardedBy("this")
    private long lastParallelismUpdate;
    @GuardedBy("this")
    private long completedWritesLength;
    @GuardedBy("this")
    private PerformanceStats lastPerfStats;
    @GuardedBy("this")
    private int currentParallelism;

    synchronized void updateParallelism() {
        long time = this.timeSupplier.get() / AbstractTimer.NANOS_TO_MILLIS;
        long elapsed = time - this.lastParallelismUpdate;
        int delta = 0; // 0: No change, +1: Increase, -1: Decrease.

        // TODO 1: Review this method.
        // TODO 2: Integrate this method with the rest of the class
        // TODO 3: Update MinMaxParallelism in SelfTester with 1 and 100, respectively.
        if (this.lastPerfStats != null) {
            // Only bother to change something if we have something to compare against.
            // If TPUT UP, then perform the same action we did last time.
            // If TPUT DOWN, then perform the opposite action we did last time.
            // If TPUT SAME, then increase or decrease parallelism based on current queue size change.

            // TPut is measured in Bytes/Millis
            double oldTPut = (double) this.completedWritesLength / elapsed;
            double tPutDiff = oldTPut - (double) this.lastPerfStats.bytes / this.lastPerfStats.elapsedMillis;
            if (Math.abs(tPutDiff / oldTPut) < 0.01) {
                // Insignificant change: use queue size to determine.
                delta = this.writes.size() - this.lastPerfStats.queueSize;
            } else if (this.lastPerfStats.delta == 0) {
                // We didn't do anything last time. Pick a change based off whether throughput went up or down.
                delta = tPutDiff < 0 ? -1 : 1;
            } else {
                // Somewhat of a significant change: use throughput direction to determine.
                delta = this.lastPerfStats.delta;
                if (tPutDiff < 0) {
                    // Throughput decreased: do the opposite of what we did last time.
                    delta = Math.min(-1, -delta); // But also decrease parallelism if we did no change.
                }
            }

            delta = MathHelpers.minMax(delta, -1, 1);
            this.currentParallelism = MathHelpers.minMax(this.currentParallelism + delta,
                    this.baseParallelism, this.baseParallelism + this.parallelismSpan);
        }

        this.lastPerfStats = new PerformanceStats(elapsed, this.completedWritesLength, this.writes.size(), delta);
        System.out.println(String.format("Elapsed: %s, Bytes: %s, Stats: %s, NewP: %s",
                elapsed, this.completedWritesLength, this.lastPerfStats, this.currentParallelism));
        this.lastParallelismUpdate = time;
        this.completedWritesLength = 0;
    }

    //endregion

    @RequiredArgsConstructor
    private static class PerformanceStats {
        final long elapsedMillis;
        final long bytes;
        final int queueSize;
        final int delta;

        @Override
        public String toString() {
            return String.format("Bytes = %d, Elapsed = %d, Queue = %d, Delta = %d",
                    this.bytes, this.elapsedMillis, this.queueSize, this.delta);
        }
    }

    //region CleanupStatus

    /**
     * Defines various states that the WriteQueue may be in after a cleanup is performed.
     */
    enum CleanupStatus {
        /**
         * The Queue is empty after the operation.
         */
        QueueEmpty,

        /**
         * The Queue is not empty after the operation.
         */
        QueueNotEmpty,

        /**
         * A permanently failed Write was detected.
         */
        WriteFailed
    }

    //endregion
}

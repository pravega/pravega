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

/**
 * A specialized queue for BookKeeper writes. Provides methods for adding new items, determining the next items to execute,
 * as well as cleaning up completed writes.
 */
@ThreadSafe
class WriteQueue {
    //region Members

    private final Supplier<Long> timeSupplier;
    private final int maxParallelism;
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
     * @param maxParallelism The maximum number of items that can be processed at once.
     */
    WriteQueue(int maxParallelism) {
        this(maxParallelism, System::nanoTime);
    }

    /**
     * Creates a new instance of the WriteQueue class.
     *
     * @param maxParallelism The maximum number of items that can be processed at once.
     * @param timeSupplier   A Supplier that returns the current time, in nanoseconds.
     */
    @VisibleForTesting
    WriteQueue(int maxParallelism, Supplier<Long> timeSupplier) {
        this.timeSupplier = Preconditions.checkNotNull(timeSupplier, "timeSupplier");
        this.maxParallelism = maxParallelism;
        this.writes = new ArrayDeque<>();
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
        double fillRate = 0;
        if (size > 0) {
            fillRate = Math.min(1, (double) this.totalLength / size / BookKeeperConfig.MAX_APPEND_LENGTH);
        }

        int processingTime = this.lastDurationMillis;
        if (processingTime == 0 && size > 0) {
            // We get in here when this method is invoked prior to any operation being completed. Since lastDurationMillis
            // is only set when an item is completed, in this special case we just estimate based on the amount of time
            // the first item in the queue has been added.
            processingTime = (int) ((this.timeSupplier.get() - this.writes.peekFirst().getTimestamp()) / AbstractTimer.NANOS_TO_MILLIS);
        }

        return new QueueStats(this.maxParallelism, size, fillRate, processingTime);
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
        for (Write write : this.writes) {
            if (accumulatedSize >= maximumAccumulatedSize || activeWriteCount >= this.maxParallelism) {
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

    //endregion

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

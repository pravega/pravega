/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

/**
 * Processes items in order, subject to capacity constraints.
 */
@ThreadSafe
public class OrderedItemProcessor<ItemType, ResultType> implements AutoCloseable {
    //region members

    private static final int CLOSE_TIMEOUT_MILLIS = 60 * 1000;
    private final int capacity;
    @GuardedBy("processingLock")
    private final Function<ItemType, CompletableFuture<ResultType>> processor;
    @GuardedBy("stateLock")
    private final Deque<QueueItem> pendingItems;
    private final Executor executor;

    /**
     * Guards access to the OrderedItemProcessor's internal state (counts and queues).
     */
    private final Object stateLock = new Object();

    /**
     * Guards access to the individual item's processors. No two invocations of the processor may run at the same time.
     */
    private final Object processingLock = new Object();
    @GuardedBy("stateLock")
    private int activeCount;
    @GuardedBy("stateLock")
    private boolean closed;
    @GuardedBy("stateLock")
    private ReusableLatch emptyNotifier;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the OrderedItemProcessor class.
     *
     * @param capacity  The maximum number of concurrent executions.
     * @param processor A Function that, given an Item, returns a CompletableFuture that indicates when the item has been
     *                  processed (successfully or not).
     * @param executor  An Executor for async invocations.
     */
    public OrderedItemProcessor(int capacity, Function<ItemType, CompletableFuture<ResultType>> processor, Executor executor) {
        Preconditions.checkArgument(capacity > 0, "capacity must be a non-negative number.");
        this.capacity = capacity;
        this.processor = Preconditions.checkNotNull(processor, "processor");
        this.executor = Preconditions.checkNotNull(executor, "executor");
        this.pendingItems = new ArrayDeque<>();
        this.activeCount = 0;
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    @SneakyThrows(Exception.class)
    public void close() {
        ReusableLatch waitSignal = null;
        synchronized (this.stateLock) {
            if (this.closed) {
                return;
            }

            this.closed = true;
            if (this.activeCount != 0 || !this.pendingItems.isEmpty()) {
                // Setup a latch that will be released when the last item completes.
                this.emptyNotifier = new ReusableLatch(false);
                waitSignal = this.emptyNotifier;
            }
        }

        if (waitSignal != null) {
            // We have unfinished items. Wait for them.
            waitSignal.await(CLOSE_TIMEOUT_MILLIS);
        }
    }

    //endregion

    //region Processing

    /**
     * Processes the given item.
     * * If the OrderedItemProcessor is below capacity, then the item will be processed immediately and the returned
     * future is the actual result from the Processor.
     * * If the max capacity is exceeded, the item will be queued up and it will be processed when capacity allows, after
     * all the items added before it have been processed.
     * * If an item before this one failed to process and this item is still in the queue (it has not yet been processed),
     * the returned future will be cancelled with a ProcessingException to prevent out-of-order executions.
     * * This method guarantees ordered execution as long as its invocations are serial. That is, it only guarantees that
     * the item has begun processing or an order assigned if the method returned successfully.
     * * If an item failed to execute, this class will auto-close and will not be usable anymore.
     *
     * @param item The item to process.
     * @return A CompletableFuture that, when completed, will indicate that the item has been processed. This will contain
     * the result of the processing function applied to this item.
     */
    public CompletableFuture<ResultType> process(ItemType item) {
        Preconditions.checkNotNull(item, "item");
        CompletableFuture<ResultType> result = null;
        synchronized (this.stateLock) {
            Exceptions.checkNotClosed(this.closed, this);
            if (hasCapacity() && this.pendingItems.isEmpty()) {
                // Not at capacity. Reserve a spot now.
                this.activeCount++;
            } else {
                // We are at capacity or have a backlog. Put the item in the queue and return its associated future.
                result = new CompletableFuture<>();
                this.pendingItems.add(new QueueItem(item, result));
            }
        }

        if (result == null) {
            // We are below max capacity, so simply process the item, without queuing up.
            // It is possible, under some conditions, that we may get in here when the queue empties up (by two spots at
            // the same time). In that case, we need to acquire the processing lock to ensure that we don't accidentally
            // process this item before we finish processing the last item in the queue.
            synchronized (this.processingLock) {
                result = processInternal(item);
            }
        }

        return result;
    }

    /**
     * Callback that is invoked when an item has completed execution.
     *
     * @param exception (Optional) An exception from the execution. If set, it indicates the item has not been
     *                  processed successfully.
     */
    @VisibleForTesting
    protected void executionComplete(Throwable exception) {
        Collection<QueueItem> toFail = null;
        Throwable failEx = null;
        synchronized (this.stateLock) {
            // Release the spot occupied by this item's execution.
            this.activeCount--;
            if (exception != null && !this.closed) {
                // Need to fail all future items and close to prevent new items from being processed.
                failEx = new ProcessingException("A previous item failed to commit. Cannot process new items.", exception);
                toFail = new ArrayList<>(this.pendingItems);
                this.pendingItems.clear();
                this.closed = true;
            }

            if (this.emptyNotifier != null && this.activeCount == 0 && this.pendingItems.isEmpty()) {
                // We were asked to notify when we were empty.
                this.emptyNotifier.release();
                this.emptyNotifier = null;
            }
        }

        if (toFail != null) {
            for (QueueItem q : toFail) {
                q.result.completeExceptionally(failEx);
            }

            return;
        }

        // We need to ensure the items are still executed in order. Once out of the main sync block, it is possible that
        // this callback may be invoked concurrently after various completions. As such, we need a special handler for
        // these, using its own synchronization, different from the main one, so that we don't hold that stateLock for too long.
        synchronized (this.processingLock) {
            while (true) {
                QueueItem toProcess;
                synchronized (this.stateLock) {
                    if (hasCapacity() && !this.pendingItems.isEmpty()) {
                        // We have spare capacity and we have something to process. Dequeue it and reserve the spot.
                        toProcess = this.pendingItems.pollFirst();
                        this.activeCount++;
                    } else {
                        // No capacity left or no more pending items.
                        break;
                    }
                }

                Futures.completeAfter(() -> processInternal(toProcess.data), toProcess.result);
            }
        }
    }

    @GuardedBy("processingLock")
    private CompletableFuture<ResultType> processInternal(ItemType data) {
        try {
            val result = this.processor.apply(data);
            result.whenCompleteAsync((r, ex) -> executionComplete(ex), this.executor);
            return result;
        } catch (Throwable ex) {
            if (!Exceptions.mustRethrow(ex)) {
                executionComplete(ex);
            }

            throw ex;
        }
    }

    @GuardedBy("stateLock")
    private boolean hasCapacity() {
        return this.activeCount < this.capacity;
    }

    //endregion

    //region ProcessingException

    public static class ProcessingException extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        private ProcessingException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    //endregion

    //region QueueItem

    @RequiredArgsConstructor
    private class QueueItem {
        final ItemType data;
        final CompletableFuture<ResultType> result;
    }

    //endregion
}

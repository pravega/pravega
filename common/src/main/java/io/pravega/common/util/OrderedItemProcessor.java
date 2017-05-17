package io.pravega.common.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.ExceptionHelpers;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.FutureHelpers;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * Processes items in order, subject to capacity constraints.
 */
@ThreadSafe
class OrderedItemProcessor<ItemType, ResultType> implements AutoCloseable {
    //region members

    private final int capacity;
    private final Function<ItemType, CompletableFuture<ResultType>> processor;
    @GuardedBy("lock")
    private final Deque<QueueItem> pendingItems;
    private final Executor executor;
    private final Object lock = new Object();
    private final Object queueProcessorLock = new Object();
    @GuardedBy("lock")
    private int activeCount;
    @GuardedBy("lock")
    private RuntimeException failedStateException;
    @GuardedBy("lock")
    private boolean closed;

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
    OrderedItemProcessor(int capacity, Function<ItemType, CompletableFuture<ResultType>> processor, Executor executor) {
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
    public void close() {
        Collection<QueueItem> toCancel;
        synchronized (this.lock) {
            if (this.closed) {
                return;
            }

            // Cancel all pending items.
            toCancel = new ArrayList<>(this.pendingItems);
            this.pendingItems.clear();
            this.closed = true;
        }

        toCancel.forEach(qi -> qi.result.cancel(true));
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
     * the returned future will be cancelled with an IllegalStateException to prevent out-of-order executions.
     * * This method guarantees ordered execution as long as its invocations are serial. That is, it only guarantees that
     * the item has begun processing or an order assigned if it returned successfully.
     *
     * @param item The item to process.
     * @return A CompletableFuture that, when completed, will indicate that the item has been processed. This will contain
     * the result of the processing function applied to this item.
     */
    CompletableFuture<ResultType> process(ItemType item) {
        Preconditions.checkNotNull(item, "item");
        CompletableFuture<ResultType> result = null;
        synchronized (this.lock) {
            Exceptions.checkNotClosed(this.closed, this);
            if (this.failedStateException != null) {
                throw this.failedStateException;
            }
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
            result = processInternal(item);
        }

        return result;
    }

    /**
     * Clears any errors set by a failed execution.
     */
    void clearError() {
        synchronized (this.lock) {
            Exceptions.checkNotClosed(this.closed, this);
            this.failedStateException = null;
        }
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
        val failEx = new AtomicReference<Throwable>();
        synchronized (this.lock) {
            // Release the spot occupied by this item's execution.
            this.activeCount--;
            if (exception != null && this.failedStateException == null) {
                // Need to fail all future items and enter a Failed State to prevent new items from being processed.
                // We can only get out of the Failed state upon explicitly clearing the exception using clearError().
                this.failedStateException = new IllegalStateException("A previous item failed to commit. " +
                        "Cannot process new items until manually cleared.", exception);
                failEx.set(this.failedStateException);
                toFail = new ArrayList<>(this.pendingItems);
                this.pendingItems.clear();
            }
        }

        if (toFail != null) {
            toFail.forEach(qi -> qi.result.completeExceptionally(failEx.get()));
            return;
        }

        // We need to ensure the items are still executed in order. Once out of the main sync block, it is possible that
        // this callback may be invoked concurrently after various completions. As such, we need a special handler for
        // these, using its own synchronization, different from the main one, so that we don't hold that lock for too long.
        synchronized (this.queueProcessorLock) {
            while (true) {
                QueueItem toProcess;
                synchronized (this.lock) {
                    if (hasCapacity() && !this.pendingItems.isEmpty()) {
                        // We have spare capacity and we have something to process. Dequeue it and reserve the spot.
                        toProcess = this.pendingItems.pollFirst();
                        this.activeCount++;
                    } else {
                        // No capacity left or no more pending items.
                        break;
                    }
                }

                FutureHelpers.completeAfter(() -> processInternal(toProcess.data), toProcess.result);
            }
        }
    }

    private CompletableFuture<ResultType> processInternal(ItemType data) {
        try {
            val result = this.processor.apply(data);
            result.whenCompleteAsync((r, ex) -> executionComplete(ex), this.executor);
            return result;
        } catch (Throwable ex) {
            if (!ExceptionHelpers.mustRethrow(ex)) {
                executionComplete(ex);
            }

            throw ex;
        }
    }

    @GuardedBy("lock")
    private boolean hasCapacity() {
        return this.activeCount < this.capacity;
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

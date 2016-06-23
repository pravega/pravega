package com.emc.logservice.common;

import com.google.common.base.Preconditions;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

/**
 * Represents a thread-safe queue that dequeues all elements at once. Blocks the Dequeue if empty until new elements arrive.
 *
 * @param <T> The type of the items in the queue.
 */
public class BlockingDrainingQueue<T> implements AutoCloseable {
    //region Members

    private Queue<T> currentQueue;
    private final Object QueueLock = new Object();
    private CompletableFuture<Void> notEmptyWaiter;
    private boolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the BlockingDrainingQueue class.
     */
    public BlockingDrainingQueue() {
        swapQueue();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        CompletableFuture<Void> waitingFuture = null;
        synchronized (QueueLock) {
            if (!this.closed) {
                this.closed = true;
                waitingFuture = this.notEmptyWaiter;
                this.notEmptyWaiter = null;
            }
        }

        if (waitingFuture != null) {
            // CompletableFuture.cancel() may not interrupt the waiting thread, but it does cancel the future with a
            // CancellationException, which is what we want.
            waitingFuture.cancel(true);
        }
    }

    //endregion

    //region Operations

    /**
     * Adds a new item to the queue.
     *
     * @param item The item to append.
     * @throws ObjectClosedException If the Queue is closed.
     */
    public void add(T item) {
        CompletableFuture<Void> waitingFuture = null;
        synchronized (QueueLock) {
            Exceptions.checkNotClosed(this.closed, this);
            this.currentQueue.add(item);

            // See if we have someone waiting for the queue not to be empty anymore. If so, signal them.
            if (this.notEmptyWaiter != null) {
                waitingFuture = this.notEmptyWaiter;
            }
        }

        if (waitingFuture != null) {
            waitingFuture.complete(null);
        }
    }

    /**
     * Returns all items from the queue. If the queue is empty, it blocks the call until items are available.
     * At the end of this call, the queue will be empty.
     *
     * @return All the items currently in the queue.
     * @throws ObjectClosedException If the Queue is closed.
     * @throws IllegalStateException If another call to takeAllEntries is in progress.
     */
    public CompletableFuture<Queue<T>> takeAllEntries() {
        synchronized (QueueLock) {
            Exceptions.checkNotClosed(this.closed, this);
            Preconditions.checkState(this.notEmptyWaiter == null, "Another call to takeAllEntries is in progress. Cannot have more than one concurrent requests.");

            if (this.currentQueue.isEmpty()) {
                // There is nothing in the queue currently. Setup a future to be notified and then wait on that.
                CompletableFuture<Queue<T>> result = new CompletableFuture<>();
                this.notEmptyWaiter = new CompletableFuture<>();
                this.notEmptyWaiter.whenComplete((r, ex) -> {
                    if (ex != null) {
                        result.completeExceptionally(ex);
                    }
                    else {
                        Queue<T> queueItems = swapQueue();
                        assert queueItems != null && queueItems.size() > 0 : "Queue unblocked but without a result.";
                        result.complete(queueItems);
                    }
                });

                // When we are done with our result, whether normally, or internally canceled or externally cancelled,
                // we need to cleanup after ourselves to allow a subsequent call to takeAllEntries to succeed.
                result.whenComplete((r, ex) -> this.notEmptyWaiter = null);
                return result;
            }
            else {
                //Queue is not empty. Take whatever we have right now and return that.
                Queue<T> queueItems = swapQueue();
                return CompletableFuture.completedFuture(queueItems);
            }
        }
    }

    /**
     * Swaps the current queue with a new, empty one.
     *
     * @return The current (previous) queue.
     */
    private Queue<T> swapQueue() {
        Queue<T> newQueue = new LinkedList<>();
        Queue<T> oldQueue;
        synchronized (QueueLock) {
            oldQueue = this.currentQueue;
            this.currentQueue = newQueue;
        }

        return oldQueue;
    }

    //endregion
}

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

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.ObjectClosedException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import javax.annotation.concurrent.GuardedBy;

/**
 * Thread-safe queue that dequeues one or more elements at once. Provides asynchronous methods for waiting for items to
 * be added (if empty) and enables instantaneous dequeuing of all elements.
 *
 * @param <T> The type of the items in the queue.
 */
public abstract class AbstractDrainingQueue<T> {
    //region Members

    @GuardedBy("lock")
    private CompletableFuture<Queue<T>> pendingTake;
    @GuardedBy("lock")
    private boolean closed;
    private final Object lock = new Object();

    ///endregion

    //region Operations

    /**
     * Closes the queue and prevents any other access to it. Any blocked call to takeAllItems() will fail with InterruptedException.
     *
     * @return If the queue has any more items in it, these will be returned here in the order in which they were inserted.
     * The items are guaranteed not to be returned both here and via take()/poll().
     */
    public Queue<T> close() {
        CompletableFuture<Queue<T>> pending = null;
        Queue<T> result = new ArrayDeque<>();
        synchronized (this.lock) {
            if (!this.closed) {
                this.closed = true;
                pending = this.pendingTake;
                this.pendingTake = null;
                int remainingSize;
                while ((remainingSize = size()) > 0) {
                    // PriorityBlockingDrainingQueue returns only items of a given priority, so we may need to fetch
                    // multiple times until we fully drain it.
                    result.addAll(fetch(remainingSize));
                }
            }
        }

        // Cancel any pending poll request.
        if (pending != null) {
            pending.cancel(true);
        }

        return result;
    }

    /**
     * Cancels any pending Future from a take() operation.
     */
    public void cancelPendingTake() {
        CompletableFuture<Queue<T>> pending;
        synchronized (this.lock) {
            pending = this.pendingTake;
            this.pendingTake = null;
        }

        // Cancel any pending poll request.
        if (pending != null) {
            pending.cancel(true);
        }
    }

    /**
     * Adds a new item to the queue.
     *
     * @param item The item to add.
     * @throws ObjectClosedException If the Queue is closed.
     */
    public void add(T item) {
        CompletableFuture<Queue<T>> pending;
        Queue<T> result = null;
        synchronized (this.lock) {
            Exceptions.checkNotClosed(this.closed, this);
            addInternal(item);
            pending = this.pendingTake;
            this.pendingTake = null;
            if (pending != null) {
                result = fetch(size());
            }
        }

        if (pending != null) {
            pending.complete(result);
        }
    }

    /**
     * Returns the next items from the queue, if any.
     *
     * @param maxCount The maximum number of items to return.
     * @return A Queue containing at most maxCount items, or empty if there is nothing in the queue.
     * @throws IllegalStateException If there is a pending take() operation which hasn't completed yet.
     */
    public Queue<T> poll(int maxCount) {
        synchronized (this.lock) {
            Exceptions.checkNotClosed(this.closed, this);
            Preconditions.checkState(this.pendingTake == null, "Cannot call poll() when there is a pending take() request.");
            return fetch(maxCount);
        }
    }

    /**
     * Returns the next items from the queue. If the queue is empty, it blocks the call until at least one item is added.
     *
     * @param maxCount The maximum number of items to return. This argument will be ignored if the queue is currently empty,
     *                 but in that case the result will always be completed with exactly one element.
     * @return A CompletableFuture that, when completed, will contain the requested result. If the queue is not currently
     * empty, this Future will already be completed, otherwise it will be completed the next time the add() method is called.
     * If the queue is closed and this Future is not yet completed, it will be cancelled.
     * @throws ObjectClosedException If the Queue is closed.
     * @throws IllegalStateException If another call to take() is in progress.
     */
    public CompletableFuture<Queue<T>> take(int maxCount) {
        synchronized (this.lock) {
            Exceptions.checkNotClosed(this.closed, this);
            Preconditions.checkState(this.pendingTake == null, "Cannot have more than one concurrent pending take() request.");
            Queue<T> result = fetch(maxCount);
            if (result.size() > 0) {
                return CompletableFuture.completedFuture(result);
            } else {
                this.pendingTake = new CompletableFuture<>();
                return this.pendingTake;
            }
        }
    }

    /**
     * Returns (without removing) the first item in the Queue.
     *
     * @return The first item, or null if {@link #size()} is 0.
     */
    public T peek() {
        synchronized (this.lock) {
            return peekInternal();
        }
    }

    /**
     * Gets a value indicating the size of this queue.
     *
     * @return The size.
     */
    public int size() {
        synchronized (this.lock) {
            return sizeInternal();
        }
    }

    //endregion

    //region Abstract Methods

    /**
     * Updates the internal data structure to include the given item.
     * NOTE: this is invoked while holding the lock. There is no need for additional synchronization in the implementation.
     *
     * @param item The item to include.
     */
    protected abstract void addInternal(T item);

    /**
     * Returns the size of the queue.
     * NOTE: this is invoked while holding the lock. There is no need for additional synchronization in the implementation.
     *
     * @return The size of the queue.
     */
    protected abstract int sizeInternal();

    /**
     * Returns the first item in the Queue.
     * NOTE: this is invoked while holding the lock. There is no need for additional synchronization in the implementation.
     *
     * @return The first item, or null if {@link #size()} is 0.
     */
    protected abstract T peekInternal();

    /**
     * Extracts a number of items from the queue.
     * NOTE: this is invoked while holding the lock. There is no need for additional synchronization in the implementation.
     *
     * @param maxCount The maximum number of items to extract.
     * @return The extracted items, in order.
     */
    protected abstract Queue<T> fetch(int maxCount);

    //endregion
}

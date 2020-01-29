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
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Represents a thread-safe queue that dequeues all elements at once. Blocks the Dequeue if empty until new elements arrive.
 *
 * @param <T> The type of the items in the queue.
 */
@ThreadSafe
public class BlockingDrainingQueue<T> {
    //region Members

    @GuardedBy("contents")
    private final ArrayDeque<T> contents;
    @GuardedBy("contents")
    private CompletableFuture<Queue<T>> pendingTake;
    @GuardedBy("contents")
    private boolean closed;

    ///endregion

    // region Constructor

    /**
     * Creates a new instance of the BlockingDrainingQueue class.
     */
    public BlockingDrainingQueue() {
        this.contents = new ArrayDeque<>();
    }

    //endregion

    //region Operations

    /**
     * Closes the queue and prevents any other access to it. Any blocked call to takeAllItems() will fail with InterruptedException.
     *
     * @return If the queue has any more items in it, these will be returned here in the order in which they were inserted.
     * The items are guaranteed not to be returned both here and via take()/poll().
     */
    public Queue<T> close() {
        CompletableFuture<Queue<T>> pending = null;
        Queue<T> result = null;
        synchronized (this.contents) {
            if (!this.closed) {
                this.closed = true;
                pending = this.pendingTake;
                this.pendingTake = null;
                result = fetch(this.contents.size());
            }
        }

        // Cancel any pending poll request.
        if (pending != null) {
            pending.cancel(true);
        }

        return result != null ? result : new LinkedList<>();
    }

    /**
     * Cancels any pending Future from a take() operation.
     */
    public void cancelPendingTake() {
        CompletableFuture<Queue<T>> pending;
        synchronized (this.contents) {
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
        synchronized (this.contents) {
            Exceptions.checkNotClosed(this.closed, this);
            this.contents.addLast(item);
            pending = this.pendingTake;
            this.pendingTake = null;
            if (pending != null) {
                result = fetch(this.contents.size());
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
        synchronized (this.contents) {
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
        synchronized (this.contents) {
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
     * Gets a value indicating the size of this queue.
     *
     * @return The size.
     */
    public int size() {
        synchronized (this.contents) {
            return this.contents.size();
        }
    }

    @GuardedBy("contents")
    private Queue<T> fetch(int maxCount) {
        int count = Math.min(maxCount, this.contents.size());
        ArrayDeque<T> result = new ArrayDeque<>(count);
        while (result.size() < count) {
            result.addLast(this.contents.pollFirst());
        }

        return result;
    }

    // endregion
}

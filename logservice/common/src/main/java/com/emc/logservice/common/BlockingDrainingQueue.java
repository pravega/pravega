package com.emc.logservice.common;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Represents a thread-safe queue that dequeues all elements at once, and blocks the dequeue until elements arrive.
 *
 * @param <T> The type of the items in the queue.
 */
public class BlockingDrainingQueue<T> {
    //region Members

    private Queue<T> currentQueue;
    private final Object QueueLock = new Object();
    private CompletableFuture<Boolean> notEmpty = null;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the BlockingDrainingQueue class.
     */
    public BlockingDrainingQueue() {
        swapQueue();
    }

    //endregion

    //region Operations

    /**
     * Adds a new item to the queue.
     *
     * @param item The item to append.
     */
    public void add(T item) {
        CompletableFuture<Boolean> notEmpty = null;
        synchronized (QueueLock) {
            this.currentQueue.add(item);

            // See if we have someone waiting for the queue not to be empty anymore. If so, signal them.
            if (this.notEmpty != null) {
                notEmpty = this.notEmpty;
                this.notEmpty = null;
            }
        }

        if (notEmpty != null) {
            notEmpty.complete(true);
        }
    }

    /**
     * Returns all items from the queue. If the queue is empty, it blocks the call until items are available.
     * At the end of this call, the queue will be empty.
     *
     * @return All the items currently in the queue.
     * @throws InterruptedException If the call had to be blocked and it got cancelled in the meanwhile.
     */
    public Queue<T> takeAllEntries() throws InterruptedException {
        Queue<T> result;
        do {
            CompletableFuture<Boolean> toWait = null;
            synchronized (QueueLock) {
                // If we don't have any elements yet, setup a waiter until we have something.
                if (this.currentQueue.isEmpty()) {
                    if (this.notEmpty != null) {
                        // Someone else was waiting. Let's wait together.
                        toWait = this.notEmpty;
                    }
                    else {
                        // Nobody else was waiting. Create a new waiter and set it globally.
                        this.notEmpty = toWait = new CompletableFuture<>();
                    }
                }
            }

            if (toWait != null) {
                try {
                    toWait.get();
                }
                catch (ExecutionException ex) {
                    // We never complete this future exceptionally, so don't bother others with it.
                }
            }

            // Swap the current result with a new one and get the current one.
            result = swapQueue();
        } while (result.isEmpty()); // Try again in case someone beat us to it.

        return result;
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

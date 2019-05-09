/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.concurrent;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import javax.annotation.concurrent.GuardedBy;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * TODO: Javadoc
 */
public class AsyncSemaphore implements AutoCloseable {
    private final int totalCapacity;
    @GuardedBy("queue")
    private int usedCapacity;
    @GuardedBy("queue")
    private final ArrayDeque<DelayedTask> queue;
    @GuardedBy("queue")
    private boolean closed;

    AsyncSemaphore(int usedCapacity, int totalCapacity) {
        Preconditions.checkArgument(totalCapacity > 0, "totalCapacity must be a positive integer");
        Preconditions.checkArgument(usedCapacity >= 0, "usedCapacity must be a non-negative integer");
        this.totalCapacity = totalCapacity;
        this.usedCapacity = usedCapacity;
        this.queue = new ArrayDeque<>();
        this.closed = false;
    }

    @Override
    public void close() {
        List<DelayedTask> toCancel = null;
        synchronized (this.queue) {
            if (!this.closed) {
                toCancel = new ArrayList<>(this.queue);
                this.queue.clear();
                this.usedCapacity = 0;
                this.closed = true;
            }
        }

        if (toCancel != null) {
            toCancel.forEach(q -> q.result.cancel(true));
        }
    }

    public <T> CompletableFuture<T> acquire(int requestedCapacity, @NonNull Supplier<CompletableFuture<T>> task) {
        Preconditions.checkArgument(requestedCapacity >= 0 && requestedCapacity <= this.totalCapacity,
                "requestedCapacity must be a non-negative number smaller than or equal to %s.", this.totalCapacity);

        DelayedTask<T> qi;
        synchronized (this.queue) {
            Exceptions.checkNotClosed(this.closed, this);
            if (canExecute(requestedCapacity)) {
                qi = null;
                this.usedCapacity += requestedCapacity;
            } else {
                // Insufficient capacity; need to queue up and execute when more becomes available.
                qi = new DelayedTask<>(requestedCapacity, task);
                this.queue.addLast(qi);
            }

        }
        if (qi == null) {
            // We have more capacity than what this task requires. Execute now without queuing.
            return execute(task, requestedCapacity);
        } else {
            // This wil be completed when its associated task is executed.
            return qi.result;
        }
    }

    public void release(int releasedCapacity) {
        Preconditions.checkArgument(releasedCapacity >= 0, "releasedCapacity must be a non-negative number.");
        synchronized (this.queue) {
            Exceptions.checkNotClosed(this.closed, this);
            this.usedCapacity = Math.max(0, this.usedCapacity - releasedCapacity);
        }

        ArrayList<DelayedTask<?>> toExecute = new ArrayList<>();
        synchronized (this.queue) {
            while (!this.queue.isEmpty() && canExecute(this.queue.peekFirst().requestedCapacity)) {
                DelayedTask<?> qi = this.queue.removeFirst();
                this.usedCapacity += qi.requestedCapacity;
                toExecute.add(qi);
            }
        }

        toExecute.forEach(this::execute);
    }

    private <T> void execute(DelayedTask<T> qi) {
        execute(qi.runTask, qi.requestedCapacity)
                .whenComplete((r, ex) -> {
                    if (ex == null) {
                        qi.result.complete(r);
                    } else {
                        qi.result.completeExceptionally(ex);
                    }
                });
    }

    private <T> CompletableFuture<T> execute(Supplier<CompletableFuture<T>> toExecute, int requestedCapacity) {
        CompletableFuture<T> result;
        try {
            result = toExecute.get();
        } catch (Throwable ex) {
            // Synchronous termination. Capture the exception and return it as the result. There is no need to bubble it up.
            result = Futures.failedFuture(ex);
        }

        // If a task failed to execute, then it had no effect; release whatever capacity it reserved.
        Futures.exceptionListener(result, ex -> release(requestedCapacity));
        return result;
    }

    @GuardedBy("queue")
    private boolean canExecute(int requestedCapacity) {
        return this.usedCapacity + requestedCapacity <= this.totalCapacity;
    }

    @Override
    public String toString() {
        synchronized (this.queue) {
            return String.format("Capacity = %d/%d, Tasks = %d", this.usedCapacity, this.totalCapacity, this.queue.size());
        }
    }

    @VisibleForTesting
    int getUsedCapacity() {
        synchronized (this.queue) {
            return this.usedCapacity;
        }
    }

    @VisibleForTesting
    int getQueueSize() {
        synchronized (this.queue) {
            return this.queue.size();
        }
    }

    @RequiredArgsConstructor
    private static class DelayedTask<T> {
        final int requestedCapacity;
        final Supplier<CompletableFuture<T>> runTask;
        final CompletableFuture<T> result = new CompletableFuture<>();
    }
}

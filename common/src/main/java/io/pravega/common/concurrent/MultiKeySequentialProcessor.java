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
import io.pravega.common.ObjectClosedException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * Concurrent async processor that allows parallel execution of tasks with different keys, but serializes the execution
 * of tasks with the same key.
 *
 * @param <KeyType> Type of the Key.
 */
@ThreadSafe
@RequiredArgsConstructor
public class MultiKeySequentialProcessor<KeyType> implements AutoCloseable {
    private final Executor executor;
    @GuardedBy("queue")
    private final Map<KeyType, CompletableFuture<?>> queue = new HashMap<>();
    @GuardedBy("queue")
    private boolean closed = false;

    @Override
    public void close() {
        ArrayList<CompletableFuture<?>> toCancel = new ArrayList<>();
        synchronized (this.queue) {
            if (!this.closed) {
                toCancel = new ArrayList<>(this.queue.values());
                this.queue.clear();
                this.closed = true;
            }
        }

        if (toCancel.size() > 0) {
            toCancel.forEach(f -> f.completeExceptionally(new ObjectClosedException(this)));
        }
    }

    /**
     * Gets the number of concurrent tasks currently executing.
     */
    @VisibleForTesting
    public int getCurrentTaskCount() {
        synchronized (this.queue) {
            int size = this.queue.size();
            if (size > 0) {
                // Some tasks may have completed, but we haven't yet been able to clean them up.
                size -= this.queue.values().stream().filter(CompletableFuture::isDone).count();
            }

            return size;
        }
    }

    /**
     * Queues up a new task to execute, subject to the given dependency Keys.
     *
     * This task will not begin execution until all previous tasks for the given dependency Keys have finished.
     * In addition, no subsequent task for any of the given dependency Keys will begin executing until this task has
     * finished executing.
     *
     * @param keys         A Collection of {@link KeyType} objects representing the Keys that this task is dependent on.
     * @param toRun        A Supplier that will be invoked when it is this task's turn to run. It will return a
     *                     CompletableFuture that will complete when this task completes.
     * @param <ReturnType> Return type.
     * @return A CompletableFuture that will complete with the result from the CompletableFuture returned by toRun,
     * when toRun completes executing.
     */
    public <ReturnType> CompletableFuture<ReturnType> add(Collection<KeyType> keys, Supplier<CompletableFuture<? extends ReturnType>> toRun) {
        Preconditions.checkArgument(!keys.isEmpty(), "keys cannot be empty.");
        CompletableFuture<ReturnType> result = new CompletableFuture<>();
        Map<KeyType, CompletableFuture<?>> existingTasks = new HashMap<>();
        synchronized (this.queue) {
            Exceptions.checkNotClosed(this.closed, this);
            // Collect all currently executing tasks for the given keys.
            for (KeyType key : keys) {
                CompletableFuture<?> existingTask = this.queue.get(key);
                if (existingTask != null) {
                    existingTasks.put(key, existingTask);
                }
            }

            if (!existingTasks.isEmpty()) {
                // Another task is in progress for at least one key. Queue up behind them, and make sure to only start the
                // execution once all of those tasks are done.
                val tasks = existingTasks.values();
                CompletableFuture.allOf(tasks.toArray(new CompletableFuture[tasks.size()]))
                                 .whenCompleteAsync((r, ex) -> Futures.completeAfter(toRun, result), this.executor);
            }

            // Update the queues for each key to point to the latest task.
            keys.forEach(key -> this.queue.put(key, result));
        }

        if (existingTasks.isEmpty()) {
            // There were no previously running tasks for any of the given keys. Need to trigger its execution now,
            // outside of the synchronized block.
            Futures.completeAfter(toRun, result);
        }

        // Cleanup: if this was the last task in the queue, then clean up the queue.
        result.whenComplete((r, ex) -> cleanup(keys));
        return result;
    }

    private void cleanup(Collection<KeyType> keys) {
        synchronized (this.queue) {
            for (KeyType key : keys) {
                val last = this.queue.getOrDefault(key, null);
                if (last != null && last.isDone()) {
                    this.queue.remove(key);
                }
            }
        }
    }
}
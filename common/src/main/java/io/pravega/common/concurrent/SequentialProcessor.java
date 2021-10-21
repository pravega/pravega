/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.common.concurrent;

import io.pravega.common.Exceptions;
import io.pravega.common.ObjectClosedException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import javax.annotation.concurrent.GuardedBy;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Async processor that serializes the execution of tasks such that no two tasks execute at the same time.
 */
@RequiredArgsConstructor
public class SequentialProcessor implements AutoCloseable {
    @NonNull
    private final Executor executor;
    @GuardedBy("lock")
    private CompletableFuture<?> lastTask;
    @GuardedBy("lock")
    private boolean closed = false;
    private final Object lock = new Object();

    @Override
    public void close() {
        CompletableFuture<?> toCancel = null;
        synchronized (this.lock) {
            if (!this.closed) {
                toCancel = this.lastTask;
                this.lastTask = null;
                this.closed = true;
            }
        }

        if (toCancel != null) {
            toCancel.completeExceptionally(new ObjectClosedException(this));
        }
    }

    /**
     * Queues up a new task to execute.
     *
     * This task will not begin execution until all previous tasks have finished. In addition, no subsequent task
     * will begin executing until this task has finished executing.
     *
     * @param toRun        A Supplier that will be invoked when it is this task's turn to run. It will return a
     *                     CompletableFuture that will complete when this task completes.
     * @param <ReturnType> Return type.
     * @return A CompletableFuture that will complete with the result from the CompletableFuture returned by toRun,
     * when toRun completes executing.
     */
    public <ReturnType> CompletableFuture<ReturnType> add(Supplier<CompletableFuture<? extends ReturnType>> toRun) {
        CompletableFuture<ReturnType> result = new CompletableFuture<>();
        CompletableFuture<?> existingTask;
        synchronized (this.lock) {
            Exceptions.checkNotClosed(this.closed, this);
            existingTask = this.lastTask;
            if (existingTask != null) {
                // Another task is in progress. Queue up behind it, and make sure to only start the execution once
                // it is done.
                existingTask.whenCompleteAsync((r, ex) -> Futures.completeAfter(toRun, result), this.executor);
            }

            this.lastTask = result;
        }

        if (existingTask == null) {
            // There is no previously running task. Need to trigger its execution now, outside of the synchronized block.
            Futures.completeAfter(toRun, result);
        }

        // Cleanup: if no more tasks are registered after this one, then clean up the field.
        result.whenComplete((r, ex) -> {
            synchronized (this.lock) {
                if (this.lastTask != null && this.lastTask.isDone()) {
                    this.lastTask = null;
                }
            }
        });

        return result;
    }
}
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * A synchronization primitive that allows executing arbitrary (concurrent) tasks, where each task requires a well-known
 * number of credits, subject to a total number of credits being available. Each task's successful execution will "borrow"
 * its share of credits, and a task cannot execute if the number of credits available is insufficient. Credits may be
 * restored externally using the {@link #release} method.
 *
 * This is similar to {@link java.util.concurrent.Semaphore}, except that this class allows for asynchronous processing
 * and each task can request an arbitrary number of credits. It can be useful in solving problems making use of the
 * Leaky Bucket Algorithm (https://en.wikipedia.org/wiki/Leaky_bucket).
 */
@ThreadSafe
@Slf4j
public class AsyncSemaphore implements AutoCloseable {
    //region Members

    private final long totalCredits;
    private final String logId;
    @GuardedBy("queue")
    private long usedCredits;
    @GuardedBy("queue")
    private final ArrayDeque<PendingTask<?>> queue;
    @GuardedBy("queue")
    private boolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the {@link AsyncSemaphore} class.
     *
     * @param totalCredits Total number of available credits.
     * @param usedCredits  Initial number of used credits.
     * @param logId        A log-friendly identifier for this {@link AsyncSemaphore}.
     */
    public AsyncSemaphore(long totalCredits, long usedCredits, String logId) {
        Preconditions.checkArgument(totalCredits > 0, "totalCredits must be a positive integer");
        Preconditions.checkArgument(usedCredits >= 0, "usedCredits must be a non-negative integer");
        this.totalCredits = totalCredits;
        this.usedCredits = usedCredits;
        this.logId = logId;
        this.queue = new ArrayDeque<>();
        this.closed = false;
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        List<PendingTask<?>> toCancel = null;
        synchronized (this.queue) {
            if (!this.closed) {
                toCancel = new ArrayList<>(this.queue);
                this.queue.clear();
                this.usedCredits = 0;
                this.closed = true;
            }
        }

        if (toCancel != null && !toCancel.isEmpty()) {
            log.debug("AsyncSemaphore[{}]: Closing. Cancelling {} task(s).", logId, toCancel.size());
            toCancel.forEach(task -> task.result.cancel(true));
        }
    }

    //endregion

    //region Operations

    /**
     * Executes the given task which requires the given number of credits.
     *
     * If there are sufficient credits available for this task to run, it will be invoked synchronously and the returned
     * result is directly provided by the given task.
     *
     * If there are insufficient credits available for this task to run, it will be queued up and executed when credits
     * become available. There is no prioritization of queued tasks - they are triggered in the order in which they
     * are queued up.
     *
     * If the {@code force} flag is set, then the task will be invoked synchronously, even if there are insufficient
     * credits. In this case, the {@link #getUsedCredits()} will exceed the max allowed credits and no other (non-forced)
     * task will be allowed to execute until {@link #getUsedCredits()} falls below the max allowed.
     *
     * A task will allocate the requested credits when it is triggered. If the task fails (synchronously or asynchronously),
     * then the requested credits are automatically released back into the pool. If the task succeeds, the credits will
     * remain.
     *
     * @param task    A {@link Supplier} that, when invoked, will execute the task.
     * @param credits The number of credits this task requires.
     * @param force   If true, the task will be executed synchronously regardless of how many credits are available. The
     *                task's credits are still recorded in this case.
     * @param <T>     Return type.
     * @return A CompletableFuture that, when completed, will contain the result of the executed task. If the task failed
     * or was rejected (i.e., due to {@link AsyncSemaphore} closing), it will be failed with the appropriate exception.
     */
    public <T> CompletableFuture<T> run(@NonNull Supplier<CompletableFuture<T>> task, long credits, boolean force) {
        Preconditions.checkArgument(credits >= 0 && credits <= this.totalCredits,
                "credits must be a non-negative number smaller than or equal to %s.", this.totalCredits);

        PendingTask<T> pt;
        synchronized (this.queue) {
            Exceptions.checkNotClosed(this.closed, this);
            if (force || canExecute(credits)) {
                pt = null;
                this.usedCredits += credits;
                log.trace("AsyncSemaphore[{}]: Task run. Credits={}, TotalUsedCredits={}, Forced={}.", this.logId, credits, this.usedCredits, force);
            } else {
                // Insufficient credits; need to queue up and execute when more becomes available.
                pt = new PendingTask<>(credits, task);
                this.queue.addLast(pt);
                log.debug("AsyncSemaphore[{}]: Task blocked. Credits={}, TotalUsedCredits={}, QueueSize={}.",
                        this.logId, credits, this.usedCredits, this.queue.size());
            }
        }

        if (pt == null) {
            // We have more credits than what this task requires. Execute now without queuing.
            return execute(task, credits);
        } else {
            // This wil be completed when its associated task is executed.
            return pt.result;
        }
    }

    /**
     * Releases a number of credits back into the pool and initiates the execution of any pending tasks that are now
     * eligible to run.
     *
     * @param credits The number of credits to release. This number will be capped at the number of currently used
     *                credits ({@link #getUsedCredits()}).
     */
    public void release(long credits) {
        Preconditions.checkArgument(credits >= 0, "credits must be a non-negative number.");
        synchronized (this.queue) {
            Exceptions.checkNotClosed(this.closed, this);
            this.usedCredits = Math.max(0, this.usedCredits - credits);
            log.trace("AsyncSemaphore[{}]: Release. Credits={}, TotalUsedCredits={}.", this.logId, credits, this.usedCredits);
        }

        ArrayList<PendingTask<?>> toExecute = new ArrayList<>();
        synchronized (this.queue) {
            while (!this.queue.isEmpty() && canExecute(this.queue.peekFirst().credits)) {
                PendingTask<?> qi = this.queue.removeFirst();
                this.usedCredits += qi.credits;
                toExecute.add(qi);
                log.debug("AsyncSemaphore[{}]: Task unblocked. Credits={}, TotalUsedCredits={}, QueueSize={}.",
                        this.logId, qi.credits, this.usedCredits, this.queue.size());
            }
        }

        toExecute.forEach(this::execute);
    }

    private <T> void execute(PendingTask<T> qi) {
        execute(qi.runTask, qi.credits)
                .whenComplete((r, ex) -> {
                    if (ex == null) {
                        qi.result.complete(r);
                    } else {
                        qi.result.completeExceptionally(ex);
                    }
                });
    }

    private <T> CompletableFuture<T> execute(Supplier<CompletableFuture<T>> toExecute, long credits) {
        CompletableFuture<T> result;
        try {
            result = toExecute.get();
        } catch (Throwable ex) {
            // Synchronous termination. Capture the exception and return it as the result. There is no need to bubble it up.
            result = Futures.failedFuture(ex);
        }

        // If a task failed to execute, then it had no effect; release whatever credits it reserved.
        Futures.exceptionListener(result, ex -> release(credits));
        return result;
    }

    @GuardedBy("queue")
    private boolean canExecute(long credits) {
        return this.usedCredits + credits <= this.totalCredits;
    }

    @Override
    public String toString() {
        synchronized (this.queue) {
            return String.format("Credits = %d/%d, Tasks = %d", this.usedCredits, this.totalCredits, this.queue.size());
        }
    }

    /**
     * Gets the current number of used credits.
     * @return The current number of used credits.
     */
    public long getUsedCredits() {
        synchronized (this.queue) {
            return this.usedCredits;
        }
    }

    @VisibleForTesting
    int getQueueSize() {
        synchronized (this.queue) {
            return this.queue.size();
        }
    }

    @RequiredArgsConstructor
    private static class PendingTask<T> {
        final long credits;
        final Supplier<CompletableFuture<T>> runTask;
        final CompletableFuture<T> result = new CompletableFuture<>();
    }

    //endregion
}

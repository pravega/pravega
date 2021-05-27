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
import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

/**
 * Processes items in order, subject to capacity constraints.
 */
@ThreadSafe
public class OrderedProcessor<ResultType> implements AutoCloseable {
    //region members

    private final int capacity;
    @GuardedBy("stateLock")
    private final Deque<QueueItem> pendingItems;
    private final Executor executor;

    /**
     * Guards access to the {@link OrderedProcessor}'s internal state (counts and queues).
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

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the {@link OrderedProcessor} class.
     *
     * @param capacity The maximum number of concurrent executions.
     * @param executor An Executor for async invocations.
     */
    public OrderedProcessor(int capacity, @NonNull Executor executor) {
        Preconditions.checkArgument(capacity > 0, "capacity must be a non-negative number.");
        this.capacity = capacity;
        this.executor = executor;
        this.pendingItems = new ArrayDeque<>();
        this.activeCount = 0;
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    @SneakyThrows(Exception.class)
    public void close() {
        val toCancel = new ArrayList<QueueItem>();
        synchronized (this.stateLock) {
            if (this.closed) {
                return;
            }

            toCancel.addAll(this.pendingItems);
            this.pendingItems.clear();
            this.closed = true;
        }

        toCancel.forEach(i -> i.result.cancel(true));
    }

    //endregion

    //region Processing

    /**
     * Executes the given item.
     * * If {@link #hasCapacity()} is true, the item will be executed immediately and the returned future is the actual
     * result from {@code toRun}.
     * * If {@link #hasCapacity()} is false, the item will be queued up and it will be processed when capacity allows,
     * after all the items added before it have been executed (whether exceptionally or with a successful outcome).
     * * This method guarantees ordered execution as long as its invocations are serial. That is, it only guarantees that
     * the item has begun processing or an order assigned if the method returned successfully.
     *
     * @param toRun A {@link Supplier} that, when invoked, will return a {@link CompletableFuture} which will indicate the
     *              outcome of the operation to execute.
     * @return A CompletableFuture that, when completed, will indicate that the item has been processed. This will contain
     * the result of the processing function applied to this item.
     */
    public CompletableFuture<ResultType> execute(@NonNull Supplier<CompletableFuture<ResultType>> toRun) {
        CompletableFuture<ResultType> result = null;
        synchronized (this.stateLock) {
            Exceptions.checkNotClosed(this.closed, this);
            if (hasCapacity() && this.pendingItems.isEmpty()) {
                // Not at capacity. Reserve a spot now.
                this.activeCount++;
            } else {
                // We are at capacity or have a backlog. Put the item in the queue and return its associated future.
                result = new CompletableFuture<>();
                this.pendingItems.add(new QueueItem(toRun, result));
            }
        }

        if (result == null) {
            // We are below max capacity, so simply process the item, without queuing up.
            // It is possible, under some conditions, that we may get in here when the queue empties up (by two spots at
            // the same time). In that case, we need to acquire the processing lock to ensure that we don't accidentally
            // process this item before we finish processing the last item in the queue.
            synchronized (this.processingLock) {
                result = processInternal(toRun);
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
        synchronized (this.stateLock) {
            // Release the spot occupied by this item's execution.
            this.activeCount--;
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

                // Futures.completeAfter will invoke processInternal synchronously, so it will still be within declared guards.
                Futures.completeAfter(() -> processInternal(toProcess.toRun), toProcess.result);
            }
        }
    }

    @GuardedBy("processingLock")
    private CompletableFuture<ResultType> processInternal(Supplier<CompletableFuture<ResultType>> toRun) {
        try {
            val result = toRun.get();
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

    //region QueueItem

    @RequiredArgsConstructor
    private class QueueItem {
        final Supplier<CompletableFuture<ResultType>> toRun;
        final CompletableFuture<ResultType> result;
    }

    //endregion
}

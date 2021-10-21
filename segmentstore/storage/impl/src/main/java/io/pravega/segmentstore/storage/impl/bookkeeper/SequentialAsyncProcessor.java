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
package io.pravega.segmentstore.storage.impl.bookkeeper;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.function.Callbacks;
import io.pravega.common.util.Retry;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import javax.annotation.concurrent.GuardedBy;

/**
 * An Executor extension that runs the same task asynchronously, but never concurrently. If multiple requests are made
 * during an existing execution of the task, it will be invoked exactly once after the current execution completes.
 */
class SequentialAsyncProcessor implements AutoCloseable {
    //region Members

    private final Runnable runnable;
    private final Retry.RetryAndThrowBase<? extends Throwable> retry;
    private final Consumer<Throwable> failureCallback;
    private final ScheduledExecutorService executor;
    @GuardedBy("this")
    private boolean running;
    @GuardedBy("this")
    private boolean runAgain;
    @GuardedBy("this")
    private boolean closed;

    //endregion

    //region Constructor

    /**
     * Region Constructor.
     *
     * @param runnable        The task to run.
     * @param retry           A Retry policy to use when executing the runnable.
     * @param failureCallback A Consumer to invoke if the runnable was unable to complete after applying the Retry policy.
     * @param executor        An Executor to run the task on.
     */
    SequentialAsyncProcessor(Runnable runnable, Retry.RetryAndThrowBase<? extends Throwable> retry, Consumer<Throwable> failureCallback, ScheduledExecutorService executor) {
        this.runnable = Preconditions.checkNotNull(runnable, "runnable");
        this.retry = Preconditions.checkNotNull(retry, "retry");
        this.failureCallback = Preconditions.checkNotNull(failureCallback, "failureCallback");
        this.executor = Preconditions.checkNotNull(executor, "executor");
    }

    //endregion

    //region Execution

    /**
     * Executes one instance of the task, or queues it up at most once should the task be currently running.
     */
    void runAsync() {
        // Determine if a task is running. If so, record the fact we want to have it run again, otherwise reserve our spot.
        synchronized (this) {
            Exceptions.checkNotClosed(this.closed, this);
            if (this.running) {
                this.runAgain = true;
                return;
            }

            this.running = true;
        }

        // Execute the task.
        runInternal();
    }

    private void runInternal() {
        this.retry.runInExecutor(this.runnable, this.executor)
                  .whenComplete((r, ex) -> {
                      if (ex != null) {
                          // If we were unable to execute after all retries, invoke the failure callback.
                          Callbacks.invokeSafely(this.failureCallback, ex, null);
                      }

                      boolean loopAgain;
                      synchronized (this) {
                          this.running = this.runAgain && !this.closed;
                          this.runAgain = false;
                          loopAgain = this.running;
                      }
                      if (loopAgain) {
                          runInternal();
                      }
                  });
    }

    @Override
    public synchronized void close() {
        this.closed = true;
    }

    //endregion
}

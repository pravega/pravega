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

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractService;
import io.pravega.common.Exceptions;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

/**
 * Base Service that runs asynchronously using a thread pool.
 */
@Slf4j
public abstract class AbstractThreadPoolService extends AbstractService implements AutoCloseable {
    //region Members

    protected final String traceObjectId;
    protected final ScheduledExecutorService executor;
    private final AtomicReference<Throwable> stopException;
    private CompletableFuture<Void> runTask;
    private final AtomicBoolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the AbstractThreadPoolService class.
     *
     * @param traceObjectId An identifier to use for logging purposes. This will be included at the beginning of all
     *                      log calls initiated by this Service.
     * @param executor      The Executor to use for async callbacks and operations.
     */
    public AbstractThreadPoolService(String traceObjectId, ScheduledExecutorService executor) {
        Preconditions.checkNotNull(executor, "executor");
        this.traceObjectId = traceObjectId;
        this.executor = executor;
        this.stopException = new AtomicReference<>();
        this.closed = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.get()) {
            Futures.await(Services.stopAsync(this, this.executor));
            log.info("{}: Closed.", this.traceObjectId);
            this.closed.set(true);
        }
    }

    //endregion

    //region AbstractService Implementation

    @Override
    protected void doStart() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        notifyStarted();
        log.info("{}: Started.", this.traceObjectId);
        this.runTask = doRun();
        this.runTask.whenComplete((r, ex) -> {
            // Handle any exception that may have been thrown.
            if (ex != null
                    && !(Exceptions.unwrap(ex) instanceof CancellationException && state() != State.RUNNING)) {
                // We ignore CancellationExceptions while shutting down - those are expected.
                errorHandler(ex);
            }

            // Make sure the service is stopped when the runTask is done (whether successfully or not).
            if (state() == State.RUNNING) {
                stopAsync();
            }
        });
    }

    @Override
    protected void doStop() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        log.info("{}: Stopping.", this.traceObjectId);

        if (this.runTask == null) {
            notifyStoppedOrFailed(null);
        } else if (this.runTask.isDone()) {
            // Our runTask is done. See if it completed normally or not.
            notifyStoppedOrFailed(Futures.getException(this.runTask));
        } else {
            // Still running. Wait (async) for the task to complete or a timeout to expire.
            CompletableFuture
                    .anyOf(this.runTask, Futures.delayedFuture(getShutdownTimeout(), this.executor))
                    .whenComplete((r, ex) -> {
                        if (ex != null) {
                            ex = Exceptions.unwrap(ex);
                        }

                        if (ex == null && !this.runTask.isDone()) {
                            // Still no exception, but our service did not properly shut down.
                            ex = new TimeoutException("Timeout expired while waiting for the Service to shut down.");

                        }

                        this.runTask = null;
                        notifyStoppedOrFailed(ex);
                    });
        }
    }

    /**
     * Notifies the AbstractService to enter the TERMINATED or FAILED state, based on the current state of the Service
     * and the given Exception.
     *
     * @param runException (Optional) A Throwable that indicates the failure cause of runTask.
     */
    private void notifyStoppedOrFailed(Throwable runException) {
        final Throwable stopException = this.stopException.get();
        if (runException == null) {
            // If runTask did not fail with an exception, see if we have a general stopException.
            runException = stopException;
        }

        if (runException instanceof CancellationException) {
            // CancellationExceptions are expected if we are shutting down the service; If this was the real
            // cause why runTask failed, then the service did not actually fail, so do not report a bogus exception.
            runException = null;
        }

        if (runException == null) {
            // Normal shutdown.
            notifyStopped();
        } else {
            if (stopException != null && stopException != runException) {
                // We have both a stopException and a runTask exception. Since the stopException came first, that is the
                // one we need to fail with, so add the runTask exception as suppressed.
                stopException.addSuppressed(runException);
                runException = stopException;
            }

            // Shutdown caused by some failure.
            notifyFailed(runException);

        }

        log.info("{}: Stopped.", this.traceObjectId);
    }

    //endregion

    /**
     * Gets a value indicating how much to wait for the service to shut down, before failing it.
     *
     * @return The Duration.
     */
    protected abstract Duration getShutdownTimeout();

    /**
     * Main execution of the Service. When this Future completes, the service auto-shuts down.
     *
     * @return A CompletableFuture that, when completed, indicates the service is terminated. If the Future completed
     * exceptionally, the Service will shut down with failure, otherwise it will terminate normally.
     */
    protected abstract CompletableFuture<Void> doRun();

    /**
     * Gets a pointer to the current Stop Exception, if any is set.
     *
     * @return The result.
     */
    protected Throwable getStopException() {
        return this.stopException.get();
    }

    /**
     * When overridden in a derived class, handles an exception that is caught by the execution of run().
     * The default implementation simply records the exception in the stopException field, which will then be used
     * to fail the service when it shuts down.
     *
     * @param ex The Exception.
     */
    protected void errorHandler(Throwable ex) {
        this.stopException.compareAndSet(null, ex);
    }
}

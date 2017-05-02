/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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

import io.pravega.common.ExceptionHelpers;
import io.pravega.common.Exceptions;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractService;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
            stopAsync();
            ServiceShutdownListener.awaitShutdown(this, false);

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
                    && !(ExceptionHelpers.getRealException(ex) instanceof CancellationException && state() != State.RUNNING)) {
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

        this.executor.execute(() -> {
            Throwable cause = this.stopException.get();

            // Cancel the last iteration and wait for it to finish.
            if (this.runTask != null) {
                try {
                    // This doesn't actually cancel the task. We need to plumb through the code with 'checkRunning' to
                    // make sure we stop any long-running tasks.
                    this.runTask.get(getShutdownTimeout().toMillis(), TimeUnit.MILLISECONDS);
                    this.runTask = null;
                } catch (Exception ex) {
                    if (cause != null) {
                        cause = ex;
                    }
                }
            }

            if (cause == null) {
                // Normal shutdown.
                notifyStopped();
            } else {
                // Shutdown caused by some failure.
                notifyFailed(cause);
            }

            log.info("{}: Stopped.", this.traceObjectId);
        });
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
     * Gets a value indicating whether a stop exception has been set.
     *
     * @return The result.
     */
    protected boolean hasStopException() {
        return this.stopException.get() != null;
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

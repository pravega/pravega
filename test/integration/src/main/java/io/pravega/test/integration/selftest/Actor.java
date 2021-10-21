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
package io.pravega.test.integration.selftest;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractService;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.concurrent.Services;
import io.pravega.test.integration.selftest.adapters.StoreAdapter;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Base class for any component that executes as part of the Self Tester.
 */
abstract class Actor extends AbstractService implements AutoCloseable {
    // region Members

    private static final Duration INITIAL_DELAY = Duration.ofMillis(500);
    protected final TestConfig config;
    protected final StoreAdapter store;
    protected final ScheduledExecutorService executorService;
    private CompletableFuture<Void> runTask;
    private final AtomicBoolean closed;
    private final AtomicReference<Throwable> stopException;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the Actor class.
     *
     * @param config          Test Configuration.
     * @param store           A StoreAdapter to execute operations on.
     * @param executorService The Executor Service to use for async tasks.
     */
    Actor(TestConfig config, StoreAdapter store, ScheduledExecutorService executorService) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(store, "store");
        Preconditions.checkNotNull(executorService, "executorService");

        this.config = config;
        this.store = store;
        this.executorService = executorService;
        this.closed = new AtomicBoolean();
        this.stopException = new AtomicReference<>();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.get()) {
            Futures.await(Services.stopAsync(this, this.executorService));
            this.closed.set(true);
        }
    }

    //endregion

    //region AbstractService Implementation

    @Override
    protected void doStart() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        notifyStarted();
        this.runTask = Futures
                .delayedFuture(INITIAL_DELAY, this.executorService)
                .thenCompose(v -> run());
        this.runTask.whenComplete((r, ex) -> stopAsync());
    }

    @Override
    protected void doStop() {
        Exceptions.checkNotClosed(this.closed.get(), this);

        this.executorService.execute(() -> {
            // Cancel the last iteration and wait for it to finish.
            Throwable failureCause = this.stopException.get();
            if (this.runTask != null) {
                try {
                    // Wait for the main task to complete before shutting down.
                    this.runTask.get(this.config.getTimeout().toMillis(), TimeUnit.MILLISECONDS);
                } catch (Throwable ex) {
                    ex = Exceptions.unwrap(ex);
                    if (failureCause != null) {
                        TestLogger.log(getLogId(), "Original Failure: %s.", failureCause);
                        failureCause = ex;
                    }
                }
            }

            if (failureCause == null) {
                notifyStopped();
            } else {
                TestLogger.log(getLogId(), "Failed: %s.", failureCause);
                notifyFailed(failureCause);
            }
        });
    }

    //endregion

    /**
     * Executes the role of this Actor.
     */
    protected abstract CompletableFuture<Void> run();

    /**
     * Gets a value indicating the Id to use in logging for this Actor.
     */
    protected abstract String getLogId();

    /**
     * Immediately stops the Actor and fails it with the given exception.
     */
    protected void fail(Throwable cause) {
        this.stopException.set(cause);
        stopAsync();
    }

    @Override
    public String toString() {
        return getLogId();
    }
}

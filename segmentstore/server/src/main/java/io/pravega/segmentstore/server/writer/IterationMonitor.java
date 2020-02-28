/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.writer;

import com.google.common.base.Preconditions;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * Helps monitor {@link StorageWriter} iterations. Schedules a Fixed-Rate Task on an Executor that, when executed, requests
 * the Idle Duration for the currently running Storage Writer Iteration and. If the Idle Duration exceeds a preset limit,
 * invokes a callback to notify listeners that such an event occurred.
 */
@Slf4j
@RequiredArgsConstructor
class IterationMonitor implements AutoCloseable {
    //region Members

    private final Supplier<Duration> getIdleDuration;
    private final Duration maxIdleDuration;
    private final Runnable onIdleTimeout;
    private final ScheduledFuture<?> monitor;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the {@link IterationMonitor} class.
     *
     * @param getIdleDuration A {@link Supplier} that, when invoked, returns a {@link Duration} representing the current
     *                        Idle Time for the Storage Writer Iteration.
     * @param checkInterval   The interval between checks.
     * @param maxIdleDuration The maximum allowed Idle Time.
     * @param onIdleTimeout   A callback that will be invoked if getIdleDuration returns a value equal to or larger than
     *                        maxIdleDuration.
     * @param executor        An Executor for running the monitor on.
     */
    IterationMonitor(@NonNull Supplier<Duration> getIdleDuration, @NonNull Duration checkInterval, @NonNull Duration maxIdleDuration,
                     @NonNull Runnable onIdleTimeout, @NonNull ScheduledExecutorService executor) {
        Preconditions.checkArgument(!checkInterval.isNegative() && !checkInterval.isZero());
        Preconditions.checkArgument(!maxIdleDuration.isNegative() && !maxIdleDuration.isZero());
        this.getIdleDuration = getIdleDuration;
        this.maxIdleDuration = maxIdleDuration;
        this.onIdleTimeout = onIdleTimeout;
        this.monitor = executor.scheduleAtFixedRate(this::runOnce, checkInterval.toMillis(), checkInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    //endregion

    //region Operations

    @Override
    public void close() {
        this.monitor.cancel(true);
    }

    private boolean isClosed() {
        return this.monitor.isCancelled();
    }

    private void runOnce() {
        if (isClosed()) {
            // Nothing to do.
            return;
        }

        val idleDuration = this.getIdleDuration.get();
        if (idleDuration.compareTo(this.maxIdleDuration) < 0) {
            // Nothing to do.
            return;
        }

        // We are done. Invoke the callback and close.
        log.info("Maximum Iteration Idle Time expired. Invoking callback and closing.");
        invokeCallback();
    }

    private void invokeCallback() {
        try {
            this.onIdleTimeout.run();
        } catch (Throwable ex) {
            // This is run in an Executor. Since nobody is listening for exceptions, the best thing we can do is log them.
            log.error("Unable to invoke on-timeout callback.", ex);
        }
    }

    //endregion
}

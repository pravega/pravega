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
import io.pravega.common.Exceptions;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * Helps monitor {@link StorageWriter} iterations.
 */
@Slf4j
class IterationMonitor implements AutoCloseable {
    private final TaskTracker tracker;
    private final Duration maxIterationDuration;
    private final Runnable onTimeout;
    private final ScheduledExecutorService executor;
    private final AtomicBoolean closed;
    private final AtomicReference<ScheduledFuture<?>> monitor;

    IterationMonitor(@NonNull TaskTracker tracker, @NonNull Duration maxAllowedElapsed, @NonNull Runnable onTimeout,
                     @NonNull ScheduledExecutorService executor) {
        Preconditions.checkArgument(!maxAllowedElapsed.isNegative() && !maxAllowedElapsed.isZero());
        this.tracker = tracker;
        this.maxIterationDuration = maxAllowedElapsed;
        this.onTimeout = onTimeout;
        this.executor = executor;
        this.closed = new AtomicBoolean();
        this.monitor = new AtomicReference<>();
    }

    @Override
    public void close() {
        this.closed.set(true);
        val m = this.monitor.getAndSet(null);
        m.cancel(true);
    }

    public void start() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkState(this.monitor.get() == null, "Already started.");
        runOnce();
    }

    private long getDelayMillis() {
        return this.maxIterationDuration.minus(this.tracker.getLongestPendingDuration()).toMillis();
    }

    private void runOnce() {
        if (this.closed.get()) {
            // Nothing to do.
            return;
        }

        long newDelay = getDelayMillis();
        if (newDelay <= 0) {
            // We are done. Invoke the callback and close.
            log.info("Timeout expired. Invoking callback and closing.");
            invokeCallback();
            close();
            return;
        }

        val f = this.executor.schedule(() -> {
            try {
                runOnce();
            } catch (Throwable ex) {
                log.error("Caught exception during execution.", ex);
                close();
            }
        }, newDelay, TimeUnit.MILLISECONDS);

        val previous = this.monitor.getAndSet(f);
        if (previous != null) {
            previous.cancel(true);
        }
    }

    private void invokeCallback() {
        try {
            this.onTimeout.run();
        } catch (Throwable ex) {
            log.error("Unable to invoke on-timeout callback.", ex);
        }
    }
}

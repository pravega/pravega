/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.logs;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.server.CacheUtilizationProvider;
import io.pravega.segmentstore.server.SegmentStoreMetrics;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * Throttling utilities for the {@link OperationProcessor} class.
 */
@Slf4j
class Throttler implements CacheUtilizationProvider.CleanupListener, AutoCloseable {
    //region Members

    private final ThrottlerCalculator throttlerCalculator;
    private final String traceObjectId;
    private final ScheduledExecutorService executor;
    private final SegmentStoreMetrics.OperationProcessor metrics;
    private final AtomicReference<InterruptibleDelay> currentDelay;
    private final AtomicBoolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the {@link Throttler} class.
     *
     * @param containerId Id of the Segment Container for this Throttler. Used for logging purposes only.
     * @param calculator  A {@link ThrottlerCalculator} to be used for determining how much to throttle.
     * @param executor    An Executor for async operations.
     * @param metrics     Metrics for reporting.
     */
    Throttler(int containerId, @NonNull ThrottlerCalculator calculator, @NonNull ScheduledExecutorService executor,
              @NonNull SegmentStoreMetrics.OperationProcessor metrics) {
        this.throttlerCalculator = calculator;
        this.executor = executor;
        this.metrics = metrics;
        this.traceObjectId = String.format("Throttler[%d]", containerId);
        this.currentDelay = new AtomicReference<>();
        this.closed = new AtomicBoolean(false);
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        this.closed.set(true);
    }

    //endregion

    //region CacheUtilizationProvider.CleanupListener Implementation

    @Override
    public void cacheCleanupComplete() {
        val currentDelay = this.currentDelay.get();
        if (currentDelay != null && isInterruptible(currentDelay.source)) {
            // We were actively throttling due to a reason that is eligible for re-throttling. Terminate the current
            // throttle cycle, which should force the throttler to re-evaluate the situation.
            log.debug("{}: Cache Cleanup complete while actively throttling ({}).", this.traceObjectId, currentDelay);
            currentDelay.delayFuture.completeExceptionally(new ThrottlingInterruptedException());
        }
    }

    @Override
    public boolean isClosed() {
        return this.closed.get();
    }

    //endregion

    //region Throttling

    /**
     * Determines whether there is an immediate need to introduce a throttling delay.
     *
     * @return True if throttling is required, false otherwise.
     */
    boolean isThrottlingRequired() {
        return this.throttlerCalculator.isThrottlingRequired();
    }

    /**
     * Throttles if necessary using the {@link ThrottlerCalculator} passed to this class' constructor as input.
     *
     * @return A CompletableFuture that is either already completed (if no throttling is required) or will complete at
     * some time in the future based on the throttling delay value returned by {@link ThrottlerCalculator}.
     */
    CompletableFuture<Void> throttle() {
        val delay = new AtomicReference<ThrottlerCalculator.DelayResult>(this.throttlerCalculator.getThrottlingDelay());
        if (!delay.get().isMaximum()) {
            // We are not delaying the maximum amount. We only need to do this once.
            val existingDelay = this.currentDelay.get();
            if (existingDelay != null && existingDelay.source == delay.get().getThrottlerName()) {
                // Looks like a previous throttle cycle was interrupted (due to cache evictions). In order to prevent
                // endless throttling (cache evictions happen frequently and there is no guarantee that the size of the
                // cache actually decreased in the meantime), we should only wait the minimum of the newly calculated
                // delay and whatever was left from the previous one (if anything).
                int remaining = (int) existingDelay.remaining.getRemaining().toMillis();
                if (remaining > 0 && remaining < delay.get().getDurationMillis()) {
                    delay.set(delay.get().withNewDelay(remaining));
                }
            }

            return throttleOnce(delay.get());
        } else {
            // The initial delay calculation indicated that we need to throttle to the maximum, which means there's
            // significant pressure. In order to protect downstream components, we need to run in a loop and delay as much
            // as needed until the pressure is relieved.
            return Futures.loop(
                    () -> delay.get().isMaximum(),
                    () -> throttleOnce(delay.get())
                            .thenRun(() -> delay.set(this.throttlerCalculator.getThrottlingDelay())),
                    this.executor);
        }
    }

    private CompletableFuture<Void> throttleOnce(ThrottlerCalculator.DelayResult delay) {
        this.metrics.processingDelay(delay.getDurationMillis());
        if (delay.isMaximum() || delay.getThrottlerName() == ThrottlerCalculator.ThrottlerName.CommitBacklog) {
            // Increase logging visibility if we throttle at the maximum limit (which means we're likely to fully block
            // processing of operations) or if this is due to the Commit Processor not being able to keep up.
            log.warn("{}: Processing delay = {}.", this.traceObjectId, delay);
        } else {
            log.debug("{}: Processing delay = {}.", this.traceObjectId, delay);
        }

        val delayFuture = createDelayFuture(delay.getDurationMillis());
        if (isInterruptible(delay.getThrottlerName())) {
            // This throttling source is eligible for interruption. Set it up so that a call to cacheCleanupComplete()
            // may find it and act on it.
            val result = new InterruptibleDelay(delayFuture, delay.getDurationMillis(), delay.getThrottlerName());
            this.currentDelay.set(result);
            return Futures
                    .exceptionallyComposeExpecting(
                            result.delayFuture,
                            ex -> ex instanceof ThrottlingInterruptedException,
                            this::throttle)
                    .whenComplete((r, e) -> this.currentDelay.set(null));
        } else {
            return delayFuture;
        }
    }

    private boolean isInterruptible(ThrottlerCalculator.ThrottlerName name) {
        return name == ThrottlerCalculator.ThrottlerName.Cache;
    }

    @VisibleForTesting
    protected CompletableFuture<Void> createDelayFuture(int millis) {
        return Futures.delayedFuture(Duration.ofMillis(millis), this.executor);
    }

    //endregion

    //region Helper Classes

    private static class InterruptibleDelay {
        final CompletableFuture<Void> delayFuture;
        final ThrottlerCalculator.ThrottlerName source;
        final TimeoutTimer remaining;

        InterruptibleDelay(CompletableFuture<Void> delayFuture, int delayMillis, ThrottlerCalculator.ThrottlerName source) {
            this.delayFuture = delayFuture;
            this.source = source;
            this.remaining = new TimeoutTimer(Duration.ofMillis(delayMillis));
        }

        @Override
        public String toString() {
            return String.format("Source = %s, Remaining = %d", this.source, this.remaining.getRemaining().toMillis());
        }
    }

    private static class ThrottlingInterruptedException extends CancellationException {
    }

    //endregion
}

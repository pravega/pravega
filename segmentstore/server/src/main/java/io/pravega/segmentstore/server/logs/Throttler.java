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
package io.pravega.segmentstore.server.logs;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.server.SegmentStoreMetrics;
import io.pravega.segmentstore.storage.ThrottleSourceListener;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * Throttling utilities for the {@link OperationProcessor} class.
 */
@Slf4j
class Throttler implements ThrottleSourceListener, AutoCloseable {
    //region Members

    private final ThrottlerCalculator throttlerCalculator;
    private final Supplier<Boolean> isSuspended;
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
     * @param isSuspended A Supplier that will be invoked upon every call to {@link #throttle()}. If it returns true,
     *                    no throttling will be applied; if it returns false, normal throttling logic is executed.
     * @param executor    An Executor for async operations.
     * @param metrics     Metrics for reporting.
     */
    Throttler(int containerId, @NonNull ThrottlerCalculator calculator, @NonNull Supplier<Boolean> isSuspended,
              @NonNull ScheduledExecutorService executor, @NonNull SegmentStoreMetrics.OperationProcessor metrics) {
        this.throttlerCalculator = calculator;
        this.isSuspended = isSuspended;
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

    //region ThrottleSourceListener Implementation

    @Override
    public void notifyThrottleSourceChanged() {
        val currentDelay = this.currentDelay.get();
        if (currentDelay != null && isInterruptible(currentDelay.source)) {
            // We were actively throttling due to a reason that is eligible for re-throttling. Terminate the current
            // throttle cycle, which should force the throttler to re-evaluate the situation.
            log.debug("{}: Throttling interrupted while actively throttling ({}).", this.traceObjectId, currentDelay);
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
        if (this.isSuspended.get()) {
            // Temporarily disabled. Do not throttle.
            log.debug("{}: Throttler suspended.", this.traceObjectId);
            return CompletableFuture.completedFuture(null);
        }

        val delay = new AtomicReference<>(this.throttlerCalculator.getThrottlingDelay());
        if (!delay.get().isMaximum()) {
            // We are not delaying the maximum amount. We only need to do this once.
            val existingDelay = this.currentDelay.get();
            if (existingDelay != null) {
                // Looks like a previous throttle cycle was interrupted. In order to prevent endless throttling (cache
                // evictions happen frequently and there is no guarantee that the size of the cache actually decreased in
                // the meantime), we should only wait the minimum of the newly calculated delay and whatever was left
                // from the previous one (if anything).
                int remaining = (int) existingDelay.remaining.getRemaining().toMillis();
                if (remaining > 0 && remaining < delay.get().getDurationMillis()) {
                    delay.set(delay.get().withNewDelay(remaining));
                }
                this.metrics.processingDelay((int) existingDelay.remaining.getElapsed().toMillis(), existingDelay.source.toString());
            }

            return throttleOnce(delay.get());
        } else {
            // The initial delay calculation indicated that we need to throttle to the maximum, which means there's
            // significant pressure. In order to protect downstream components, we need to run in a loop and delay as much
            // as needed until the pressure is relieved.
            return Futures.loop(
                    () -> delay.get().isMaximum() && !this.isSuspended.get(),
                    () -> throttleOnce(delay.get())
                            .thenRun(() -> delay.set(this.throttlerCalculator.getThrottlingDelay())),
                    this.executor);
        }
    }

    @VisibleForTesting
    protected CompletableFuture<Void> throttleOnce(ThrottlerCalculator.DelayResult delay) {
        if (delay.isMaximum()
                || delay.getThrottlerName() == ThrottlerCalculator.ThrottlerName.DurableDataLog) {
            // Increase logging visibility if we throttle at the maximum limit (which means we're likely to fully block
            // processing of operations) or if this is due to us not being able to ingest items quickly enough.
            log.warn("{}: Processing delay = {}.", this.traceObjectId, delay);
        } else {
            log.debug("{}: Processing delay = {}.", this.traceObjectId, delay);
        }

        val delayFuture = createDelayFuture(delay.getDurationMillis());
        if (isInterruptible(delay.getThrottlerName())) {
            // This throttling source is eligible for interruption. Set it up so that a call to notifyThrottleSourceChanged()
            // may find it and act on it.
            val result = new InterruptibleDelay(delayFuture, delay.getDurationMillis(), delay.getThrottlerName());
            this.currentDelay.set(result);
            // Check if we have throttle-exempt operations after calculating a interruptible delay and just before throttling.
            if (this.isSuspended.get()) {
                notifyThrottleSourceChanged();
            }
            return Futures
                    .exceptionallyComposeExpecting(
                            result.delayFuture,
                            ex -> ex instanceof ThrottlingInterruptedException,
                            this::throttle)
                    .whenComplete((r, e) -> {
                        if (this.currentDelay.get() != null && !this.currentDelay.get().remaining.hasRemaining()) {
                            this.metrics.processingDelay(delay.getDurationMillis(), delay.getThrottlerName().toString());
                        }
                        this.currentDelay.set(null);
                    });
        } else {
            // The future won't be interrupted, so we can assume it will run till completion.
            if (delay.getThrottlerName() != null) {
                this.metrics.processingDelay(delay.getDurationMillis(), delay.getThrottlerName().toString());
            }
            return delayFuture;
        }
    }

    private boolean isInterruptible(ThrottlerCalculator.ThrottlerName name) {
        return name != null && name.isInterruptible();
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

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
package io.pravega.controller.eventProcessor.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.client.stream.Position;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.controller.eventProcessor.RequestHandler;
import io.pravega.controller.retryable.RetryableException;
import io.pravega.shared.controller.event.ControllerEvent;
import lombok.AllArgsConstructor;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Phaser;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.pravega.controller.eventProcessor.impl.EventProcessorHelper.indefiniteRetries;
import static io.pravega.controller.eventProcessor.impl.EventProcessorHelper.withRetries;
import static io.pravega.controller.eventProcessor.impl.EventProcessorHelper.writeBack;

/**
 * This event processor allows concurrent event processing.
 * It receives an event, schedules its background processing and returns the control to
 * Event processor cell to fetch and supply next event.
 */
@Slf4j
public class ConcurrentEventProcessor<R extends ControllerEvent, H extends RequestHandler<R>>
        extends EventProcessor<R> {
    private static final int MAX_CONCURRENT = 1000;
    private static final PositionCounter MAX = new PositionCounter(null, Long.MAX_VALUE);

    private final ConcurrentSkipListSet<PositionCounter> running;
    private final ConcurrentSkipListSet<PositionCounter> completed;
    private final AtomicReference<PositionCounter> checkpoint;
    private final ScheduledExecutorService executor;
    private final H requestHandler;
    private final AtomicLong counter = new AtomicLong(0);
    private final AtomicBoolean stop = new AtomicBoolean(false);
    private final Comparator<PositionCounter> positionCounterComparator = Comparator.comparingLong(o -> o.counter);
    private final Semaphore semaphore;
    private final ScheduledFuture<?> periodicCheckpoint;
    private final Checkpointer checkpointer;
    private final Writer<R> internalWriter;
    /**
     * The phaser is used to count number of ongoing requests and act as a 
     * barrier to complete shutdown until all ongoing requests are completed. 
     */
    private final Phaser phaser;

    public ConcurrentEventProcessor(final H requestHandler,
                                    final ScheduledExecutorService executor) {
        this(requestHandler, MAX_CONCURRENT, executor, null, null, 1, TimeUnit.MINUTES);
    }

    @VisibleForTesting
    ConcurrentEventProcessor(final H requestHandler,
                             final int maxConcurrent,
                             final ScheduledExecutorService executor,
                             final Checkpointer checkpointer,
                             final Writer<R> writer,
                             final long checkpointPeriod,
                             final TimeUnit timeUnit) {
        Preconditions.checkNotNull(requestHandler);
        Preconditions.checkNotNull(executor);

        this.requestHandler = requestHandler;
        running = new ConcurrentSkipListSet<>(positionCounterComparator);
        completed = new ConcurrentSkipListSet<>(positionCounterComparator);
        this.checkpointer = checkpointer;
        this.checkpoint = new AtomicReference<>();
        this.internalWriter = writer;
        this.executor = executor;
        periodicCheckpoint = this.executor.scheduleAtFixedRate(this::periodicCheckpoint, 0, checkpointPeriod, timeUnit);
        semaphore = new Semaphore(maxConcurrent);
        // It is initialized with 1 unarrived party for phase 0.
        // Until all registered parties do not arrive, the phase is not advanced. 
        // The final arriveAndAwaitAdvance is invoked from shutdown(afterstop) and blocks until all registered parties 
        // have arrived. 
        this.phaser = new Phaser(1);
    }

    @Override
    protected void process(R request, Position position) {
        // Limiting number of concurrent processing using semaphores. Otherwise we will keep picking messages from the stream
        // and it could lead to memory overload.
        if (!stop.get()) {
            semaphore.acquireUninterruptibly();
            // Use phaser.register to register a new party to indicate starting of a new processing.
            phaser.register();

            long next = counter.incrementAndGet();
            PositionCounter pc = new PositionCounter(position, next);
            running.add(pc);

            // In case of a retryable exception, retry few times before putting the event back into event stream.
            withRetries(() -> requestHandler.process(request, stop::get), executor)
                    .whenCompleteAsync((r, e) -> {
                        CompletableFuture<Void> future;
                        if (e != null) {
                            log.warn("ConcurrentEventProcessor Processing failed {}", e.getClass().getName());
                            future = handleProcessingError(request, e);
                        } else {
                            log.debug("ConcurrentEventProcessor Processing complete");
                            future = CompletableFuture.completedFuture(null);
                        }

                        future.whenCompleteAsync((res, ex) -> {
                            // do not update checkpoint if stop has been initiated and request has been cancelled.
                            if (!stop.get() || ex == null || !(Exceptions.unwrap(ex) instanceof CancellationException)) {
                                checkpoint(pc);
                            }
                            
                            // Report arrival and deregister a party to indicate completion of an ongoing processing.
                            phaser.arriveAndDeregister();
                            semaphore.release();
                        }, executor);
                    }, executor);
        } else {
            // note: Since stop was requested we will not do any processing on new event.
            // Event processor will pick the next event until it is eventually stopped. But we will keep ignoring them.
            // And since this class does its own checkpointing, so we are not updating our last checkpoint.
            log.warn("processing requested after processor is stopped.");
        }
    }

    private CompletableFuture<Void> handleProcessingError(R request, Throwable e) {
        CompletableFuture<Void> future;
        Throwable cause = Exceptions.unwrap(e);

        if (cause instanceof RetriesExhaustedException) {
            cause = cause.getCause();
        }

        if (RetryableException.isRetryable(cause)) {
            log.warn("ConcurrentEventProcessor Processing failed, Retryable Exception {}. Putting the event back.", cause.getClass().getName());

            Writer<R> writer;
            if (internalWriter != null) {
                writer = internalWriter;
            } else if (getSelfWriter() != null) {
                writer = getSelfWriter();
            } else {
                writer = null;
            }

            future = indefiniteRetries(() -> writeBack(request, writer), executor);
        } else {
            // Fail the future with actual failure. The failure will be handled by the caller. 
            Throwable actual = Exceptions.unwrap(e);
            log.warn("ConcurrentEventProcessor Processing failed, {} {}", actual.getClass(), actual.getMessage());
            future = Futures.failedFuture(actual);
        }

        return future;
    }

    @Override
    protected void afterStop() {
        stop.set(true);
        // Invoke arriveAndAwaitAdvance and wait for phase to advance which will happen 
        // only when all ongoing processing is completed and all registered parties have arrived. 
        phaser.arriveAndAwaitAdvance();
        phaser.arriveAndDeregister();
        periodicCheckpoint.cancel(true);
    }

    @VisibleForTesting
    boolean isStopFlagSet() {
        return stop.get();
    }

    /**
     * This method maintains a sorted list of position for requests currently being processed.
     * As processing of each request completes, it is removed from the sorted list and moved to
     * completed list.
     * Completed is also a sorted list that contains all completed requests greater than smallest running position.
     * In other words, we maintain all requests from smallest processing to current position in either running or completed
     * sorted list.
     * Note: Smallest position will always be in the running list.
     * We also maintain a single checkpoint, which is the highest completed position smaller than smallest running position.
     *
     * @param pc position for which processing completed
     */
    @Synchronized
    private void checkpoint(PositionCounter pc) {
        running.remove(pc);
        completed.add(pc);

        final PositionCounter smallest = running.isEmpty() ? MAX : running.first();
        final List<PositionCounter> checkpointCandidates = completed.stream()
                .filter(x -> positionCounterComparator.compare(x, smallest) < 0).collect(Collectors.toList());
        if (checkpointCandidates.size() > 0) {
            final PositionCounter checkpointPosition = checkpointCandidates.get(checkpointCandidates.size() - 1);
            completed.removeAll(checkpointCandidates);
            checkpoint.set(checkpointPosition);
        }
    }

    @VisibleForTesting
    void periodicCheckpoint() {
        try {
            if (checkpoint.get() != null && checkpoint.get().position != null) {
                if (checkpointer != null) {
                    checkpointer.store(checkpoint.get().position);
                } else if (getCheckpointer() != null) {
                    getCheckpointer().store(checkpoint.get().position);
                }
            }
        } catch (Exception e) {
            log.warn("error while trying to store checkpoint in the store {}", e);
        }
    }

    @AllArgsConstructor
    private static class PositionCounter {
        private final Position position;
        private final long counter;
    }
}

/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.eventProcessor.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.client.stream.Position;
import io.pravega.common.Exceptions;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.controller.eventProcessor.RequestHandler;
import io.pravega.controller.retryable.RetryableException;
import io.pravega.shared.controller.event.ControllerEvent;
import lombok.AllArgsConstructor;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
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
    }

    @Override
    protected void process(R request, Position position) {
        // Limiting number of concurrent processing using semaphores. Otherwise we will keep picking messages from the stream
        // and it could lead to memory overload.
        if (!stop.get()) {
            semaphore.acquireUninterruptibly();

            long next = counter.incrementAndGet();
            PositionCounter pc = new PositionCounter(position, next);
            running.add(pc);

            // In case of a retryable exception, retry few times before putting the event back into event stream.
            withRetries(() -> requestHandler.process(request), executor)
                    .whenCompleteAsync((r, e) -> {
                        CompletableFuture<Void> future;
                        if (e != null) {
                            log.warn("ConcurrentEventProcessor Processing failed {}", e.getClass().getName());
                            future = handleProcessingError(request, e);
                        } else {
                            log.debug("ConcurrentEventProcessor Processing complete");
                            future = CompletableFuture.completedFuture(null);
                        }

                        future.thenAcceptAsync(x -> {
                            checkpoint(pc);
                            semaphore.release();
                        }, executor);
                    }, executor);
        } else {
            // note: Since stop was requested we will not do any processing on new event.
            // Event processor will pick the next event until it is eventually stopped. But we will keep ignoring them.
            // And since this class does its own checkpointing, so we are not updating our last checkpoint.
            log.info("processing requested after processor is stopped");
        }
    }

    private CompletableFuture<Void> handleProcessingError(R request, Throwable e) {
        CompletableFuture<Void> future;
        Throwable cause = Exceptions.unwrap(e);

        if (cause instanceof RetriesExhaustedException) {
            cause = cause.getCause();
        }

        if (RetryableException.isRetryable(cause)) {
            log.info("ConcurrentEventProcessor Processing failed, Retryable Exception {}. Putting the event back.", cause.getClass().getName());

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
            log.error("ConcurrentEventProcessor Processing failed, exiting {}", e);
            future = CompletableFuture.completedFuture(null);
        }

        return future;
    }

    @Override
    protected void afterStop() {
        stop.set(true);
        periodicCheckpoint.cancel(true);
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

    private void periodicCheckpoint() {
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

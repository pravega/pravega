/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.server.eventProcessor;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.controller.eventProcessor.impl.EventProcessor;
import com.emc.pravega.controller.requests.ControllerEvent;
import com.emc.pravega.controller.retryable.RetryableException;
import com.emc.pravega.controller.store.checkpoint.CheckpointStoreException;
import com.emc.pravega.stream.Position;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * This event processor allows concurrent event processing.
 * It receives an event, schedules its background processing and returns the control to
 * Event processor cell to fetch and supply next event.
 */
@Slf4j
public class ConcurrentEventProcessor<R extends ControllerEvent, H extends RequestHandler<R>>
        extends EventProcessor<R> implements AutoCloseable {

    private static final int MAX_CONCURRENT = 10000;

    private final ConcurrentSkipListSet<PositionCounter> running;
    private final ConcurrentSkipListSet<PositionCounter> completed;
    private final AtomicReference<PositionCounter> checkpoint;
    private final ScheduledExecutorService executor;
    private final AtomicBoolean stop = new AtomicBoolean(false);
    private final H requestHandler;
    private final AtomicLong counter = new AtomicLong(0);
    private final Comparator<PositionCounter> positionCounterComparator = Comparator.comparingLong(o -> o.counter);
    private final Semaphore semaphore;

    ConcurrentEventProcessor(final H requestHandler,
                             final ScheduledExecutorService executor) {
        Preconditions.checkNotNull(requestHandler);
        Preconditions.checkNotNull(executor);

        this.requestHandler = requestHandler;

        running = new ConcurrentSkipListSet<>(positionCounterComparator);
        completed = new ConcurrentSkipListSet<>(positionCounterComparator);

        this.checkpoint = new AtomicReference<>();

        this.executor = executor;

        semaphore = new Semaphore(MAX_CONCURRENT);
    }

    @Override
    protected void process(R event, Position position) {
        // Limiting number of concurrent processing using semaphores. Otherwise we will keep picking messages from the stream
        // and it could lead to memory overload.
        PositionCounter pc;
        R request;
        long next;
        try {
            semaphore.acquire();
        } catch (Exception e) {
            log.warn("exception thrown while acquiring semaphore {}", e);
            semaphore.release();
            // TODO: throw meaningful exception..
            // What do we want to do here?
            // if we fail to acquire lock, we want to reprocess this event as for no fault of it, we are making it
            throw new CompletionException(e);
        }

        next = counter.incrementAndGet();
        request = event;
        pc = new PositionCounter(position, next);
        running.add(pc);

        requestHandler.process(request)
                .whenCompleteAsync((r, e) -> {
                    complete(pc);
                    semaphore.release();

                    if (e != null) {
                        log.error("ScaleEventProcessor Processing failed {}", e);

                        if (RetryableException.isRetryable(e)) {
                            putBack(request.getKey(), request);
                        }
                    }
                }, executor);
    }

    public void stop() {
        stop.set(true);
    }

    /**
     * This method puts the event back into event processor's stream.
     * If processing of a request could not be done because of retryable exceptions,
     * We will put it back into the request stream. This frees up compute cycles and ensures
     * that checkpointing is not stalled on completion of some task.
     *
     * @param request request which has to be put back in the request stream.
     */
    @Synchronized
    private void putBack(String key, R request) {
        FutureHelpers.getAndHandleExceptions(getSelfWriter().writeEvent(key, request), RuntimeException::new);
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
    private void complete(PositionCounter pc) {
        running.remove(pc);
        completed.add(pc);

        final PositionCounter smallest = running.first();

        final List<PositionCounter> checkpointCandidates = completed.stream()
                .filter(x -> positionCounterComparator.compare(x, smallest) < 0).collect(Collectors.toList());
        final PositionCounter checkpointPosition = checkpointCandidates.get(checkpointCandidates.size() - 1);
        completed.removeAll(checkpointCandidates);
        checkpoint.set(checkpointPosition);
        try {
            getCheckpointer().store(checkpoint.get().position);
        } catch (CheckpointStoreException e) {
            // log and ignore
            log.warn("failed to checkpoint {}", e);
        }
    }

    @Override
    public void close() throws Exception {
        // wait for all background processing to complete.
        stop();
    }

    @AllArgsConstructor
    private class PositionCounter {
        private final Position position;
        private final long counter;
    }

    //    static final ExceptionHandler CONCURRENTEP_EXCEPTION_HANDLER = (Throwable t) -> {
    //        Throwable y = ExceptionHelpers.getRealException(t);
    //        ExceptionHandler.Directive ret = ExceptionHandler.DEFAULT_EXCEPTION_HANDLER.run(y);
    //        if (RetryableException.isRetryable(y)) {
    //
    //        }
    //        return ret;
    //    };
}

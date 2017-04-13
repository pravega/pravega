/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.server.eventProcessor;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.controller.eventProcessor.impl.EventProcessor;
import com.emc.pravega.controller.requests.ControllerEvent;
import com.emc.pravega.controller.requests.ScaleEvent;
import com.emc.pravega.controller.retryable.RetryableException;
import com.emc.pravega.controller.store.checkpoint.CheckpointStoreException;
import com.emc.pravega.stream.EventRead;
import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.EventStreamWriter;
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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * This actor processes commit txn events.
 * It does the following 2 operations in order.
 * 1. Send commit txn message to active segments of the stream.
 * 2. Change txn state from committing to committed.
 */
@Slf4j
public class ScaleEventProcessor<R extends ControllerEvent, H extends RequestHandler<R>>
        extends EventProcessor<R> implements AutoCloseable {

    private static final int MAX_CONCURRENT = 10000;

    private final ConcurrentSkipListSet<PositionCounter> running;
    private final ConcurrentSkipListSet<PositionCounter> completed;
    private final AtomicReference<PositionCounter> checkpoint;
    private final EventStreamWriter<R> writer;
    private final ScheduledExecutorService executor;
    private final AtomicBoolean stop = new AtomicBoolean(false);
    private final H requestHandler;
    private final AtomicLong counter = new AtomicLong(0);
    private final Comparator<PositionCounter> positionCounterComparator = Comparator.comparingLong(o -> o.counter);
    private final Semaphore semaphore;
    private final ScheduledFuture<?> scheduledFuture;
    private final Checkpointer checkpointer;

    ScaleEventProcessor(final EventStreamWriter<R> writer,
                        final Checkpointer checkpointer,
                        final H requestHandler,
                        final ScheduledExecutorService executor) {
        Preconditions.checkNotNull(writer);
        Preconditions.checkNotNull(checkpointer);
        Preconditions.checkNotNull(requestHandler);
        Preconditions.checkNotNull(executor);

        this.requestHandler = requestHandler;

        running = new ConcurrentSkipListSet<>(positionCounterComparator);
        completed = new ConcurrentSkipListSet<>(positionCounterComparator);

        this.writer = writer;

        this.checkpoint = new AtomicReference<>();

        this.executor = executor;
        this.checkpointer = checkpointer;

        // periodic checkpointing - every one minute
        scheduledFuture = this.executor.scheduleAtFixedRate(this::checkpoint, 1, 1, TimeUnit.MINUTES);
        semaphore = new Semaphore(MAX_CONCURRENT);
    }

    @Override
    protected void process(R event) {
        process(event, null);
    }

    void process(R event, Position position) {
        // limiting number of concurrent processing. Otherwise we will keep picking messages from the stream
        // and it could lead to memory overload.

        PositionCounter pc;
        R request;
        long next;
        try {
            semaphore.acquire();
        } catch (Exception e) {
            log.warn("exception thrown while acquiring semaphore {}", e);
            semaphore.release();
            // TODO: throw meaningful exception
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

    @FunctionalInterface
    public interface Checkpointer {
        void checkpoint(Position position) throws CheckpointStoreException;
    }

    public void stop() {
        stop.set(true);
    }

    /**
     * This method puts the request back into the request stream.
     * If processing of a request could not be done because of retryable exceptions,
     * We will put it back into the request stream. This frees up compute cycles and ensures that checkpointing is not stalled
     * on completion of some task.
     * <p>
     * If we fail in trying to write to request stream, should we ignore or retry indefinitely?
     * Since this class gives a guarantee of at least once processing with retry on retryable failures,
     * so we may do that. But we have already processed the message at least once. So we should not waste our
     * cycles here.
     * <p>
     * Example: For scale operations: we have a request relayed after a 'mute' delay, so it is not fatal.
     * In the interest of not stalling checkpointing for long, we should fail fast and put the request back in the queue
     * for it to be retried asynchronously after a delay as we move our checkpoint ahead.
     * <p>
     * For tx.timeout operations: if we fail to put the request back in the stream, the txn will never timeout.
     *
     * @param request request which has to be put back in the request stream.
     */
    @Synchronized
    private void putBack(String key, R request) {
        FutureHelpers.getAndHandleExceptions(writer.writeEvent(key, request), RuntimeException::new);
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
    }

    @Synchronized
    private void checkpoint() {
        try {
            if (checkpoint.get() != null) {
                checkpointer.checkpoint(checkpoint.get().position);
            }
        } catch (Exception e) {
            // Even if this fails, its ok. Next checkpoint periodic trigger will store the checkpoint.
            log.error("Request reader checkpointing failed {}", e);
        } catch (Throwable t) {
            log.error("Checkpointing failed with fatal error {}", t);
        }
    }

    @Override
    public void close() throws Exception {
        scheduledFuture.cancel(true);
        stop();
    }

    @AllArgsConstructor
    private class PositionCounter {
        private final Position position;
        private final long counter;
    }

}

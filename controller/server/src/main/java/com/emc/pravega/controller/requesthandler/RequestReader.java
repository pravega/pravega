/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.requesthandler;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.controller.requests.ControllerRequest;
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

/**
 * Common class for reading requests from a pravega stream.
 * It implements a runnable.
 * It keeps polling the supplied stream for events and calls
 * registered request handler for processing these requests.
 * Request handlers are supposed to request events asynchronously.
 * The request reader submits a processing of a request asynchronously
 * and moves on to next request.
 * It handles exceptions that are thrown by processing and if the thrown
 * exception is of type Retryable, then
 * the request is written into the stream.
 * <p>
 * It is expected of requesthandlers to wrap their processing in enough
 * retries locally before throwing a retryable
 * exception to request reader.
 * <p>
 * The request reader also maintains a checkpoint candidate position from
 * among the events for which processing is complete.
 * Everytime a new request completes, it updates the checkpoint candidate.
 * It periodically checkpoints the candidate position object into metadata store.
 *
 * @param <R>
 * @param <H>
 */
@Slf4j
public class RequestReader<R extends ControllerRequest, H extends RequestHandler<R>> implements AutoCloseable {

    @FunctionalInterface
    public interface Checkpointer {
        void checkpoint(Position position) throws CheckpointStoreException;
    }

    private static final int MAX_CONCURRENT = 10000;

    private final ConcurrentSkipListSet<PositionCounter> running;
    private final ConcurrentSkipListSet<PositionCounter> completed;
    private final AtomicReference<PositionCounter> checkpoint;
    private final EventStreamWriter<R> writer;
    private final EventStreamReader<R> reader;
    private final ScheduledExecutorService executor;
    private final AtomicBoolean stop = new AtomicBoolean(false);
    private final H requestHandler;
    private final AtomicLong counter = new AtomicLong(0);
    private final Comparator<PositionCounter> positionCounterComparator = Comparator.comparingLong(o -> o.counter);
    private final Semaphore semaphore;
    private final ScheduledFuture<?> scheduledFuture;
    private final CompletableFuture<Void> promise = new CompletableFuture<>();
    private final Checkpointer checkpointer;

    RequestReader(final EventStreamWriter<R> writer,
                  final EventStreamReader<R> reader,
                  final H requestHandler,
                  final Checkpointer checkpointer,
                  final ScheduledExecutorService executor) {
        Preconditions.checkNotNull(writer);
        Preconditions.checkNotNull(reader);
        Preconditions.checkNotNull(requestHandler);
        Preconditions.checkNotNull(checkpointer);

        this.requestHandler = requestHandler;

        running = new ConcurrentSkipListSet<>(positionCounterComparator);
        completed = new ConcurrentSkipListSet<>(positionCounterComparator);

        this.writer = writer;
        this.reader = reader;

        this.checkpoint = new AtomicReference<>();

        this.executor = executor;
        this.checkpointer = checkpointer;

        // periodic checkpointing - every one minute
        scheduledFuture = this.executor.scheduleAtFixedRate(this::checkpoint, 1, 1, TimeUnit.MINUTES);
        semaphore = new Semaphore(MAX_CONCURRENT);
    }

    public void stop() {
        stop.set(true);
    }

    /**
     * One dedicated thread that reads one event at a time and schedules its processing asynchronously.
     * Once processing is scheduled, it goes back to polling for next event.
     * Before starting it first acquires a semaphore. This ensures that we do not have more than max allowed
     * concurrent processing of events. Otherwise we run the risk of out of memory as we will keep posting requests in
     * our executor queue.
     * <p>
     * It also gets the next counter value. A counter is an ever increasing number.
     */
    public CompletableFuture<Void> run() {
        CompletableFuture.runAsync(() -> {
            while (!stop.get()) {
                try {
                    // limiting number of concurrent processing. Otherwise we will keep picking messages from the stream
                    // and it could lead to memory overload.
                    semaphore.acquire();

                    EventRead<R> event;
                    long next;
                    R request;
                    PositionCounter pc;

                    try {
                        next = counter.incrementAndGet();

                        event = reader.readNextEvent(2000);
                        if (event == null || event.getEvent() == null) {
                            log.trace("timeout elapsed but no request received.");
                            continue;
                        }

                        request = event.getEvent();
                        pc = new PositionCounter(event.getPosition(), next);
                        running.add(pc);
                    } catch (Exception e) {
                        semaphore.release();
                        log.warn("error reading event {}", e);
                        throw e;
                    }

                    CompletableFuture<Void> process;
                    try {
                        process = requestHandler.process(request);
                    } catch (Exception e) {
                        log.error("exception thrown while creating processing future {}", e);
                        complete(pc);
                        semaphore.release();
                        throw e;
                    }

                    process.whenCompleteAsync((r, e) -> {
                        complete(pc);
                        semaphore.release();

                        if (e != null) {
                            log.error("Processing failed RequestReader {}", e);

                            if (RetryableException.isRetryable(e)) {
                                putBack(request.getKey(), request);
                            }
                        }
                    }, executor);
                } catch (Exception | Error e) {
                    // Catch all exceptions (not throwable) and log and ignore.
                    // Ideally we should never come here. But this is a safety check to ensure request processing continues even if
                    // an exception is thrown while doing reads for next events.
                    // And we should never stop processing of other requests in the queue even if processing a request throws
                    // an exception.
                    log.error("Exception thrown while processing event. Logging and continuing. Stack trace {}", e);
                } catch (Throwable t) {
                    log.error("Fatal exception while processing event {}", t);
                    promise.completeExceptionally(t);
                    throw t;
                }
            }
        });

        return promise;
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
            promise.completeExceptionally(t);
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

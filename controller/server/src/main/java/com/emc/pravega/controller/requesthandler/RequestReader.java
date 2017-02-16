/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.requesthandler;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.controller.RetryableException;
import com.emc.pravega.controller.requests.ControllerRequest;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.stream.EventRead;
import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Common class for reading requests from a pravega stream. It implements a runnable.
 * It keeps polling the supplied stream for events and calls registered request handler for processing these requests.
 * Request handlers are supposed to request events asynchronously.
 * The request reader submits a processing of a request asynchronously and moves on to next request.
 * It handles exceptions that are thrown by processing and if the thrown exception is of type Retryable, then
 * the request is written into the stream.
 * <p>
 * It is expected of requesthandlers to wrap their processing in enough retries locally before throwing a retryable
 * exception to request reader.
 * <p>
 * The request reader also maintains a checkpoint candidate position from among the events for which processing is complete.
 * Everytime a new request completes, it updates the checkpoint candidate.
 * It periodically checkpoints the candidate position object into metadata store.
 *
 * @param <R>
 * @param <H>
 */
@Slf4j
public class RequestReader<R extends ControllerRequest, H extends RequestHandler<R>> implements Runnable {

    private static final int MAX_CONCURRENT = 10000;

    private final String readerId;
    private final String readerGroup;
    private final ConcurrentSkipListSet<PositionCounter> running;
    private final ConcurrentSkipListSet<PositionCounter> completed;
    private final AtomicReference<PositionCounter> checkpoint;
    private final EventStreamWriter<R> writer;
    private final EventStreamReader<R> reader;
    private final JavaSerializer<Position> serializer;
    private final ScheduledExecutorService executor;
    private final StreamMetadataStore streamMetadataStore;
    private final AtomicBoolean stop = new AtomicBoolean(false);
    private final H requestHandler;
    private final AtomicLong counter = new AtomicLong(0);
    private final Comparator<PositionCounter> positionCounterComparator = Comparator.comparingLong(o -> o.counter);
    private final ReentrantReadWriteLock counterLock;
    private final Semaphore semaphore;

    RequestReader(final String readerId,
                  final String readerGroup,
                  final EventStreamWriter<R> writer,
                  final EventStreamReader<R> reader,
                  final StreamMetadataStore streamMetadataStore,
                  final H requestHandler,
                  final ScheduledExecutorService executor) {
        Preconditions.checkNotNull(writer);
        Preconditions.checkNotNull(reader);
        Preconditions.checkNotNull(streamMetadataStore);
        Preconditions.checkNotNull(requestHandler);

        this.streamMetadataStore = streamMetadataStore;
        this.requestHandler = requestHandler;

        this.readerId = readerId;
        this.readerGroup = readerGroup;
        running = new ConcurrentSkipListSet<>(positionCounterComparator);
        completed = new ConcurrentSkipListSet<>(positionCounterComparator);

        this.writer = writer;
        this.reader = reader;

        this.checkpoint = new AtomicReference<>();

        serializer = new JavaSerializer<>();

        this.executor = executor;

        // periodic checkpointing - every one minute
        this.executor.scheduleAtFixedRate(this::checkpoint, 1, 1, TimeUnit.MINUTES);
        counterLock = new ReentrantReadWriteLock();
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
     * The getNextCounter method ensures that if we reach Long.MAX, it resets counters for all running and completed tasks
     * and then resets the overall counter to highest value in them.
     */
    @Override
    public void run() {
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
                    next = getNextCounter();

                    event = reader.readNextEvent(60000);
                    request = event.getEvent();
                    pc = new PositionCounter(event.getPosition(), next);
                    running.add(pc);
                } catch (Exception e) {
                    semaphore.release();
                    log.warn("error reading event {}", e.getMessage());
                    throw e;
                }

                CompletableFuture<Void> process;
                try {
                    process = requestHandler.process(request);
                } catch (Exception e) {
                    log.error("exception thrown while creating processing future {}", e.getMessage());
                    complete(pc);
                    semaphore.release();
                    throw e;
                }

                process.whenCompleteAsync((r, e) -> {
                    semaphore.release();
                    complete(pc);

                    if (e != null) {
                        log.error("Processing failed RequestReader {}", e.getMessage());

                        if (RetryableException.isRetryable(e)) {
                            putBack(request.getKey(), request);
                        }
                    }
                }, executor);
            } catch (Exception e) {
                // Catch all exceptions (not throwable) and log and ignore.
                // Ideally we should never come here. But this is a safety check to ensure request processing continues even if
                // an exception is thrown while doing reads for next events.
                // And we should never stop processing of other requests in the queue even if processing a request throws
                // an exception.
                log.error("Exception thrown while processing event. {}. Logging and continuing. Stack trace {}", e.getMessage(), e.getStackTrace());
            }
        }
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

        counterLock.readLock().lock();
        try {
            final PositionCounter smallest = running.first();

            final List<PositionCounter> checkpointCandidates = completed.stream()
                    .filter(x -> positionCounterComparator.compare(x, smallest) < 0).collect(Collectors.toList());
            final PositionCounter checkpointPosition = checkpointCandidates.get(checkpointCandidates.size() - 1);
            completed.removeAll(checkpointCandidates);
            checkpoint.set(checkpointPosition);
        } finally {
            counterLock.readLock().unlock();
        }
    }

    @Synchronized
    private void checkpoint() {
        // Even if this fails, its ok. Next checkpoint periodic trigger will store the checkpoint.
        // TODO: store it in persistent store
        serializer.serialize(checkpoint.get().position);
        // checkpoint(readerId, readerGroup, serialize);
    }

    /**
     * Only called from run method so no synchronization required.
     *
     * @return next counter
     */
    private long getNextCounter() {
        if (counter.get() == Long.MAX_VALUE) {
            // update all existing positioncounters
            counterLock.writeLock().lock();
            try {
                completed.forEach(pos -> {
                    pos.counter = Long.MAX_VALUE - pos.counter;
                });

                running.forEach(pos -> {
                    pos.counter = Long.MAX_VALUE - pos.counter;
                });

                long p1 = Optional.ofNullable(running.pollLast()).map(p -> p.counter).orElse(0L);
                long p2 = Optional.ofNullable(completed.pollLast()).map(p -> p.counter).orElse(0L);

                counter.set(Math.max(p1, p2));
            } finally {
                counterLock.writeLock().unlock();
            }
        }
        return counter.incrementAndGet();
    }

    @AllArgsConstructor
    private class PositionCounter {
        Position position;
        long counter;
    }
}

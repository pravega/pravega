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

import com.emc.pravega.ClientFactory;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.util.Retry;
import com.emc.pravega.controller.NonRetryableException;
import com.emc.pravega.controller.RetryableException;
import com.emc.pravega.controller.requests.ControllerRequest;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.util.Config;
import com.emc.pravega.stream.EventRead;
import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.ReaderConfig;
import com.emc.pravega.stream.impl.ClientFactoryImpl;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.impl.PositionComparator;
import com.google.common.base.Preconditions;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Slf4j
public class RequestReader<R extends ControllerRequest, H extends RequestHandler<R>> implements Runnable {
    private static final int CORE_POOL_SIZE = 1000;
    private final String readerId;
    private final String readerGroup;
    private final ConcurrentSkipListSet<Position> running;
    private final ConcurrentSkipListSet<Position> completed;
    private final AtomicReference<Position> checkpoint;
    private final EventStreamWriter<R> writer;
    private final EventStreamReader<R> reader;
    private final PositionComparator positionComparator;
    private final JavaSerializer<Position> serializer;
    private final ScheduledExecutorService executor;
    private final StreamMetadataStore streamMetadataStore;
    private AtomicBoolean stop = new AtomicBoolean(false);
    private final H requestHandler;

    public RequestReader(final String requestStream,
                         final String readerId,
                         final String readerGroup,
                         final Position start,
                         final StreamMetadataStore streamMetadataStore,
                         final H requestHandler) {
        Preconditions.checkNotNull(streamMetadataStore);
        Preconditions.checkNotNull(requestHandler);

        this.streamMetadataStore = streamMetadataStore;
        this.requestHandler = requestHandler;

        this.readerId = readerId;
        this.readerGroup = readerGroup;
        positionComparator = new PositionComparator();
        running = new ConcurrentSkipListSet<>(positionComparator);
        completed = new ConcurrentSkipListSet<>(positionComparator);

        // controller is localhost.
        ClientFactory clientFactory = new ClientFactoryImpl(Config.INTERNAL_SCOPE, URI.create(String.format("tcp://localhost:%d", Config.SERVER_PORT)));

        // TODO: create reader in a group instead of a standalone reader.
        // read request stream name from configuration
        // read reader group name from configuration
        // read reader id from configuration
        writer = clientFactory.createEventWriter(requestStream,
                new JavaSerializer<>(),
                new EventWriterConfig(null));

        reader = clientFactory.createReader(requestStream,
                new JavaSerializer<>(),
                new ReaderConfig(),
                start);

        //        reader = clientFactory.createReader(Defaults.READER_GROUP,
        //                "1",
        //                new JavaSerializer<>(),
        //                new ReaderConfig());

        this.checkpoint = new AtomicReference<>();
        this.checkpoint.set(start);

        serializer = new JavaSerializer<>();

        this.executor = new ScheduledThreadPoolExecutor(CORE_POOL_SIZE);

        // periodic checkpointing - every one minute
        executor.scheduleAtFixedRate(this::checkpoint, 1, 1, TimeUnit.MINUTES);
    }

    public void stop() {
        stop.set(true);
    }

    @Override
    public void run() {
        while (!stop.get()) {
            try {
                EventRead<R> event = reader.readNextEvent(60000);
                R request = event.getEvent();
                running.add(event.getPosition());
                requestHandler.process(request, executor)
                        .whenComplete((r, e) -> {
                            if (e != null) {
                                if (RetryableException.isRetryable(e)) {
                                    putBack(request.getKey(), request);
                                }
                            }
                            complete(event);
                        });
            } catch (Exception e) {
                // Catch all exceptions (not throwable) and log and ignore.
                // Ideally we should never come here. But this is a safety check to ensure request processing continues even if
                // an exception is thrown while doing reads for next events.
                // And we should never stop processing of other requests in the queue even if processing a request throws
                // an exception.
                log.warn("Exception thrown while processing event", e);
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
        // TODO: retry on failure
        writer.writeEvent(key, request);
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
     * @param event event for which processing completed
     */
    private void complete(EventRead<R> event) {
        running.remove(event.getPosition());
        completed.add(event.getPosition());
        // find the lowest in running
        final Position smallest = running.first();

        final List<Position> checkpointCandidates = completed.stream().filter(x -> positionComparator.compare(x, smallest) < 0).collect(Collectors.toList());
        final Position checkpointPosition = checkpointCandidates.get(checkpointCandidates.size() - 1);
        completed.removeAll(checkpointCandidates);
        checkpoint.set(checkpointPosition);
    }

    @Synchronized
    private void checkpoint() {
        // Even if this fails, its ok. Next checkpoint periodic trigger will store the checkpoint.
        Retry.withExpBackoff(100, 10, 3, 1000)
                .retryingOn(RetryableException.class)
                .throwingOn(NonRetryableException.class)
                .run(() ->
                        FutureHelpers.getAndHandleExceptions(
                                streamMetadataStore.checkpoint(readerId, readerGroup, serializer.serialize(checkpoint.get())),
                                e -> {
                                    if (e instanceof RetryableException) {
                                        return (RetryableException) e;
                                    } else {
                                        return new RuntimeException(e);
                                    }
                                }));
    }
}

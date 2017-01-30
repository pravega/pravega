package com.emc.pravega.controller.requesthandler;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.util.Retry;
import com.emc.pravega.controller.NonRetryableException;
import com.emc.pravega.controller.RetryableException;
import com.emc.pravega.controller.requests.ControllerRequest;
import com.emc.pravega.controller.requests.RequestStreamConstants;
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
public class RequestReader<Request extends ControllerRequest, Handler extends RequestHandler<Request>> implements Runnable {
    private static final int CORE_POOL_SIZE = 1000;
    private final String readerId;
    private final String readerGroup;
    private final ConcurrentSkipListSet<Position> running;
    private final ConcurrentSkipListSet<Position> completed;
    private final AtomicReference<Position> checkpoint;
    private final EventStreamWriter<Request> writer;
    private final EventStreamReader<Request> reader;
    private final PositionComparator positionComparator;
    private final JavaSerializer<Position> serializer;
    private final ScheduledExecutorService executor;
    private final StreamMetadataStore streamMetadataStore;
    private AtomicBoolean stop = new AtomicBoolean(false);
    private final Handler requestHandler;

    public RequestReader(final String readerId,
                         final String readerGroup,
                         final Position start,
                         final StreamMetadataStore streamMetadataStore,
                         final Handler requestHandler) {
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
        ClientFactory clientFactory = new ClientFactoryImpl(RequestStreamConstants.SCOPE, URI.create(String.format("tcp://localhost:%d", Config.SERVER_PORT)));

        // TODO: create reader in a group instead of a standalone reader.
        // read request stream name from configuration
        // read reader group name from configuration
        // read reader id from configuration
        writer = clientFactory.createEventWriter(RequestStreamConstants.REQUEST_STREAM,
                new JavaSerializer<>(),
                new EventWriterConfig(null));

        reader = clientFactory.createReader(RequestStreamConstants.REQUEST_STREAM,
                new JavaSerializer<>(),
                new ReaderConfig(),
                start);

//        reader = clientFactory.createReader(RequestStreamConstants.READER_GROUP,
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
                EventRead<Request> event = reader.readNextEvent(60000);
                Request request = event.getEvent();
                running.add(event.getPosition());
                requestHandler.process(request, executor)
                        .whenComplete((r, e) -> {
                            if (e != null) {
                                if (RetryableException.isRetryable(e)) {
                                    putBack(requestHandler.getKey(request), request);
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
     * we will put it back into the request stream. This frees up compute cycles and ensures that checkpointing is not stalled
     * on completion of some task.
     * <p>
     * If we fail in trying to write to request stream, we will retry few times and then drop it. This is because we do not want to
     * hold on to this thread.
     * Since we have a request relayed after a 'mute' delay, so it is not fatal. And we should not stall and wait here as
     * we would a) waste compute cycles b) stall checkpointing
     * In the interest of not stalling checkpointing for long, we should fail fast and put the request back in the queue
     * for it to be retried asynchronously after a delay as we move our checkpoint ahead.
     *
     * @param request request which has to be put back in the request stream.
     */
    @Synchronized
    private void putBack(String key, Request request) {
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
    private void complete(EventRead<Request> event) {
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

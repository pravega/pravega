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
package io.pravega.controller.task;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.server.eventProcessor.requesthandlers.TaskExceptions;
import io.pravega.controller.store.index.HostIndex;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.task.Stream.RequestSweeper;
import io.pravega.controller.util.RetryHelper;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.shared.controller.event.ControllerEventSerializer;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

/**
 * Class with methods related to publishing/tracking completion of events to event processor framework.
 */
@Slf4j
public class EventHelper implements AutoCloseable {
    private static final long COMPLETION_TIMEOUT_MILLIS = Duration.ofMinutes(2).toMillis();
    private final CompletableFuture<Void> writerInitFuture = new CompletableFuture<>();
    private final AtomicReference<EventStreamWriter<ControllerEvent>> requestEventWriterRef = new AtomicReference<>();
    private final ScheduledExecutorService executor;
    private final ScheduledExecutorService eventExecutor;
    private final String hostId;
    private final HostIndex hostTaskIndex;
    private final ControllerEventSerializer controllerEventSerializer = new ControllerEventSerializer();
    private final AtomicLong completionTimeoutMillis = new AtomicLong(COMPLETION_TIMEOUT_MILLIS);

    public EventHelper(final EventStreamWriter<ControllerEvent> requestEventWriter,
                       final ScheduledExecutorService executor,
                       final ScheduledExecutorService eventExecutor,
                       final String hostId, final HostIndex hostIndex) {
        this.executor = executor;
        this.eventExecutor = eventExecutor;
        this.hostId = hostId;
        this.hostTaskIndex = hostIndex;
        this.requestEventWriterRef.set(requestEventWriter);
        this.writerInitFuture.complete(null);
    }

    @VisibleForTesting
    public EventHelper(final ScheduledExecutorService executor,
                 final String hostId, final HostIndex hostIndex) {
        this.executor = executor;
        this.eventExecutor = executor;
        this.hostId = hostId;
        this.hostTaskIndex = hostIndex;
        this.writerInitFuture.complete(null);
    }


    @VisibleForTesting
    public void setRequestEventWriter(EventStreamWriter<ControllerEvent> requestEventWriter) {
        EventStreamWriter<ControllerEvent> oldWriter = requestEventWriterRef.getAndSet(requestEventWriter);
        if (oldWriter != null) {
            oldWriter.close();
        }
        writerInitFuture.complete(null);
    }

    @VisibleForTesting
    public void setCompletionTimeoutMillis(long timeoutMillis) {
        completionTimeoutMillis.set(timeoutMillis);
    }

    /**
     * This method takes an event and a future supplier and guarantees that if future supplier has been executed then event will
     * be posted in request stream. It does it by following approach:
     * 1. it first adds the index for the event to be posted to the current host.
     * 2. it then invokes future.
     * 3. it then posts event.
     * 4. removes the index.
     *
     * If controller fails after step 2, a replacement controller will failover all indexes and {@link RequestSweeper} will
     * post events for any index that is found.
     *
     * Upon failover, an index can be found if failure occurred in any step before 3. It is safe to post duplicate events
     * because event processing is idempotent. It is also safe to post event even if step 2 was not performed because the
     * event will be ignored by the processor after a while.
     *
     * @param event      Event to publish.
     * @param futureSupplier  Supplier future to execute before submitting event.
     * @param <T> The Type of the future's result.
     * @return CompletableFuture<T> returned by Supplier or Exception.
     */
    public <T> CompletableFuture<T> addIndexAndSubmitTask(ControllerEvent event, Supplier<CompletableFuture<T>> futureSupplier) {
        String id = UUID.randomUUID().toString();
        // We first add index and then call the metadata update.
        //  While trying to perform a metadata update, upon getting a connection exception or a write conflict exception
        // (which can also occur if we had retried on a store exception), we will still post the event because we
        //  don't know whether our update succeeded. Posting the event is harmless, though. If the update
        // has succeeded, then the event will be used for processing. If the update had failed, then the event
        // will be discarded. We will throw the exception that we received from running futureSupplier or return the
        // successful value
        return addRequestToIndex(this.hostId, id, event)
                .thenCompose(v -> Futures.handleCompose(futureSupplier.get(),
                        (r, e) -> {
                            if (e == null || (Exceptions.unwrap(e) instanceof StoreException.StoreConnectionException ||
                                    Exceptions.unwrap(e) instanceof StoreException.WriteConflictException)) {
                                return RetryHelper.withIndefiniteRetriesAsync(() -> writeEvent(event),
                                        ex -> log.warn("writing event failed with {}", ex.getMessage()), this.executor)
                                        .thenCompose(z -> removeTaskFromIndex(this.hostId, id))
                                        .thenApply(vd -> {
                                            if (e != null) {
                                                throw new CompletionException(e);
                                            } else {
                                                return r;
                                            }
                                        });
                            } else {
                                throw new CompletionException(e);
                            }
                        }));
    }

    public CompletableFuture<Void> checkDone(Supplier<CompletableFuture<Boolean>> condition) {
        return checkDone(condition, 100L);
    }

    public CompletableFuture<Void> checkDone(Supplier<CompletableFuture<Boolean>> condition, long delay) {
        // Check whether workflow is complete by adding a delay between each iteration.
        // If the work is not complete within `completionTimeoutMillis` throw TimeoutException.
        AtomicBoolean isDone = new AtomicBoolean(false);
        return RetryHelper.loopWithTimeout(() -> !isDone.get(), () -> condition.get().thenAccept(isDone::set),
                delay, 5000L, completionTimeoutMillis.get(), executor);
    }

    public CompletableFuture<Void> writeEvent(ControllerEvent event) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        writerInitFuture.thenComposeAsync(v -> requestEventWriterRef.get().writeEvent(event.getKey(), event),
                eventExecutor)
                .whenComplete((r, e) -> {
                    if (e != null) {
                        log.warn("Exception while posting event {} {}", e.getClass().getName(), e.getMessage());
                        if (e instanceof TaskExceptions.ProcessingDisabledException) {
                            result.completeExceptionally(e);
                        } else {
                            // transform any other event write exception to retryable exception
                            result.completeExceptionally(new TaskExceptions.PostEventException("Failed to post event", e));
                        }
                    } else {
                        log.info("Successfully posted event {}", event);
                        result.complete(null);
                    }
                });
        return result;
    }

    /**
     * Adds specified request in the host's task index.
     * This is idempotent operation.
     *
     * @param hostId      Host identifier.
     * @param id          Unique id used while adding task to index.
     * @param task        Task Request to index.
     * @return            A future when completed will indicate that the task is indexed for the given host.
     */
    public CompletableFuture<Void> addRequestToIndex(String hostId, String id, ControllerEvent task) {
        return this.hostTaskIndex.addEntity(hostId, id, controllerEventSerializer.toByteBuffer(task).array());
    }

    /**
     * Removes the index for task identified by `id` in host task index for host identified by `hostId`
     * This is idempotent operation.
     *
     * @param hostId Node whose child is to be removed.
     * @param id     Unique id used while adding task to index.
     * @return Future which when completed will indicate that the task has been removed from index.
     */
    public CompletableFuture<Void> removeTaskFromIndex(String hostId, String id) {
        return this.hostTaskIndex.removeEntity(hostId, id, true);
    }

    @Override
    public void close() {
        if (!writerInitFuture.isDone()) {
            writerInitFuture.cancel(true);
        }
        EventStreamWriter<ControllerEvent> writer = requestEventWriterRef.get();
        if (writer != null) {
            writer.close();
        }
    }
}

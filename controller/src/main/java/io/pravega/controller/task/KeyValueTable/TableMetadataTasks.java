/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.task.KeyValueTable;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.common.tracing.TagLogger;
import io.pravega.controller.retryable.RetryableException;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.eventProcessor.ControllerEventProcessors;
import io.pravega.controller.server.eventProcessor.requesthandlers.TaskExceptions;
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import io.pravega.controller.store.kvtable.KVTableState;
import io.pravega.controller.store.stream.StoreException;

import io.pravega.controller.stream.api.grpc.v1.Controller.CreateKeyValueTableStatus;
import io.pravega.controller.store.kvtable.KVTableMetadataStore;
import io.pravega.controller.task.Stream.RequestSweeper;
import io.pravega.controller.util.RetryHelper;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.shared.controller.event.kvtable.CreateTableEvent;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import lombok.Synchronized;
import org.slf4j.LoggerFactory;

import static io.pravega.controller.task.Stream.TaskStepsRetryHelper.withRetries;
import static io.pravega.shared.NameUtils.getQualifiedTableSegmentName;


/**
 * Collection of metadata update tasks on KeyValueTable.
 * <p>
 * Any update to the task method signature should be avoided, since it can cause problems during upgrade.
 * Instead, a new overloaded method may be created with the same task annotation name but a new version.
 */
public class TableMetadataTasks implements AutoCloseable {
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(TableMetadataTasks.class));
    private final KVTableMetadataStore kvtMetadataStore;
    private final SegmentHelper segmentHelper;
    private final ScheduledExecutorService executor;
    private final ScheduledExecutorService eventExecutor;
    private final String hostId;
    private final GrpcAuthHelper authHelper;
    private final RequestTracker requestTracker;
    private String requestStreamName;
    private final CompletableFuture<Void> writerInitFuture = new CompletableFuture<>();
    private final AtomicReference<EventStreamWriter<ControllerEvent>> requestEventWriterRef = new AtomicReference<>();


    public TableMetadataTasks(final KVTableMetadataStore kvtMetadataStore,
                              final SegmentHelper segmentHelper, final ScheduledExecutorService executor,
                              final ScheduledExecutorService eventExecutor, final String hostId,
                              GrpcAuthHelper authHelper, RequestTracker requestTracker) {
        this.kvtMetadataStore = kvtMetadataStore;
        this.segmentHelper = segmentHelper;
        this.executor = executor;
        this.eventExecutor = eventExecutor;
        this.hostId = hostId;
        this.authHelper = authHelper;
        this.requestTracker = requestTracker;
    }

    @Synchronized
    public void initializeStreamWriters(final EventStreamClientFactory clientFactory,
                                        final String streamName) {
        this.requestStreamName = streamName;
        requestEventWriterRef.set(clientFactory.createEventWriter(requestStreamName,
                ControllerEventProcessors.CONTROLLER_EVENT_SERIALIZER,
                EventWriterConfig.builder().build()));
        writerInitFuture.complete(null);
    }

    /**
     *  Create a Key-Value Table.
     *
     * @param scope      scope name.
     * @param kvtName    KVTable name.
     * @param kvtConfig  KVTable configuration.
     * @param createTimestamp  KVTable creation timestamp.
     * @return update status.
     */
    public CompletableFuture<CreateKeyValueTableStatus.Status> createKeyValueTable(String scope, String kvtName,
                                                                                   KeyValueTableConfiguration kvtConfig,
                                                                                   final long createTimestamp) {
        final long requestId = requestTracker.getRequestIdFor("createKVTable", scope, kvtName);
        return RetryHelper.withRetriesAsync(() -> Futures.exceptionallyExpecting(kvtMetadataStore.getState(scope, kvtName, true, null, executor),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, KVTableState.UNKNOWN)
                .thenCompose(state -> {
                    if (state.equals(KVTableState.UNKNOWN) || state.equals(KVTableState.CREATING)) {
                        return Futures.completeOn(kvtMetadataStore.checkScopeExists(scope)
                                .thenCompose(exists -> {
                                    if (exists) {
                                        return Futures.exceptionallyExpecting(kvtMetadataStore.createEntryForKVTable(scope, kvtName, executor)
                                                        .thenCompose(v -> kvtMetadataStore.getKVTable(scope, kvtName, null).getId()
                                                        .thenCompose(uuid -> {
                                                            CreateTableEvent event = new CreateTableEvent(scope, kvtName, kvtConfig.getPartitionCount(),
                                                                                                        createTimestamp, requestId, UUID.fromString(uuid));
                                                            return addIndexAndSubmitTask(event,
                                                                    () -> CompletableFuture.completedFuture(null))
                                                                    .thenCompose(x -> checkDone(() -> isCreated(scope, kvtName, kvtConfig, executor))
                                                                            .thenCompose(y -> kvtMetadataStore.getConfiguration(scope, kvtName, null, executor)
                                                                            .thenCompose(cfg -> {
                                                                                if (cfg.getPartitionCount() == kvtConfig.getPartitionCount()) {
                                                                                   return CompletableFuture.completedFuture(CreateKeyValueTableStatus.Status.SUCCESS);
                                                                                } else {
                                                                                   return CompletableFuture.completedFuture(CreateKeyValueTableStatus.Status.TABLE_EXISTS);
                                                                                }
                                                                            })));
                                                        })),
                                                e -> Exceptions.unwrap(e) instanceof StoreException.DataExistsException,
                                                CreateKeyValueTableStatus.Status.TABLE_EXISTS);
                                    } else {
                                        return CompletableFuture.completedFuture(CreateKeyValueTableStatus.Status.SCOPE_NOT_FOUND);
                                    }
                                }), executor);
                    } else {
                       return CompletableFuture.completedFuture(CreateKeyValueTableStatus.Status.TABLE_EXISTS);
                    }
                }), e -> Exceptions.unwrap(e) instanceof RetryableException, Integer.MAX_VALUE, executor);
    }

    private CompletableFuture<Void> checkDone(Supplier<CompletableFuture<Boolean>> condition) {
        return checkDone(condition, 100L);
    }

    private CompletableFuture<Void> checkDone(Supplier<CompletableFuture<Boolean>> condition, long delay) {
        AtomicBoolean isDone = new AtomicBoolean(false);
        return Futures.loop(() -> !isDone.get(),
                () -> Futures.delayedFuture(condition, delay, executor)
                        .thenAccept(isDone::set), executor);
    }

    private CompletableFuture<Boolean> isCreated(String scope, String kvtName, KeyValueTableConfiguration kvtConfig, Executor executor) {
       return Futures.exceptionallyExpecting(kvtMetadataStore.getState(scope, kvtName, true, null, executor),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, KVTableState.UNKNOWN)
                .thenCompose(state -> CompletableFuture.completedFuture(state.equals(KVTableState.ACTIVE)));
    }

    @VisibleForTesting
    public void setRequestEventWriter(EventStreamWriter<ControllerEvent> requestEventWriter) {
        requestEventWriterRef.set(requestEventWriter);
        writerInitFuture.complete(null);
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
     * @return CompletableFuture<T> returned by Supplier or Exception.
     */
    @VisibleForTesting
    <T> CompletableFuture<T> addIndexAndSubmitTask(ControllerEvent event, Supplier<CompletableFuture<T>> futureSupplier) {
        String id = UUID.randomUUID().toString();
        // We first add index and then call the metadata update.
        //  While trying to perform a metadata update, upon getting a connection exception or a write conflict exception
        // (which can also occur if we had retried on a store exception), we will still post the event because we
        //  don't know whether our update succeeded. Posting the event is harmless, though. If the update
        // has succeeded, then the event will be used for processing. If the update had failed, then the event
        // will be discarded. We will throw the exception that we received from running futureSupplier or return the
        // successful value
        return this.kvtMetadataStore.addRequestToIndex(this.hostId, id, event)
                .thenCompose(v -> Futures.handleCompose(futureSupplier.get(),
                        (r, e) -> {
                            if (e == null || (Exceptions.unwrap(e) instanceof StoreException.StoreConnectionException ||
                                    Exceptions.unwrap(e) instanceof StoreException.WriteConflictException)) {
                                return RetryHelper.withIndefiniteRetriesAsync(() -> writeEvent(event),
                                        ex -> log.warn("writing event failed with {}", ex.getMessage()), executor)
                                        .thenCompose(z -> kvtMetadataStore.removeTaskFromIndex(this.hostId, id))
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

    CompletableFuture<Void> writeEvent(ControllerEvent event) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        writerInitFuture.thenComposeAsync(v -> requestEventWriterRef.get().writeEvent(event.getKey(), event),
                eventExecutor)
                .whenComplete((r, e) -> {
                    if (e != null) {
                        log.warn("exception while posting event {} {}", e.getClass().getName(), e.getMessage());
                        if (e instanceof TaskExceptions.ProcessingDisabledException) {
                            result.completeExceptionally(e);
                        } else {
                            // transform any other event write exception to retryable
                            // exception
                            result.completeExceptionally(new TaskExceptions.PostEventException("Failed to post event",
                                    e));
                        }
                    } else {
                        log.info("event posted successfully");
                        result.complete(null);
                    }
                });
        return result;
    }

    private String retrieveDelegationToken() {
        return authHelper.retrieveMasterToken();
    }
    
    public CompletableFuture<Void> createNewSegments(String scope, String kvt,
                                                     List<Long> segmentIds, long requestId) {
        return Futures.toVoid(Futures.allOfWithResults(segmentIds
                .stream()
                .parallel()
                .map(segment -> createNewSegment(scope, kvt, segment, retrieveDelegationToken(), requestId))
                .collect(Collectors.toList())));
    }

    private CompletableFuture<Void> createNewSegment(String scope, String kvt, long segmentId, String controllerToken,
                                                     long requestId) {
        final String qualifiedTableSegmentName = getQualifiedTableSegmentName(scope, kvt, segmentId);
        return Futures.toVoid(withRetries(() -> segmentHelper.createTableSegment(qualifiedTableSegmentName, controllerToken, requestId), executor));
    }

    @Override
    public void close() throws Exception {
        if (!writerInitFuture.isDone()) {
            writerInitFuture.cancel(true);
        }
        EventStreamWriter<ControllerEvent> writer = requestEventWriterRef.get();
        if (writer != null) {
            writer.close();
        }
    }
}

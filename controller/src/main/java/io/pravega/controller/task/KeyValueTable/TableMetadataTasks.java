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
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.common.tracing.TagLogger;
import io.pravega.common.util.BitConverter;
import io.pravega.controller.retryable.RetryableException;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.eventProcessor.ControllerEventProcessors;
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import io.pravega.controller.store.kvtable.AbstractKVTableMetadataStore;
import io.pravega.controller.store.kvtable.KVTOperationContext;
import io.pravega.controller.store.kvtable.KVTableState;
import io.pravega.controller.store.stream.StoreException;

import io.pravega.controller.stream.api.grpc.v1.Controller.CreateKeyValueTableStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteKVTableStatus;
import io.pravega.controller.store.kvtable.KVTableMetadataStore;
import io.pravega.controller.task.EventHelper;
import io.pravega.controller.util.RetryHelper;
import io.pravega.shared.controller.event.kvtable.CreateTableEvent;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import io.pravega.shared.controller.event.kvtable.DeleteTableEvent;
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
    private static final int NUM_RETRIES = 10;
    private final KVTableMetadataStore kvtMetadataStore;
    private final SegmentHelper segmentHelper;
    private final ScheduledExecutorService executor;
    private final ScheduledExecutorService eventExecutor;
    private final String hostId;
    private final GrpcAuthHelper authHelper;
    private final RequestTracker requestTracker;
    private EventHelper eventHelper;

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

    @VisibleForTesting
    public TableMetadataTasks(final KVTableMetadataStore kvtMetadataStore,
                              final SegmentHelper segmentHelper, final ScheduledExecutorService executor,
                              final ScheduledExecutorService eventExecutor, final String hostId,
                              GrpcAuthHelper authHelper, RequestTracker requestTracker, EventHelper helper) {
        this.kvtMetadataStore = kvtMetadataStore;
        this.segmentHelper = segmentHelper;
        this.executor = executor;
        this.eventExecutor = eventExecutor;
        this.hostId = hostId;
        this.authHelper = authHelper;
        this.requestTracker = requestTracker;
        this.eventHelper = helper;
    }

    @Synchronized
    public void initializeStreamWriters(final EventStreamClientFactory clientFactory,
                                        final String streamName) {

        this.eventHelper = new EventHelper(clientFactory.createEventWriter(streamName,
                ControllerEventProcessors.CONTROLLER_EVENT_SERIALIZER,
                EventWriterConfig.builder().build()), this.executor, this.eventExecutor, hostId,
                ((AbstractKVTableMetadataStore) this.kvtMetadataStore).getHostTaskIndex());
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
        return RetryHelper.withRetriesAsync(() -> {
               // 1. check if scope with this name exists...
               return kvtMetadataStore.checkScopeExists(scope)
                   .thenCompose(exists -> {
                        if (!exists) {
                            return CompletableFuture.completedFuture(CreateKeyValueTableStatus.Status.SCOPE_NOT_FOUND);
                        }
                        //2. check state of the KVTable, if found
                        return Futures.exceptionallyExpecting(kvtMetadataStore.getState(scope, kvtName, true, null, executor),
                                 e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, KVTableState.UNKNOWN)
                                    .thenCompose(state -> {
                                       if (state.equals(KVTableState.UNKNOWN) || state.equals(KVTableState.CREATING)) {
                                           //3. get a new UUID for the KVTable we will be creating.
                                           byte[] newUUID = kvtMetadataStore.newScope(scope).newId();
                                           CreateTableEvent event = new CreateTableEvent(scope, kvtName, kvtConfig.getPartitionCount(),
                                                        createTimestamp, requestId, BitConverter.readUUID(newUUID, 0));
                                           //4. Update ScopeTable with the entry for this KVT and Publish the event for creation
                                           return eventHelper.addIndexAndSubmitTask(event,
                                                   () -> kvtMetadataStore.createEntryForKVTable(scope, kvtName, newUUID, executor))
                                                   .thenCompose(x -> isCreateProcessed(scope, kvtName, kvtConfig, createTimestamp, executor));
                                       }
                                       return isCreateProcessed(scope, kvtName, kvtConfig, createTimestamp, executor);
                                 });
                            });
               }, e -> Exceptions.unwrap(e) instanceof RetryableException, NUM_RETRIES, executor);
    }

    /**
     * Delete a KeyValueTable.
     *
     * @param scope      scope.
     * @param kvtName    KeyValueTable name.
     * @return delete status.
     */
    public CompletableFuture<DeleteKVTableStatus.Status> deleteKeyValueTable(final String scope, final String kvtName) {
        final long requestId = requestTracker.getRequestIdFor("deleteKeyValueTable", scope, kvtName);
        return RetryHelper.withRetriesAsync(() -> {
                    KVTOperationContext context = kvtMetadataStore.createContext(scope, kvtName);
                    return Futures.exceptionallyExpecting(kvtMetadataStore.getState(scope, kvtName, false, context, executor),
                        e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, KVTableState.UNKNOWN)
                        .thenCompose(state -> {
                            if (KVTableState.UNKNOWN.equals(state)) {
                                return CompletableFuture.completedFuture(DeleteKVTableStatus.Status.TABLE_NOT_FOUND);
                            } else {
                                return kvtMetadataStore.getKVTable(scope, kvtName, context).getId()
                                        .thenCompose(id -> {
                                            DeleteTableEvent deleteEvent = new DeleteTableEvent(scope, kvtName, requestId, UUID.fromString(id));
                                            return eventHelper.addIndexAndSubmitTask(deleteEvent,
                                                    () -> kvtMetadataStore.setState(scope, kvtName, KVTableState.DELETING, context, executor))
                                                    .thenCompose(x -> eventHelper.checkDone(() -> isDeleted(scope, kvtName, context)))
                                                    .thenApply(y -> DeleteKVTableStatus.Status.SUCCESS);
                                        });
                            }
                        });
                }, e -> Exceptions.unwrap(e) instanceof RetryableException, NUM_RETRIES, executor);
    }

    public CompletableFuture<Void> deleteSegments(String scope, String kvt, Set<Long> segmentsToDelete,
                                                        String delegationToken, long requestId) {
        log.debug("{}/{} deleting {} segments", scope, kvt, segmentsToDelete.size());
        return Futures.allOf(segmentsToDelete
                .stream()
                .parallel()
                .map(segment -> deleteSegment(scope, kvt, segment, delegationToken, requestId))
                .collect(Collectors.toList()));
    }

    public CompletableFuture<Void> deleteSegment(String scope, String kvt, long segmentId, String delegationToken,
                                                       long requestId) {
        final String qualifiedTableSegmentName = getQualifiedTableSegmentName(scope, kvt, segmentId);
        log.debug("Deleting segment {} with Id {}", qualifiedTableSegmentName, segmentId);
        return Futures.toVoid(withRetries(() -> segmentHelper.deleteTableSegment(qualifiedTableSegmentName,
                                                false, delegationToken, requestId), executor));
    }

    private CompletableFuture<Boolean> isDeleted(String scope, String kvtName, KVTOperationContext context) {
        return Futures.exceptionallyExpecting(kvtMetadataStore.getState(scope, kvtName, false, context, executor),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, KVTableState.UNKNOWN)
                .thenCompose(state -> {
                    if (state.equals(KVTableState.UNKNOWN)) {
                        return CompletableFuture.completedFuture(Boolean.TRUE);
                    } else {
                        return CompletableFuture.completedFuture(Boolean.FALSE);
                    }
                });
    }

    private CompletableFuture<CreateKeyValueTableStatus.Status> isCreateProcessed(String scope, String kvtName,
                                                                                  KeyValueTableConfiguration kvtConfig,
                                                                                  final long createTimestamp,
                                                                                  Executor executor) {
        return eventHelper.checkDone(() -> isCreated(scope, kvtName, executor))
                .thenCompose(y -> isSameCreateRequest(scope, kvtName, kvtConfig, createTimestamp, executor))
                .thenCompose(same -> {
                    if (same) {
                        return CompletableFuture.completedFuture(CreateKeyValueTableStatus.Status.SUCCESS);
                    } else {
                        return CompletableFuture.completedFuture(CreateKeyValueTableStatus.Status.TABLE_EXISTS);
                    }
                });
    }

    private CompletableFuture<Boolean> isCreated(String scope, String kvtName, Executor executor) {
       return Futures.exceptionallyExpecting(kvtMetadataStore.getState(scope, kvtName, true, null, executor),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, KVTableState.UNKNOWN)
               .thenApply(state -> {
                    log.debug("KVTable State is {}", state.toString());
                    return state.equals(KVTableState.ACTIVE);
                });
    }

    private CompletableFuture<Boolean> isSameCreateRequest(final String requestScopeName, final String requestKVTName,
                                                           final KeyValueTableConfiguration requestKVTConfig,
                                                           final long requestCreateTimestamp,
                                                           Executor executor) {
    return kvtMetadataStore.getCreationTime(requestScopeName, requestKVTName, null, executor)
    .thenCompose(creationTime -> {
        if (creationTime == requestCreateTimestamp) {
            return kvtMetadataStore.getConfiguration(requestScopeName, requestKVTName, null, executor)
                    .thenCompose(cfg -> {
                        if (cfg.getPartitionCount() == requestKVTConfig.getPartitionCount()) {
                            return CompletableFuture.completedFuture(Boolean.TRUE);
                        } else {
                            return CompletableFuture.completedFuture(Boolean.FALSE);
                        }
                    });
            }
        return CompletableFuture.completedFuture(Boolean.FALSE);
        });
    }

    public String retrieveDelegationToken() {
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
        log.debug("Creating segment {}", qualifiedTableSegmentName);
        return Futures.toVoid(withRetries(() -> segmentHelper.createTableSegment(qualifiedTableSegmentName, controllerToken, requestId, true), executor));
    }

    @Override
    public void close() throws Exception {
        eventHelper.close();
    }
}

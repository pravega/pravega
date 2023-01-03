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
package io.pravega.controller.task.KeyValueTable;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.TagLogger;
import io.pravega.controller.retryable.RetryableException;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.eventProcessor.ControllerEventProcessors;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.kvtable.AbstractKVTableMetadataStore;
import io.pravega.controller.store.kvtable.KVTableMetadataStore;
import io.pravega.controller.store.kvtable.KVTableState;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateKeyValueTableStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteKVTableStatus;
import io.pravega.controller.task.EventHelper;
import io.pravega.controller.util.RetryHelper;
import io.pravega.shared.controller.event.kvtable.CreateTableEvent;
import io.pravega.shared.controller.event.kvtable.DeleteTableEvent;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
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
    private static final int NUM_RETRIES = 10;
    private final KVTableMetadataStore kvtMetadataStore;
    private final SegmentHelper segmentHelper;
    private final ScheduledExecutorService executor;
    private final ScheduledExecutorService eventExecutor;
    private final String hostId;
    private final GrpcAuthHelper authHelper;
    private EventHelper eventHelper;

    public TableMetadataTasks(final KVTableMetadataStore kvtMetadataStore,
                              final SegmentHelper segmentHelper, final ScheduledExecutorService executor,
                              final ScheduledExecutorService eventExecutor, final String hostId,
                              GrpcAuthHelper authHelper) {
        this.kvtMetadataStore = kvtMetadataStore;
        this.segmentHelper = segmentHelper;
        this.executor = executor;
        this.eventExecutor = eventExecutor;
        this.hostId = hostId;
        this.authHelper = authHelper;
    }

    @VisibleForTesting
    public TableMetadataTasks(final KVTableMetadataStore kvtMetadataStore,
                              final SegmentHelper segmentHelper, final ScheduledExecutorService executor,
                              final ScheduledExecutorService eventExecutor, final String hostId,
                              GrpcAuthHelper authHelper, EventHelper helper) {
        this.kvtMetadataStore = kvtMetadataStore;
        this.segmentHelper = segmentHelper;
        this.executor = executor;
        this.eventExecutor = eventExecutor;
        this.hostId = hostId;
        this.authHelper = authHelper;
        this.eventHelper = helper;
    }

    @Synchronized
    public void initializeStreamWriters(final EventStreamClientFactory clientFactory,
                                        final String streamName) {
        if (this.eventHelper != null) {
            this.eventHelper.close();
        }
        this.eventHelper = new EventHelper(clientFactory.createEventWriter(streamName,
                ControllerEventProcessors.CONTROLLER_EVENT_SERIALIZER,
                EventWriterConfig.builder().enableConnectionPooling(true).retryAttempts(Integer.MAX_VALUE).build()), this.executor, this.eventExecutor, hostId,
                ((AbstractKVTableMetadataStore) this.kvtMetadataStore).getHostTaskIndex());
    }

    /**
     *  Create a Key-Value Table.
     *
     * @param scope      scope name.
     * @param kvtName    KVTable name.
     * @param kvtConfig  KVTable configuration.
     * @param createTimestamp  KVTable creation timestamp.
     * @param requestId  request id
     * @return update status.
     */
    public CompletableFuture<CreateKeyValueTableStatus.Status> createKeyValueTable(String scope, String kvtName,
                                                                                   KeyValueTableConfiguration kvtConfig,
                                                                                   final long createTimestamp,
                                                                                   long requestId) {
        OperationContext context = kvtMetadataStore.createContext(scope, kvtName, requestId);

        return RetryHelper.withRetriesAsync(() -> {
               // 1. check if scope with this name exists...
               return kvtMetadataStore.checkScopeExists(scope, context, executor)
                   .thenCompose(exists -> {
                        if (!exists) {
                            return CompletableFuture.completedFuture(CreateKeyValueTableStatus.Status.SCOPE_NOT_FOUND);
                        }
                        return kvtMetadataStore.isScopeSealed(scope, context, executor).thenCompose(isScopeSealed -> {
                            if (isScopeSealed) {
                                return CompletableFuture.completedFuture(CreateKeyValueTableStatus.Status.SCOPE_NOT_FOUND);
                            }
                            //2. check state of the KVTable, if found
                            return Futures.exceptionallyExpecting(kvtMetadataStore.getState(scope, kvtName, true,
                                                    context, executor),
                                            e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, KVTableState.UNKNOWN)
                                    .thenCompose(state -> {
                                        if (state.equals(KVTableState.UNKNOWN) || state.equals(KVTableState.CREATING)) {
                                            //3. get a new UUID for the KVTable we will be creating.
                                            UUID id = kvtMetadataStore.newScope(scope).newId();

                                            CreateTableEvent event = new CreateTableEvent(scope, kvtName, kvtConfig.getPartitionCount(),
                                                    kvtConfig.getPrimaryKeyLength(), kvtConfig.getSecondaryKeyLength(),
                                                    createTimestamp, requestId, id, kvtConfig.getRolloverSizeBytes());
                                            //4. Update ScopeTable with the entry for this KVT and Publish the event for creation
                                            return eventHelper.addIndexAndSubmitTask(event,
                                                            () -> kvtMetadataStore.createEntryForKVTable(scope, kvtName, id,
                                                                    context, executor))
                                                    .thenCompose(x -> isCreateProcessed(scope, kvtName, kvtConfig,
                                                            createTimestamp, executor, context));
                                        }
                                        log.info(requestId, "KeyValue table {}/{} already exists", scope, kvtName);
                                        return isCreateProcessed(scope, kvtName, kvtConfig, createTimestamp, executor, context);
                                    });
                        });

                        });
               }, e -> Exceptions.unwrap(e) instanceof RetryableException, NUM_RETRIES, executor);
    }

    /**
     * Delete a KeyValueTable.
     *
     * @param scope      scope.
     * @param kvtName    KeyValueTable name.
     * @param requestId  request id.
     * @return delete status.
     */
    public CompletableFuture<DeleteKVTableStatus.Status> deleteKeyValueTable(final String scope, final String kvtName,
                                                                             long requestId) {
            OperationContext context = kvtMetadataStore.createContext(scope, kvtName, requestId);

            return RetryHelper.withRetriesAsync(() -> {
                    return Futures.exceptionallyExpecting(kvtMetadataStore.getState(scope, kvtName, false, context, executor),
                        e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, KVTableState.UNKNOWN)
                        .thenCompose(state -> {
                            if (KVTableState.UNKNOWN.equals(state)) {
                                return CompletableFuture.completedFuture(DeleteKVTableStatus.Status.TABLE_NOT_FOUND);
                            } else {
                                return kvtMetadataStore.getKVTable(scope, kvtName, context).getId(context)
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
        log.debug(requestId, "{}/{} deleting {} segments", scope, kvt, segmentsToDelete.size());
        return Futures.allOf(segmentsToDelete
                .stream()
                .parallel()
                .map(segment -> deleteSegment(scope, kvt, segment, delegationToken, requestId))
                .collect(Collectors.toList()));
    }

    public CompletableFuture<Void> deleteSegment(String scope, String kvt, long segmentId, String delegationToken,
                                                       long requestId) {
        final String qualifiedTableSegmentName = getQualifiedTableSegmentName(scope, kvt, segmentId);
        log.debug(requestId, "Deleting segment {} with Id {}", qualifiedTableSegmentName, segmentId);
        return Futures.toVoid(withRetries(() -> segmentHelper.deleteTableSegment(qualifiedTableSegmentName,
                                                false, delegationToken, requestId), executor));
    }

    @VisibleForTesting
    CompletableFuture<Boolean> isDeleted(String scope, String kvtName, OperationContext context) {
        return Futures.exceptionallyExpecting(kvtMetadataStore.getState(scope, kvtName, true, context, executor),
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
                                                                                  Executor executor, OperationContext context) {
        return eventHelper.checkDone(() -> isCreated(scope, kvtName, executor, context))
                .thenCompose(y -> isSameCreateRequest(scope, kvtName, kvtConfig, createTimestamp, executor, context))
                .thenCompose(same -> {
                    if (same) {
                        return CompletableFuture.completedFuture(CreateKeyValueTableStatus.Status.SUCCESS);
                    } else {
                        return CompletableFuture.completedFuture(CreateKeyValueTableStatus.Status.TABLE_EXISTS);
                    }
                });
    }

    private CompletableFuture<Boolean> isCreated(String scope, String kvtName, Executor executor, OperationContext context) {
       return Futures.exceptionallyExpecting(kvtMetadataStore.getState(scope, kvtName, true, context, executor),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, KVTableState.UNKNOWN)
               .thenApply(state -> {
                    log.debug(context.getRequestId(), "KVTable State is {}", state.toString());
                    return state.equals(KVTableState.ACTIVE);
                });
    }

    private CompletableFuture<Boolean> isSameCreateRequest(final String requestScopeName, final String requestKVTName,
                                                           final KeyValueTableConfiguration requestKVTConfig,
                                                           final long requestCreateTimestamp,
                                                           Executor executor, final OperationContext context) {
    return kvtMetadataStore.getCreationTime(requestScopeName, requestKVTName, context, executor)
    .thenCompose(creationTime -> {
        if (creationTime == requestCreateTimestamp) {
            return kvtMetadataStore.getConfiguration(requestScopeName, requestKVTName, context, executor)
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

    public CompletableFuture<Void> createNewSegments(String scope, String kvt, List<Long> segmentIds,
                                                     int keyLength, long requestId, long rolloverSizeBytes) {
        return Futures.toVoid(Futures.allOfWithResults(segmentIds
                .stream()
                .parallel()
                .map(segment -> createNewSegment(scope, kvt, segment, keyLength, retrieveDelegationToken(), requestId, rolloverSizeBytes))
                .collect(Collectors.toList())));
    }

    private CompletableFuture<Void> createNewSegment(String scope, String kvt, long segmentId, int keyLength, String controllerToken,
                                                     long requestId, long rolloverSizeBytes) {
        final String qualifiedTableSegmentName = getQualifiedTableSegmentName(scope, kvt, segmentId);
        log.debug("Creating segment {}", qualifiedTableSegmentName);
        return Futures.toVoid(withRetries(() -> segmentHelper.createTableSegment(qualifiedTableSegmentName, controllerToken,
                requestId, false, keyLength, rolloverSizeBytes), executor));
    }

    @Override
    public void close() {
        if (eventHelper != null) {
            eventHelper.close();
        }
    }
}

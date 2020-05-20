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
import io.pravega.common.tracing.RequestTracker;
import io.pravega.common.tracing.TagLogger;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.eventProcessor.ControllerEventProcessors;
import io.pravega.controller.server.eventProcessor.requesthandlers.TaskExceptions;
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import io.pravega.controller.store.kvtable.CreateKVTableResponse;
import io.pravega.controller.store.kvtable.KVTOperationContext;
import io.pravega.controller.store.stream.*;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateKeyValueTableStatus;
import io.pravega.controller.store.kvtable.KVTableMetadataStore;
import io.pravega.shared.NameUtils;
import io.pravega.shared.controller.event.ControllerEvent;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import lombok.Synchronized;
import org.slf4j.LoggerFactory;


/**
 * Collection of metadata update tasks on KeyValueTable.
 * <p>
 * Any update to the task method signature should be avoided, since it can cause problems during upgrade.
 * Instead, a new overloaded method may be created with the same task annotation name but a new version.
 */
public class KVTableMetadataTasks {

    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(KVTableMetadataTasks.class));
    private final KVTableMetadataStore kvtMetadataStore;
    private final SegmentHelper segmentHelper;
    private String requestStreamName;
    private final CompletableFuture<Void> writerInitFuture = new CompletableFuture<>();
    private final AtomicReference<EventStreamWriter<ControllerEvent>> requestEventWriterRef = new AtomicReference<>();
    private final GrpcAuthHelper authHelper;
    private final RequestTracker requestTracker;
    private final ScheduledExecutorService executor;
    private final ScheduledExecutorService eventExecutor;

    public KVTableMetadataTasks(final KVTableMetadataStore kvtMetadataStore,
                               final SegmentHelper segmentHelper, final ScheduledExecutorService executor,
                               final ScheduledExecutorService eventExecutor,
                               GrpcAuthHelper authHelper, RequestTracker requestTracker) {
        this.executor = executor;
        this.eventExecutor = eventExecutor;
        this.kvtMetadataStore = kvtMetadataStore;
        this.segmentHelper = segmentHelper;
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
     *  Create a Key-Value Table
     *
     * @param scope      scope name.
     * @param kvtName    KVTable name.
     * @param kvtConfig  KVTable configuration.
     * @return update status.
     */
    public CompletableFuture<CreateKeyValueTableStatus.Status> createKeyValueTable(String scope, String kvtName,
                                                                                   KeyValueTableConfiguration kvtConfig,
                                                                                   final long createTimestamp) {
        final long requestId = requestTracker.getRequestIdFor("createKVTable", scope, kvtName);
         return this.kvtMetadataStore.createKeyValueTable(scope, kvtName, kvtConfig, createTimestamp, null, executor)
                .thenComposeAsync(response -> {
                            log.info(requestId, "{}/{} created in metadata store", scope, kvtName);

                            Controller.CreateKeyValueTableStatus.Status status = translate(response.getStatus());
                            return CompletableFuture.completedFuture(status);
                        });

                    // only if its a new stream or an already existing non-active stream then we will create
                    // segments and change the state of the stream to active.
                    /*
                    if (response.getStatus().equals(CreateStreamResponse.CreateStatus.NEW) ||
                            response.getStatus().equals(CreateStreamResponse.CreateStatus.EXISTS_CREATING)) {
                        final int startingSegmentNumber = response.getStartingSegmentNumber();
                        final int minNumSegments = response.getConfiguration().getScalingPolicy().getMinNumSegments();
                        List<Long> newSegments = IntStream.range(startingSegmentNumber, startingSegmentNumber + minNumSegments)
                                .boxed()
                                .map(x -> NameUtils.computeSegmentId(x, 0))
                                .collect(Collectors.toList());
                        return notifyNewSegments(scope, stream, response.getConfiguration(), newSegments, this.retrieveDelegationToken(), requestId)
                                .thenCompose(v -> createMarkStream(scope, stream, timestamp, requestId))
                                .thenCompose(y -> {
                                    final OperationContext context = streamMetadataStore.createContext(scope, stream);

                                    return withRetries(() -> {
                                        CompletableFuture<Void> future;
                                        if (config.getRetentionPolicy() != null) {
                                            future = bucketStore.addStreamToBucketStore(BucketStore.ServiceType.RetentionService, scope, stream, executor);
                                        } else {
                                            future = CompletableFuture.completedFuture(null);
                                        }
                                        return future
                                                .thenCompose(v -> streamMetadataStore.getVersionedState(scope, stream, context, executor)
                                                        .thenCompose(state -> {
                                                            if (state.getObject().equals(State.CREATING)) {
                                                                return streamMetadataStore.updateVersionedState(scope, stream, State.ACTIVE,
                                                                        state, context, executor);
                                                            } else {
                                                                return CompletableFuture.completedFuture(state);
                                                            }
                                                        }));
                                    }, executor)
                                            .thenApply(z -> status);
                                });
                    } else {
                        return CompletableFuture.completedFuture(status);
                    }
                }, executor)
                .handle((result, ex) -> {
                    if (ex != null) {
                        Throwable cause = Exceptions.unwrap(ex);
                        if (cause instanceof StoreException.DataNotFoundException) {
                            return Controller.CreateStreamStatus.Status.SCOPE_NOT_FOUND;
                        } else {
                            log.warn(requestId, "Create stream failed due to ", ex);
                            return Controller.CreateStreamStatus.Status.FAILURE;
                        }
                    } else {
                        return result;
                    }
                });
        */
        /*
        // 1. get configuration
        return kvtMetadataStore.getConfigurationRecord(scope, kvtName, context, executor)
                .thenCompose(configProperty -> {
                    // 2. post event to start update workflow
                    if (!configProperty.getObject().isUpdating()) {
                        return addIndexAndSubmitTask(new UpdateStreamEvent(scope, stream, requestId), 
                                // 3. update new configuration in the store with updating flag = true
                                // if attempt to update fails, we bail out with no harm done
                                () -> kvtMetadataStore.startUpdateConfiguration(scope, stream, newConfig,
                                        context, executor))
                                // 4. wait for update to complete
                                .thenCompose(x -> checkDone(() -> isUpdated(scope, stream, newConfig, context))
                                        .thenApply(y -> UpdateStreamStatus.Status.SUCCESS));
                    } else {
                        log.warn(requestId, "Another update in progress for {}/{}",
                                scope, stream);
                        return CompletableFuture.completedFuture(UpdateStreamStatus.Status.FAILURE);
                    }
                })
                .exceptionally(ex -> {
                    log.warn(requestId, "Exception thrown in trying to update stream configuration {}",
                            ex.getMessage());
                    return handleUpdateStreamError(ex, requestId);
                });
                */

    }
/*
    private CompletableFuture<Void> checkDone(Supplier<CompletableFuture<Boolean>> condition) {
        return checkDone(condition, 100L);
    }
    
    private CompletableFuture<Void> checkDone(Supplier<CompletableFuture<Boolean>> condition, long delay) {
        AtomicBoolean isDone = new AtomicBoolean(false);
        return Futures.loop(() -> !isDone.get(),
                () -> Futures.delayedFuture(condition, delay, executor)
                             .thenAccept(isDone::set), executor);
    }

    @VisibleForTesting
    CompletableFuture<Boolean> isUpdated(String scope, String stream, StreamConfiguration newConfig, OperationContext context) {
        CompletableFuture<State> stateFuture = kvtMetadataStore.getState(scope, stream, true, context, executor);
        CompletableFuture<StreamConfigurationRecord> configPropertyFuture
                = kvtMetadataStore.getConfigurationRecord(scope, stream, context, executor).thenApply(VersionedMetadata::getObject);
        return CompletableFuture.allOf(stateFuture, configPropertyFuture)
                                .thenApply(v -> {
                                    State state = stateFuture.join();
                                    StreamConfigurationRecord configProperty = configPropertyFuture.join();

                                    // if property is updating and doesn't match our request, it's a subsequent update
                                    if (configProperty.isUpdating()) {
                                        return !configProperty.getStreamConfiguration().equals(newConfig);
                                    } else {
                                        // if update-barrier is not updating, then update is complete if property matches our expectation 
                                        // and state is not updating 
                                        return !(configProperty.getStreamConfiguration().equals(newConfig) && state.equals(State.UPDATING));
                                    }
                                });
    }
*/

    /**
     * Delete a stream. Precondition for deleting a stream is that the stream sholud be sealed.
     *
     * @param scope      scope.
     * @param stream     stream name.
     * @param contextOpt optional context
     * @return delete status.
     */
    /*
    public CompletableFuture<DeleteStreamStatus.Status> deleteStream(final String scope, final String stream,
                                                                     final OperationContext contextOpt) {

        final OperationContext context = contextOpt == null ? kvtMetadataStore.createContext(scope, stream) : contextOpt;
        final long requestId = requestTracker.getRequestIdFor("deleteStream", scope, stream);

        // We can delete streams only if they are sealed. However, for partially created streams, they could be in different
        // stages of partial creation and we should be able to clean them up. 
        // Case 1: A partially created stream may just have some initial metadata created, in which case the Stream's state may not
        // have been set up it may be present under the scope.
        // In this case we can simply delete all metadata for the stream directly. 
        // Case 2: A partially created stream could be in state CREATING, in which case it would definitely have metadata created 
        // and possibly segments too. This requires same clean up as for a sealed stream - metadata + segments. 
        // So we will submit delete workflow.  
        return Futures.exceptionallyExpecting(
                kvtMetadataStore.getState(scope, stream, false, context, executor),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, State.UNKNOWN)
                .thenCompose(state -> {
                    if (State.SEALED.equals(state) || State.CREATING.equals(state)) {
                        return kvtMetadataStore.getCreationTime(scope, stream, context, executor)
                                                  .thenApply(time -> new DeleteStreamEvent(scope, stream, requestId, time))
                                                  .thenCompose(event -> writeEvent(event))
                                                  .thenApply(x -> true);
                    } else if (State.UNKNOWN.equals(state)) {
                        // Since the state is not created, so the segments and state 
                        // are definitely not created.
                        // so we can simply delete the stream metadata which deletes stream from scope as well. 
                        return kvtMetadataStore.deleteStream(scope, stream, context, executor)
                                                  .exceptionally(e -> {
                                                      throw new CompletionException(e);
                                                  })
                                                  .thenApply(v -> true);
                    } else {
                        // we cannot delete the stream. Return false from here. 
                        return CompletableFuture.completedFuture(false);
                    }
                })
                .thenCompose(result -> {
                    if (result) {
                        return checkDone(() -> isDeleted(scope, stream))
                                .thenApply(x -> DeleteStreamStatus.Status.SUCCESS);
                    } else {
                        return CompletableFuture.completedFuture(DeleteStreamStatus.Status.STREAM_NOT_SEALED);
                    }
                })
                .exceptionally(ex -> {
                    log.warn(requestId, "Exception thrown while deleting stream {}", ex.getMessage());
                    return handleDeleteStreamError(ex, requestId);
                });


    }

    private CompletableFuture<Boolean> isDeleted(String scope, String stream) {
        return kvtMetadataStore.checkStreamExists(scope, stream)
                .thenApply(x -> !x);
    }
*/
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
     */
    /*
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
        return kvtMetadataStore.addRequestToIndex(context.getHostId(), id, event)
            .thenCompose(v -> Futures.handleCompose(futureSupplier.get(),
                (r, e) -> {
                    if (e == null || (Exceptions.unwrap(e) instanceof StoreException.StoreConnectionException ||
                            Exceptions.unwrap(e) instanceof StoreException.WriteConflictException)) {
                        return RetryHelper.withIndefiniteRetriesAsync(() -> writeEvent(event),
                                ex -> log.warn("writing event failed with {}", ex.getMessage()), executor)
                                          .thenCompose(z -> kvtMetadataStore.removeTaskFromIndex(context.getHostId(), id))
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
    */
    public CompletableFuture<Void> writeEvent(ControllerEvent event) {
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

    @VisibleForTesting
    public void setRequestEventWriter(EventStreamWriter<ControllerEvent> requestEventWriter) {
        requestEventWriterRef.set(requestEventWriter);
        writerInitFuture.complete(null);
    }

    private CreateKeyValueTableStatus.Status translate(CreateKVTableResponse.CreateStatus status) {
        CreateKeyValueTableStatus.Status retVal;
        switch (status) {
            case NEW:
                retVal = CreateKeyValueTableStatus.Status.SUCCESS;
                break;
            case EXISTS_ACTIVE:
            case EXISTS_CREATING:
                retVal = CreateKeyValueTableStatus.Status.TABLE_EXISTS;
                break;
            case FAILED:
            default:
                retVal = CreateKeyValueTableStatus.Status.FAILURE;
                break;
        }
        return retVal;
    }
/*
    public CompletableFuture<Void> notifyNewSegments(String scope, String stream, List<Long> segmentIds, OperationContext context,
                                                     String controllerToken) {
        return notifyNewSegments(scope, stream, segmentIds, context, controllerToken, RequestTag.NON_EXISTENT_ID);
    }

    public CompletableFuture<Void> notifyNewSegments(String scope, String stream, List<Long> segmentIds, OperationContext context,
                                                     String controllerToken, long requestId) {
        return withRetries(() -> kvtMetadataStore.getConfiguration(scope, stream, context, executor), executor)
                .thenCompose(configuration -> notifyNewSegments(scope, stream, configuration, segmentIds, controllerToken, requestId));
    }

    public CompletableFuture<Void> notifyNewSegments(String scope, String stream, StreamConfiguration configuration,
                                                     List<Long> segmentIds, String controllerToken, long requestId) {
        return Futures.toVoid(Futures.allOfWithResults(segmentIds
                .stream()
                .parallel()
                .map(segment -> notifyNewSegment(scope, stream, segment, configuration.getScalingPolicy(), controllerToken, requestId))
                .collect(Collectors.toList())));
    }

    public CompletableFuture<Void> notifyNewSegment(String scope, String stream, long segmentId, ScalingPolicy policy,
                                                    String controllerToken) {
        return Futures.toVoid(withRetries(() -> segmentHelper.createSegment(scope, stream, segmentId, policy,
                controllerToken, RequestTag.NON_EXISTENT_ID), executor));
    }

    public CompletableFuture<Void> notifyNewSegment(String scope, String stream, long segmentId, ScalingPolicy policy,
                                                    String controllerToken, long requestId) {
        return Futures.toVoid(withRetries(() -> segmentHelper.createSegment(scope,
                stream, segmentId, policy, controllerToken, requestId), executor));
    }

    public CompletableFuture<Void> notifyDeleteSegments(String scope, String stream, Set<Long> segmentsToDelete,
                                                        String delegationToken, long requestId) {
        return Futures.allOf(segmentsToDelete
                 .stream()
                 .parallel()
                 .map(segment -> notifyDeleteSegment(scope, stream, segment, delegationToken, requestId))
                 .collect(Collectors.toList()));
    }

    public CompletableFuture<Void> notifyDeleteSegment(String scope, String stream, long segmentId, String delegationToken,
                                                       long requestId) {
        return Futures.toVoid(withRetries(() -> segmentHelper.deleteSegment(scope,
                stream, segmentId, delegationToken, requestId), executor));
    }

    private CompletableFuture<Long> getSegmentOffset(String scope, String stream, long segmentId, String delegationToken) {

        return withRetries(() -> segmentHelper.getSegmentInfo(
                scope,
                stream,
                segmentId,
                delegationToken), executor)
                .thenApply(WireCommands.StreamSegmentInfo::getWriteOffset);
    }

    private DeleteStreamStatus.Status handleDeleteStreamError(Throwable ex, long requestId) {
        Throwable cause = Exceptions.unwrap(ex);
        if (cause instanceof StoreException.DataNotFoundException) {
            return DeleteStreamStatus.Status.STREAM_NOT_FOUND;
        } else {
            log.warn(requestId, "Delete stream failed.", ex);
            return DeleteStreamStatus.Status.FAILURE;
        }
    }
*/
    public String retrieveDelegationToken() {
        return authHelper.retrieveMasterToken();
    }
}

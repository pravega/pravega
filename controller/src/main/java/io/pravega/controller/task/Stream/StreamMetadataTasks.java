/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.task.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.control.impl.ModelHelper;
import io.pravega.common.Exceptions;
import io.pravega.common.Timer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.RequestTag;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.common.tracing.TagLogger;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.controller.metrics.StreamMetrics;
import io.pravega.controller.metrics.TransactionMetrics;
import io.pravega.controller.retryable.RetryableException;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.eventProcessor.ControllerEventProcessors;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.stream.AbstractStreamMetadataStore;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.CreateStreamResponse;
import io.pravega.controller.store.stream.EpochTransitionOperationExceptions;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.State;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;
import io.pravega.controller.store.stream.records.RetentionSet;
import io.pravega.controller.store.stream.records.StreamConfigurationRecord;
import io.pravega.controller.store.stream.records.StreamCutRecord;
import io.pravega.controller.store.stream.records.StreamCutReferenceRecord;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;
import io.pravega.controller.store.stream.records.StreamTruncationRecord;
import io.pravega.controller.store.task.LockFailedException;
import io.pravega.controller.store.task.Resource;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScaleStatusResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentRange;
import io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.AddSubscriberStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteSubscriberStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.UpdateSubscriberStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.SubscribersResponse;
import io.pravega.controller.task.EventHelper;
import io.pravega.controller.task.Task;
import io.pravega.controller.task.TaskBase;
import io.pravega.controller.util.Config;
import io.pravega.controller.util.RetryHelper;
import io.pravega.shared.NameUtils;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.shared.controller.event.DeleteStreamEvent;
import io.pravega.shared.controller.event.ScaleOpEvent;
import io.pravega.shared.controller.event.SealStreamEvent;
import io.pravega.shared.controller.event.TruncateStreamEvent;
import io.pravega.shared.controller.event.UpdateStreamEvent;
import io.pravega.shared.protocol.netty.WireCommands;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import lombok.Synchronized;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.LoggerFactory;

import static io.pravega.controller.task.Stream.TaskStepsRetryHelper.withRetries;

/**
 * Collection of metadata update tasks on stream.
 * Task methods are annotated with @Task annotation.
 * <p>
 * Any update to the task method signature should be avoided, since it can cause problems during upgrade.
 * Instead, a new overloaded method may be created with the same task annotation name but a new version.
 */
public class StreamMetadataTasks extends TaskBase {
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(StreamMetadataTasks.class));
    private static final int SUBSCRIBER_OPERATION_RETRIES = 10;

    private final AtomicLong retentionFrequencyMillis;
    
    private final StreamMetadataStore streamMetadataStore;
    private final BucketStore bucketStore;
    private final SegmentHelper segmentHelper;

    private final GrpcAuthHelper authHelper;
    private final RequestTracker requestTracker;
    private final ScheduledExecutorService eventExecutor;
    private EventHelper eventHelper;


    public StreamMetadataTasks(final StreamMetadataStore streamMetadataStore,
                               BucketStore bucketStore, final TaskMetadataStore taskMetadataStore,
                               final SegmentHelper segmentHelper, final ScheduledExecutorService executor,
                               final ScheduledExecutorService eventExecutor, final String hostId,
                               GrpcAuthHelper authHelper, RequestTracker requestTracker, final long retentionFrequencyMillis) {
        this(streamMetadataStore, bucketStore, taskMetadataStore, segmentHelper, executor, eventExecutor, new Context(hostId),
                authHelper, requestTracker);
        this.retentionFrequencyMillis.set(retentionFrequencyMillis);
    }

    @VisibleForTesting
    public StreamMetadataTasks(final StreamMetadataStore streamMetadataStore,
                               BucketStore bucketStore, final TaskMetadataStore taskMetadataStore,
                               final SegmentHelper segmentHelper, final ScheduledExecutorService executor,
                               final String hostId, GrpcAuthHelper authHelper, RequestTracker requestTracker) {
        this(streamMetadataStore, bucketStore, taskMetadataStore, segmentHelper, executor, executor, new Context(hostId),
             authHelper, requestTracker);
    }

    @VisibleForTesting
    public StreamMetadataTasks(final StreamMetadataStore streamMetadataStore,
                               BucketStore bucketStore, final TaskMetadataStore taskMetadataStore,
                               final SegmentHelper segmentHelper, final ScheduledExecutorService executor,
                               final String hostId, GrpcAuthHelper authHelper, RequestTracker requestTracker,
                               final EventHelper helper) {
        this(streamMetadataStore, bucketStore, taskMetadataStore, segmentHelper, executor, executor, new Context(hostId),
                authHelper, requestTracker);
        this.eventHelper = helper;
    }

    private StreamMetadataTasks(final StreamMetadataStore streamMetadataStore,
                                BucketStore bucketStore, final TaskMetadataStore taskMetadataStore,
                                final SegmentHelper segmentHelper, final ScheduledExecutorService executor,
                                final ScheduledExecutorService eventExecutor, final Context context,
                                GrpcAuthHelper authHelper, RequestTracker requestTracker) {
        super(taskMetadataStore, executor, context);
        this.eventExecutor = eventExecutor;
        this.streamMetadataStore = streamMetadataStore;
        this.bucketStore = bucketStore;
        this.segmentHelper = segmentHelper;
        this.authHelper = authHelper;
        this.requestTracker = requestTracker;
        this.retentionFrequencyMillis = new AtomicLong(Duration.ofMinutes(Config.MINIMUM_RETENTION_FREQUENCY_IN_MINUTES).toMillis());
        this.setReady();
    }

    @Synchronized
    public void initializeStreamWriters(final EventStreamClientFactory clientFactory,
                                        final String streamName) {
        this.eventHelper = new EventHelper(clientFactory.createEventWriter(streamName,
                ControllerEventProcessors.CONTROLLER_EVENT_SERIALIZER,
                EventWriterConfig.builder().build()),
                this.executor, this.eventExecutor, this.context.getHostId(),
                ((AbstractStreamMetadataStore) this.streamMetadataStore).getHostTaskIndex());
    }


    /**
     * Method to create stream with retries for lock failed exception.
     *
     * @param scope           scope.
     * @param stream          stream name.
     * @param config          stream configuration.
     * @param createTimestamp creation timestamp.
     * @param numOfRetries    number of retries for LockFailedException
     * @return CompletableFuture which when completed will have creation status for the stream.
     */
    public CompletableFuture<CreateStreamStatus.Status> createStreamRetryOnLockFailure(String scope, String stream, StreamConfiguration config,
                                                                                       long createTimestamp, int numOfRetries) {
        return RetryHelper.withRetriesAsync(() ->  createStream(scope, stream, config, createTimestamp),
                e -> Exceptions.unwrap(e) instanceof LockFailedException, numOfRetries, executor)
                .exceptionally(e -> {
                    Throwable unwrap = Exceptions.unwrap(e);
                    if (unwrap instanceof RetriesExhaustedException) {
                        throw new CompletionException(unwrap.getCause());
                    } else {
                        throw new CompletionException(unwrap);
                    }
                });
    }
    
    /**
     * Create stream.
     *
     * @param scope           scope.
     * @param stream          stream name.
     * @param config          stream configuration.
     * @param createTimestamp creation timestamp.
     * @return creation status.
     */
    @Task(name = "createStream", version = "1.0", resource = "{scope}/{stream}")
    public CompletableFuture<CreateStreamStatus.Status> createStream(String scope, String stream, StreamConfiguration config, long createTimestamp) {
        return execute(
                new Resource(scope, stream),
                new Serializable[]{scope, stream, config, createTimestamp},
                () -> createStreamBody(scope, stream, config, createTimestamp));
    }

    /**
     * Update stream's configuration.
     *
     * @param scope      scope.
     * @param stream     stream name.
     * @param newConfig     modified stream configuration.
     * @param contextOpt optional context
     * @return update status.
     */
    public CompletableFuture<UpdateStreamStatus.Status> updateStream(String scope, String stream, StreamConfiguration newConfig,
                                                                     OperationContext contextOpt) {
        final OperationContext context = contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;
        final long requestId = requestTracker.getRequestIdFor("updateStream", scope, stream);

        // 1. get configuration
        return streamMetadataStore.getConfigurationRecord(scope, stream, context, executor)
                .thenCompose(configProperty -> {
                    // 2. post event to start update workflow
                    if (!configProperty.getObject().isUpdating()) {
                        return eventHelper.addIndexAndSubmitTask(new UpdateStreamEvent(scope, stream, requestId),
                                // 3. update new configuration in the store with updating flag = true
                                // if attempt to update fails, we bail out with no harm done
                                () -> streamMetadataStore.startUpdateConfiguration(scope, stream, newConfig,
                                        context, executor))
                                // 4. wait for update to complete
                                .thenCompose(x -> eventHelper.checkDone(() -> isUpdated(scope, stream, newConfig, context))
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
    }

    @VisibleForTesting
    CompletableFuture<Boolean> isUpdated(String scope, String stream, StreamConfiguration newConfig, OperationContext context) {
        CompletableFuture<State> stateFuture = streamMetadataStore.getState(scope, stream, true, context, executor);
        CompletableFuture<StreamConfigurationRecord> configPropertyFuture
                = streamMetadataStore.getConfigurationRecord(scope, stream, context, executor).thenApply(VersionedMetadata::getObject);
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

    /**
     * Add a subscriber to stream metadata.
     * Needed only for Consumption based retention.
     * @param scope      scope.
     * @param stream     stream name.
     * @param newSubscriber  Id of the ReaderGroup to be added as subscriber
     * @param contextOpt optional context
     * @return update status.
     */
    public CompletableFuture<AddSubscriberStatus.Status> addSubscriber(String scope, String stream,
                                                                     String newSubscriber,
                                                                     OperationContext contextOpt) {
        final OperationContext context = contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;
        final long requestId = requestTracker.getRequestIdFor("addSubscriber", scope, stream);

        return RetryHelper.withRetriesAsync(() -> streamMetadataStore.checkStreamExists(scope, stream)
        .thenCompose(exists -> {
            // 1. check Stream exists
            if (!exists) {
                return CompletableFuture.completedFuture(AddSubscriberStatus.Status.STREAM_NOT_FOUND);
            } else {
                // 2. get subscribers data
                return Futures.exceptionallyExpecting(streamMetadataStore.getSubscriber(scope, stream, newSubscriber, context, executor),
                     e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, null)
                    .thenCompose(subscribersData -> {
                    //4. If SubscriberRecord does not exist create one...
                    if (subscribersData == null) {
                        streamMetadataStore.createSubscriber(scope, stream, newSubscriber, context, executor);
                        return CompletableFuture.completedFuture(AddSubscriberStatus.Status.SUCCESS);
                    } else {
                        return CompletableFuture.completedFuture(AddSubscriberStatus.Status.SUBSCRIBER_EXISTS);
                    }
                    })
                    .exceptionally(ex -> {
                        log.warn(requestId, "Exception thrown in trying to add subscriber {}",
                                    ex.getMessage());
                        Throwable cause = Exceptions.unwrap(ex);
                        if (cause instanceof StoreException.DataNotFoundException) {
                           return AddSubscriberStatus.Status.STREAM_NOT_FOUND;
                        } else if (cause instanceof TimeoutException) {
                           throw new CompletionException(cause);
                        } else {
                           log.warn(requestId, "Add subscriber {} failed due to {}", newSubscriber, cause);
                           return AddSubscriberStatus.Status.FAILURE;
                        }
                    });
            }
        }), e -> Exceptions.unwrap(e) instanceof RetryableException, SUBSCRIBER_OPERATION_RETRIES, executor);
    }

    /**
     * Remove a subscriber from subscribers' metadata.
     * Needed for Consumption based retention.
     * @param scope      scope.
     * @param stream     stream name.
     * @param subscriber  Id of the ReaderGroup to be added as subscriber.
     * @param contextOpt optional context
     * @return update status.
     */
    public CompletableFuture<DeleteSubscriberStatus.Status> deleteSubscriber(String scope, String stream,
                                                                             String subscriber,
                                                                             OperationContext contextOpt) {
        final OperationContext context = contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;
        final long requestId = requestTracker.getRequestIdFor("removeSubscriber", scope, stream);

        return RetryHelper.withRetriesAsync(() -> streamMetadataStore.checkStreamExists(scope, stream)
           .thenCompose(exists -> {
               // 1. check Stream exists
               if (!exists) {
                   return CompletableFuture.completedFuture(DeleteSubscriberStatus.Status.STREAM_NOT_FOUND);
               }
               // 2. remove subscriber
               return streamMetadataStore.deleteSubscriber(scope, stream, subscriber, context, executor)
                       .thenApply(x -> DeleteSubscriberStatus.Status.SUCCESS)
                       .exceptionally(ex -> {
                           log.warn(requestId, "Exception thrown when trying to remove subscriber from stream {}", ex.getMessage());
                           Throwable cause = Exceptions.unwrap(ex);
                           if (cause instanceof StoreException.DataNotFoundException) {
                               return DeleteSubscriberStatus.Status.SUBSCRIBER_NOT_FOUND;
                           } else if (cause instanceof TimeoutException) {
                               throw new CompletionException(cause);
                           } else {
                               log.warn(requestId, "Remove subscriber from stream failed due to ", cause);
                               return DeleteSubscriberStatus.Status.FAILURE;
                           }
                       });
           }), e -> Exceptions.unwrap(e) instanceof RetryableException, SUBSCRIBER_OPERATION_RETRIES, executor);
    }


    /**
     * Get list of  subscribers for Stream.
     * Needed only for Consumption based retention.
     * @param scope      scope.
     * @param stream     stream name.
     * @param contextOpt optional context
     * @return update status.
     */

    public CompletableFuture<SubscribersResponse> listSubscribers(String scope, String stream, OperationContext contextOpt) {
        final OperationContext context = contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;
        final long requestId = requestTracker.getRequestIdFor("listSubscribers", scope, stream);
        return streamMetadataStore.checkStreamExists(scope, stream)
                .thenCompose(exists -> {
                    if (!exists) {
                        return CompletableFuture.completedFuture(SubscribersResponse.newBuilder()
                                .setStatus(SubscribersResponse.Status.STREAM_NOT_FOUND).build());
                    }
                  // 2. get subscribers
                  return streamMetadataStore.listSubscribers(scope, stream, context, executor)
                          .thenApply(result -> SubscribersResponse.newBuilder()
                                     .setStatus(SubscribersResponse.Status.SUCCESS)
                                     .addAllSubscribers(result).build())
                            .exceptionally(ex -> {
                                log.warn(requestId, "Exception thrown in trying to get list of Stream subscribers. {}",
                                        ex.getMessage());
                                Throwable cause = Exceptions.unwrap(ex);
                                if (cause instanceof TimeoutException) {
                                    throw new CompletionException(cause);
                                } else {
                                    log.warn(requestId, "listSubscribers failed due to {}", ex.getMessage());
                                    return SubscribersResponse.newBuilder()
                                            .setStatus(SubscribersResponse.Status.FAILURE).build();
                                }
                            });
                });

    }

    /**
     * Remove a subscriber from subscribers' metadata.
     * Needed for Consumption based retention.
     * @param scope      scope.
     * @param stream     stream name.
     * @param subscriber  Stream Subscriber publishing the StreamCut.
     * @param truncationStreamCut  Truncation StreamCut.
     * @param contextOpt optional context
     * @return update status.
     */
    public CompletableFuture<UpdateSubscriberStatus.Status> updateSubscriberStreamCut(String scope, String stream,
                                                                             String subscriber, ImmutableMap<Long, Long> truncationStreamCut,
                                                                             OperationContext contextOpt) {
        final OperationContext context = contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;
        final long requestId = requestTracker.getRequestIdFor("updateSubscriberStreamCut", scope, stream);

        return RetryHelper.withRetriesAsync(() -> streamMetadataStore.checkStreamExists(scope, stream)
                .thenCompose(exists -> {
                            // 1. check Stream exists
                            if (!exists) {
                                return CompletableFuture.completedFuture(UpdateSubscriberStatus.Status.STREAM_NOT_FOUND);
                            }
                            // 2. check if StreamCut is valid
                            return Futures.exceptionallyExpecting(streamMetadataStore.getSubscriber(scope, stream, subscriber, contextOpt, executor),
                                    e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, null)
                                    .thenCompose(subscriberRecord -> {
                                        if (subscriberRecord == null) {
                                            return CompletableFuture.completedFuture(UpdateSubscriberStatus.Status.SUBSCRIBER_NOT_FOUND);
                                        } else {
                                            if (subscriberRecord.getObject().getTruncationStreamCut().isEmpty()) {
                                                return streamMetadataStore.isStreamCutValid(scope, stream, truncationStreamCut, context, executor)
                                                        .thenCompose( isValid -> {
                                                         if (isValid) {
                                                           return updateStreamCut(scope, stream, subscriber, truncationStreamCut, context, requestId);
                                                         } else {
                                                           return CompletableFuture.completedFuture(UpdateSubscriberStatus.Status.STREAMCUT_NOT_VALID);
                                                         }
                                                 });
                                            } else {
                                                return streamMetadataStore.isStreamCutValidForTruncation(scope, stream, truncationStreamCut,
                                                        subscriberRecord.getObject().getTruncationStreamCut(), context, executor)
                                                        .thenCompose(isValid -> {
                                                            if (isValid) {
                                                                return updateStreamCut(scope, stream, subscriber, truncationStreamCut, context, requestId);
                                                            } else {
                                                                return CompletableFuture.completedFuture(UpdateSubscriberStatus.Status.STREAMCUT_NOT_VALID);
                                                            }
                                                        });
                                            }
                                    }
                            });
        }), e -> Exceptions.unwrap(e) instanceof RetryableException, SUBSCRIBER_OPERATION_RETRIES, executor);
    }

    private CompletableFuture<UpdateSubscriberStatus.Status> updateStreamCut(String scope, String stream, String subscriber,
                                                                             ImmutableMap<Long, Long> truncationStreamCut,
                                                                             OperationContext context, long requestId) {
        // 3. Update the StreamCut
        return streamMetadataStore.updateSubscriberStreamCut(scope, stream, subscriber, truncationStreamCut, context, executor)
                .thenApply(x -> UpdateSubscriberStatus.Status.SUCCESS)
                .exceptionally(ex -> {
                    log.warn(requestId, "Exception thrown when trying to remove subscriber from stream {}", ex.getMessage());
                    Throwable cause = Exceptions.unwrap(ex);
                    if (cause instanceof StoreException.DataNotFoundException) {
                        return UpdateSubscriberStatus.Status.SUBSCRIBER_NOT_FOUND;
                    } else if (cause instanceof TimeoutException) {
                        throw new CompletionException(cause);
                    } else {
                        log.warn(requestId, "Remove subscriber from stream failed due to ", cause);
                        return UpdateSubscriberStatus.Status.FAILURE;
                    }
                });
    }

    /**
     * Method to check retention policy and generate new periodic cuts and/or truncate stream at an existing stream cut.
     * 
     * @param scope scope
     * @param stream stream
     * @param policy retention policy
     * @param recordingTime time of recording
     * @param contextOpt operation context
     * @param delegationToken token to be sent to segmentstore to authorize this operation.
     * @return future.
     */
    public CompletableFuture<Void> retention(final String scope, final String stream, final RetentionPolicy policy,
                                             final long recordingTime, final OperationContext contextOpt, final String delegationToken) {
        Preconditions.checkNotNull(policy);
        final OperationContext context = contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;
        final long requestId = requestTracker.getRequestIdFor("truncateStream", scope, stream);

        return streamMetadataStore.getRetentionSet(scope, stream, context, executor)
                .thenCompose(retentionSet -> {
                    StreamCutReferenceRecord latestCut = retentionSet.getLatest();
                    
                    return generateStreamCutIfRequired(scope, stream, latestCut, recordingTime, context, delegationToken)
                            .thenCompose(newRecord -> truncate(scope, stream, policy, context, retentionSet, newRecord, recordingTime, requestId));
                })
                .thenAccept(x -> StreamMetrics.reportRetentionEvent(scope, stream));

    }

    private CompletableFuture<StreamCutRecord> generateStreamCutIfRequired(String scope, String stream,
                                                                           StreamCutReferenceRecord previous, long recordingTime,
                                                                           OperationContext context, String delegationToken) {
        if (previous == null || recordingTime - previous.getRecordingTime() > retentionFrequencyMillis.get()) {
            return Futures.exceptionallyComposeExpecting(
                    previous == null ? CompletableFuture.completedFuture(null) :
                            streamMetadataStore.getStreamCutRecord(scope, stream, previous, context, executor),
                    e -> e instanceof StoreException.DataNotFoundException, () -> null)
                          .thenCompose(previousRecord -> generateStreamCut(scope, stream, previousRecord, context, delegationToken)
                                  .thenCompose(newRecord -> streamMetadataStore.addStreamCutToRetentionSet(scope, stream, newRecord, context, executor)
                                                                               .thenApply(x -> {
                                                                                   log.debug("New streamCut generated for stream {}/{}", scope, stream);
                                                                                   return newRecord;
                                                                               })));
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    private CompletableFuture<Void> truncate(String scope, String stream, RetentionPolicy policy, OperationContext context,
                                             RetentionSet retentionSet, StreamCutRecord newRecord, long recordingTime, long requestId) {
        Optional<StreamCutReferenceRecord> truncationRecord = findTruncationRecord(policy, retentionSet, newRecord, recordingTime);
        if (!truncationRecord.isPresent()) {
            log.info("No suitable truncation record found, per retention policy for stream {}/{}", scope, stream);
            return CompletableFuture.completedFuture(null);
        }
        log.info("Found truncation record for stream {}/{} truncationRecord time/size: {}/{}", scope, stream,
                truncationRecord.get().getRecordingTime(), truncationRecord.get().getRecordingSize());
        return streamMetadataStore.getStreamCutRecord(scope, stream, truncationRecord.get(), context, executor)
                   .thenCompose(streamCutRecord -> startTruncation(scope, stream, streamCutRecord.getStreamCut(), context, requestId))
                   .thenCompose(started -> {
                       if (started) {
                                return streamMetadataStore.deleteStreamCutBefore(scope, stream, truncationRecord.get(), context, executor);
                            } else {
                                throw new RuntimeException("Could not start truncation");
                       }
                   })
                   .exceptionally(e -> {
                       if (Exceptions.unwrap(e) instanceof IllegalArgumentException) {
                           // This is ignorable exception. Throwing this will cause unnecessary retries and exceptions logged.
                           log.debug(requestId, "Cannot truncate at given " +
                                        "streamCut because it intersects with existing truncation point");
                                return null;
                           } else {
                                throw new CompletionException(e);
                          }
                       });
    }

    private Optional<StreamCutReferenceRecord> findTruncationRecord(RetentionPolicy policy, RetentionSet retentionSet,
                                                           StreamCutRecord newRecord, long recordingTime) {
        switch (policy.getRetentionType()) {
            case TIME:
                return retentionSet.getRetentionRecords().stream().filter(x -> x.getRecordingTime() < recordingTime - policy.getRetentionParam())
                        .max(Comparator.comparingLong(StreamCutReferenceRecord::getRecordingTime));
            case SIZE:
                // find a stream cut record Si from retentionSet R = {S1.. Sn} such that Sn.size - Si.size > policy and
                // Sn.size - Si+1.size < policy
                Optional<StreamCutRecord> latestOpt = Optional.ofNullable(newRecord);

                return latestOpt.flatMap(latest ->
                        retentionSet.getRetentionRecords().stream().filter(x -> (latest.getRecordingSize() - x.getRecordingSize()) > policy.getRetentionParam())
                                .max(Comparator.comparingLong(StreamCutReferenceRecord::getRecordingTime)));
            default:
                throw new NotImplementedException(policy.getRetentionType().toString());
        }
    }

    /**
     * Generate a new stream cut.
     *
     * @param scope      scope.
     * @param stream     stream name.
     * @param contextOpt optional context
     * @param delegationToken token to be sent to segmentstore.
     * @param previous previous stream cut record
     * @return streamCut.
     */
    public CompletableFuture<StreamCutRecord> generateStreamCut(final String scope, final String stream, final StreamCutRecord previous,
                                                                final OperationContext contextOpt, String delegationToken) {
        final OperationContext context = contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;

        return streamMetadataStore.getActiveSegments(scope, stream, context, executor)
                .thenCompose(activeSegments -> Futures.allOfWithResults(activeSegments
                        .stream()
                        .parallel()
                        .collect(Collectors.toMap(x -> x, x -> getSegmentOffset(scope, stream, x.segmentId(), delegationToken)))))
                .thenCompose(map -> {
                    final long generationTime = System.currentTimeMillis();
                    ImmutableMap.Builder<Long, Long> builder = ImmutableMap.builder();
                    map.forEach((key, value) -> builder.put(key.segmentId(), value));
                    ImmutableMap<Long, Long> streamCutMap = builder.build();
                    return streamMetadataStore.getSizeTillStreamCut(scope, stream, streamCutMap, Optional.ofNullable(previous), context, executor)
                                              .thenApply(sizeTill -> new StreamCutRecord(generationTime, sizeTill, streamCutMap));
                });
    }

    /**
     * Truncate a stream.
     *
     * @param scope      scope.
     * @param stream     stream name.
     * @param streamCut  stream cut.
     * @param contextOpt optional context
     * @return update status.
     */
    public CompletableFuture<UpdateStreamStatus.Status> truncateStream(final String scope, final String stream,
                                                                       final Map<Long, Long> streamCut,
                                                                       final OperationContext contextOpt) {
        final OperationContext context = contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;
        final long requestId = requestTracker.getRequestIdFor("truncateStream", scope, stream);

        // 1. get stream cut
        return startTruncation(scope, stream, streamCut, context, requestId)
                // 4. check for truncation to complete
                .thenCompose(truncationStarted -> {
                    if (truncationStarted) {
                        return eventHelper.checkDone(() -> isTruncated(scope, stream, streamCut, context), 1000L)
                                .thenApply(y -> UpdateStreamStatus.Status.SUCCESS);
                    } else {
                        log.warn(requestId, "Unable to start truncation for {}/{}", scope, stream);
                        return CompletableFuture.completedFuture(UpdateStreamStatus.Status.FAILURE);
                    }
                })
                .exceptionally(ex -> {
                    log.warn(requestId, "Exception thrown in trying to update stream configuration", ex);
                    return handleUpdateStreamError(ex, requestId);
                });
    }

    public CompletableFuture<Boolean> startTruncation(String scope, String stream, Map<Long, Long> streamCut,
                                                       OperationContext contextOpt, long requestId) {
        final OperationContext context = contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;

        return streamMetadataStore.getTruncationRecord(scope, stream, context, executor)
                .thenCompose(property -> {
                    if (!property.getObject().isUpdating()) {
                        // 2. post event with new stream cut if no truncation is ongoing
                        return eventHelper.addIndexAndSubmitTask(new TruncateStreamEvent(scope, stream, requestId),
                                // 3. start truncation by updating the metadata
                                () -> streamMetadataStore.startTruncation(scope, stream, streamCut,
                                        context, executor))
                                .thenApply(x -> {
                                    log.debug(requestId, "Started truncation request for stream {}/{}", scope, stream);
                                    return true;
                                });
                    } else {
                        log.warn(requestId, "Another truncation in progress for {}/{}", scope, stream);
                        return CompletableFuture.completedFuture(false);
                    }
                });
    }

    @VisibleForTesting
    CompletableFuture<Boolean> isTruncated(String scope, String stream, Map<Long, Long> streamCut, OperationContext context) {
        CompletableFuture<State> stateFuture = streamMetadataStore.getState(scope, stream, true, context, executor);
        CompletableFuture<StreamTruncationRecord> configPropertyFuture
                = streamMetadataStore.getTruncationRecord(scope, stream, context, executor).thenApply(VersionedMetadata::getObject);
        return CompletableFuture.allOf(stateFuture, configPropertyFuture)
                                .thenApply(v -> {
                                    State state = stateFuture.join();
                                    StreamTruncationRecord truncationRecord = configPropertyFuture.join();

                                    // if property is updating and doesn't match our request, it's a subsequent update
                                    if (truncationRecord.isUpdating()) {
                                        return !truncationRecord.getStreamCut().equals(streamCut);
                                    } else {
                                        // if truncate-barrier is not updating, then truncate is complete if property matches our expectation 
                                        // and state is not updating 
                                        return !(truncationRecord.getStreamCut().equals(streamCut) && state.equals(State.TRUNCATING));
                                    }
                                });
    }

    /**
     * Seal a stream.
     *
     * @param scope      scope.
     * @param stream     stream name.
     * @param contextOpt optional context
     * @return update status.
     */
    public CompletableFuture<UpdateStreamStatus.Status> sealStream(String scope, String stream, OperationContext contextOpt) {
        return sealStream(scope, stream, contextOpt, 10);
    }

    @VisibleForTesting
    CompletableFuture<UpdateStreamStatus.Status> sealStream(String scope, String stream, OperationContext contextOpt, int retryCount) {
        final OperationContext context = contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;
        final long requestId = requestTracker.getRequestIdFor("sealStream", scope, stream);

        // 1. post event for seal.
        SealStreamEvent event = new SealStreamEvent(scope, stream, requestId);
        return eventHelper.addIndexAndSubmitTask(event,
                // 2. set state to sealing
                () -> RetryHelper.withRetriesAsync(() -> streamMetadataStore.getVersionedState(scope, stream, context, executor)
                .thenCompose(state -> {
                    if (state.getObject().equals(State.SEALED)) {
                        return CompletableFuture.completedFuture(state);
                    } else {
                        return streamMetadataStore.updateVersionedState(scope, stream, State.SEALING, state, context, executor);
                    }
                }), RetryHelper.RETRYABLE_PREDICATE.or(e -> Exceptions.unwrap(e) instanceof StoreException.OperationNotAllowedException), retryCount, executor))
                // 3. return with seal initiated.
                .thenCompose(result -> {
                    if (result.getObject().equals(State.SEALED) || result.getObject().equals(State.SEALING)) {
                        return eventHelper.checkDone(() -> isSealed(scope, stream, context))
                                .thenApply(x -> UpdateStreamStatus.Status.SUCCESS);
                    } else {
                        return CompletableFuture.completedFuture(UpdateStreamStatus.Status.FAILURE);
                    }
                })
                .exceptionally(ex -> {
                    log.warn(requestId, "Exception thrown in trying to notify sealed segments {}", ex.getMessage());
                    return handleUpdateStreamError(ex, requestId);
                });
    }

    private CompletableFuture<Boolean> isSealed(String scope, String stream, OperationContext context) {
        return streamMetadataStore.getState(scope, stream, true, context, executor)
                .thenApply(state -> state.equals(State.SEALED));
    }

    /**
     * Delete a stream. Precondition for deleting a stream is that the stream sholud be sealed.
     *
     * @param scope      scope.
     * @param stream     stream name.
     * @param contextOpt optional context
     * @return delete status.
     */
    public CompletableFuture<DeleteStreamStatus.Status> deleteStream(final String scope, final String stream,
                                                                     final OperationContext contextOpt) {
        final OperationContext context = contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;
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
                streamMetadataStore.getState(scope, stream, false, context, executor), 
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, State.UNKNOWN)
                .thenCompose(state -> {
                    if (State.SEALED.equals(state) || State.CREATING.equals(state)) {
                        return streamMetadataStore.getCreationTime(scope, stream, context, executor)
                                                  .thenApply(time -> new DeleteStreamEvent(scope, stream, requestId, time))
                                                  .thenCompose(event -> eventHelper.writeEvent(event))
                                                  .thenApply(x -> true);
                    } else if (State.UNKNOWN.equals(state)) {
                        // Since the state is not created, so the segments and state 
                        // are definitely not created.
                        // so we can simply delete the stream metadata which deletes stream from scope as well. 
                        return streamMetadataStore.deleteStream(scope, stream, context, executor)
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
                        return eventHelper.checkDone(() -> isDeleted(scope, stream))
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
        return streamMetadataStore.checkStreamExists(scope, stream)
                .thenApply(x -> !x);
    }

    /**
     * Helper method to perform scale operation against an scale request.
     * This method posts a request in the request stream and then starts the scale operation while
     * tracking it's progress. Eventually, after scale completion, it sends a response to the caller.
     *
     * @param scope          scope.
     * @param stream         stream name.
     * @param segmentsToSeal segments to be sealed.
     * @param newRanges      key ranges for new segments.
     * @param scaleTimestamp scaling time stamp.
     * @param context        optional context
     * @return returns the newly created segments.
     */
    public CompletableFuture<ScaleResponse> manualScale(String scope, String stream, List<Long> segmentsToSeal,
                                                        List<Map.Entry<Double, Double>> newRanges, long scaleTimestamp,
                                                        OperationContext context) {
        final long requestId = requestTracker.getRequestIdFor("scaleStream", scope, stream, String.valueOf(scaleTimestamp));
        ScaleOpEvent event = new ScaleOpEvent(scope, stream, segmentsToSeal, newRanges, true, scaleTimestamp, requestId);

        return eventHelper.addIndexAndSubmitTask(event,
                () -> streamMetadataStore.submitScale(scope, stream, segmentsToSeal, new ArrayList<>(newRanges),
                        scaleTimestamp, null, context, executor))
                        .handle((startScaleResponse, e) -> {
                            ScaleResponse.Builder response = ScaleResponse.newBuilder();

                            if (e != null) {
                                Throwable cause = Exceptions.unwrap(e);
                                if (cause instanceof EpochTransitionOperationExceptions.PreConditionFailureException) {
                                    response.setStatus(ScaleResponse.ScaleStreamStatus.PRECONDITION_FAILED);
                                } else {
                                    log.warn(requestId, "Scale for stream {}/{} failed with exception {}", scope, stream, cause);
                                    response.setStatus(ScaleResponse.ScaleStreamStatus.FAILURE);
                                }
                            } else {
                                log.info(requestId, "scale for stream {}/{} started successfully", scope, stream);
                                response.setStatus(ScaleResponse.ScaleStreamStatus.STARTED);
                                response.addAllSegments(
                                        startScaleResponse.getObject().getNewSegmentsWithRange().entrySet()
                                                .stream()
                                                .map(segment -> convert(scope, stream, segment))
                                                .collect(Collectors.toList()));
                                response.setEpoch(startScaleResponse.getObject().getActiveEpoch());
                            }
                            return response.build();
                        });
    }

    /**
     * Helper method to check if scale operation against an epoch completed or not.
     *
     * @param scope          scope.
     * @param stream         stream name.
     * @param epoch          stream epoch.
     * @param context        optional context
     * @return returns the newly created segments.
     */
    public CompletableFuture<ScaleStatusResponse> checkScale(String scope, String stream, int epoch,
                                                                        OperationContext context) {
        CompletableFuture<EpochRecord> activeEpochFuture =
                streamMetadataStore.getActiveEpoch(scope, stream, context, true, executor);
        CompletableFuture<State> stateFuture =
                streamMetadataStore.getState(scope, stream, true, context, executor);
        CompletableFuture<EpochTransitionRecord> etrFuture =
                streamMetadataStore.getEpochTransition(scope, stream, context, executor).thenApply(VersionedMetadata::getObject);
        return CompletableFuture.allOf(stateFuture, activeEpochFuture)
                        .handle((r, ex) -> {
                            ScaleStatusResponse.Builder response = ScaleStatusResponse.newBuilder();

                            if (ex != null) {
                                Throwable e = Exceptions.unwrap(ex);
                                if (e instanceof StoreException.DataNotFoundException) {
                                    response.setStatus(ScaleStatusResponse.ScaleStatus.INVALID_INPUT);
                                } else {
                                    response.setStatus(ScaleStatusResponse.ScaleStatus.INTERNAL_ERROR);
                                }
                            } else {
                                EpochRecord activeEpoch = activeEpochFuture.join();
                                State state = stateFuture.join();
                                EpochTransitionRecord etr = etrFuture.join();
                                if (epoch > activeEpoch.getEpoch()) {
                                    response.setStatus(ScaleStatusResponse.ScaleStatus.INVALID_INPUT);
                                } else if (activeEpoch.getEpoch() == epoch || activeEpoch.getReferenceEpoch() == epoch) {
                                    response.setStatus(ScaleStatusResponse.ScaleStatus.IN_PROGRESS);
                                } else {
                                    // active epoch == scale epoch + 1 but the state is scaling, the previous workflow 
                                    // has not completed.
                                    if (epoch + 1 == activeEpoch.getReferenceEpoch() && state.equals(State.SCALING) &&
                                            (etr.equals(EpochTransitionRecord.EMPTY) || etr.getNewEpoch() == activeEpoch.getEpoch())) {
                                        response.setStatus(ScaleStatusResponse.ScaleStatus.IN_PROGRESS);
                                    } else {
                                        response.setStatus(ScaleStatusResponse.ScaleStatus.SUCCESS);
                                    }
                                }
                            }
                                return response.build();
                            });
    }

    @VisibleForTesting
    <T> CompletableFuture<T> addIndexAndSubmitTask(ControllerEvent event, Supplier<CompletableFuture<T>> futureSupplier) {
        return eventHelper.addIndexAndSubmitTask(event, futureSupplier);
    }

    public CompletableFuture<Void> writeEvent(ControllerEvent event) {
        return eventHelper.writeEvent(event);
    }

    @VisibleForTesting
    public void setRequestEventWriter(EventStreamWriter<ControllerEvent> requestEventWriter) {
        eventHelper.setRequestEventWriter(requestEventWriter);
    }

    CompletableFuture<Void> removeTaskFromIndex(String hostId, String id) {
        return eventHelper.removeTaskFromIndex(hostId, id);
    }

    @VisibleForTesting
    CompletableFuture<CreateStreamStatus.Status> createStreamBody(String scope, String stream, StreamConfiguration config, long timestamp) {
        final long requestId = requestTracker.getRequestIdFor("createStream", scope, stream);
        return this.streamMetadataStore.createStream(scope, stream, config, timestamp, null, executor)
                .thenComposeAsync(response -> {
                    log.info(requestId, "{}/{} created in metadata store", scope, stream);
                    CreateStreamStatus.Status status = translate(response.getStatus());
                    // only if its a new stream or an already existing non-active stream then we will create
                    // segments and change the state of the stream to active.
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
                            return CreateStreamStatus.Status.SCOPE_NOT_FOUND;
                        } else {
                            log.warn(requestId, "Create stream failed due to ", ex);
                            return CreateStreamStatus.Status.FAILURE;
                        }
                    } else {
                        return result;
                    }
                });
    }

    /**
     * Method to create mark stream linked to the base stream. Mark Stream is a special single segmented dedicated internal stream where
     * watermarks for the said stream are stored. 
     * @param scope scope for base stream
     * @param baseStream name of base stream
     * @param timestamp timestamp 
     * @param requestId request id for stream creation. 
     * @return Completable future which is completed successfully when the internal mark stream is created
     */
    private CompletableFuture<Void> createMarkStream(String scope, String baseStream, long timestamp, long requestId) {
        String markStream = NameUtils.getMarkStreamForStream(baseStream);
        StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build();
        return this.streamMetadataStore.createStream(scope, markStream, config, timestamp, null, executor)
                                .thenCompose(response -> {
                                    final long segmentId = NameUtils.computeSegmentId(response.getStartingSegmentNumber(), 0);
                                    return notifyNewSegment(scope, markStream, segmentId, response.getConfiguration().getScalingPolicy(),
                                            this.retrieveDelegationToken(), requestId);
                                })
                                .thenCompose(v -> {
                                    final OperationContext context = streamMetadataStore.createContext(scope, markStream);

                                    return streamMetadataStore.getVersionedState(scope, markStream, context, executor)
                                                       .thenCompose(state -> 
                                                               Futures.toVoid(streamMetadataStore.updateVersionedState(scope, markStream, State.ACTIVE,
                                                               state, context, executor)));
                                });
    }

    private CreateStreamStatus.Status translate(CreateStreamResponse.CreateStatus status) {
        CreateStreamStatus.Status retVal;
        switch (status) {
            case NEW:
                retVal = CreateStreamStatus.Status.SUCCESS;
                break;
            case EXISTS_ACTIVE:
            case EXISTS_CREATING:
                retVal = CreateStreamStatus.Status.STREAM_EXISTS;
                break;
            case FAILED:
            default:
                retVal = CreateStreamStatus.Status.FAILURE;
                break;
        }
        return retVal;
    }

    public CompletableFuture<Void> notifyNewSegments(String scope, String stream, List<Long> segmentIds, OperationContext context,
                                                     String controllerToken) {
        return notifyNewSegments(scope, stream, segmentIds, context, controllerToken, RequestTag.NON_EXISTENT_ID);
    }

    public CompletableFuture<Void> notifyNewSegments(String scope, String stream, List<Long> segmentIds, OperationContext context,
                                                     String controllerToken, long requestId) {
        return withRetries(() -> streamMetadataStore.getConfiguration(scope, stream, context, executor), executor)
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

    public CompletableFuture<Void> notifyTruncateSegment(String scope, String stream, Map.Entry<Long, Long> segmentCut,
                                                         String delegationToken, long requestId) {
        return Futures.toVoid(withRetries(() -> segmentHelper.truncateSegment(scope, stream, segmentCut.getKey(),
                segmentCut.getValue(), delegationToken, requestId), executor));
    }

    public CompletableFuture<Map<Long, Long>> getSealedSegmentsSize(String scope, String stream, List<Long> segments, String delegationToken) {
        return Futures.allOfWithResults(
                segments
                        .stream()
                        .parallel()
                        .collect(Collectors.toMap(x -> x, x -> getSegmentOffset(scope, stream, x, delegationToken))));
    }

    public CompletableFuture<Void> notifySealedSegments(String scope, String stream, List<Long> sealedSegments,
                                                        String delegationToken) {
        return notifySealedSegments(scope, stream, sealedSegments, delegationToken, RequestTag.NON_EXISTENT_ID);
    }

    public CompletableFuture<Void> notifySealedSegments(String scope, String stream, List<Long> sealedSegments,
                                                         String delegationToken, long requestId) {
        return Futures.allOf(
                sealedSegments
                        .stream()
                        .parallel()
                        .map(id -> notifySealedSegment(scope, stream, id, delegationToken, requestId))
                        .collect(Collectors.toList()));
    }

    private CompletableFuture<Void> notifySealedSegment(final String scope, final String stream, final long sealedSegment,
                                                        String delegationToken, long requestId) {
        return Futures.toVoid(withRetries(() -> segmentHelper.sealSegment(
                scope,
                stream,
                sealedSegment,
                delegationToken, requestId), executor));
    }

    public CompletableFuture<Void> notifyPolicyUpdates(String scope, String stream, List<StreamSegmentRecord> activeSegments,
                                                       ScalingPolicy policy, String delegationToken, long requestId) {
        return Futures.toVoid(Futures.allOfWithResults(activeSegments
                .stream()
                .parallel()
                .map(segment -> notifyPolicyUpdate(scope, stream, policy, segment.segmentId(), delegationToken, requestId))
                .collect(Collectors.toList())));
    }

    private CompletableFuture<Long> getSegmentOffset(String scope, String stream, long segmentId, String delegationToken) {

        return withRetries(() -> segmentHelper.getSegmentInfo(
                scope,
                stream,
                segmentId,
                delegationToken), executor)
                .thenApply(WireCommands.StreamSegmentInfo::getWriteOffset);
    }

    private CompletableFuture<Void> notifyPolicyUpdate(String scope, String stream, ScalingPolicy policy, long segmentId,
                                                       String delegationToken, long requestId) {
        return withRetries(() -> segmentHelper.updatePolicy(
                scope,
                stream,
                policy,
                segmentId,
                delegationToken, requestId), executor);
    }

    private SegmentRange convert(String scope, String stream, Map.Entry<Long, Map.Entry<Double, Double>> segment) {
        return ModelHelper.createSegmentRange(scope, stream, segment.getKey(), segment.getValue().getKey(),
                segment.getValue().getValue());
    }

    private UpdateStreamStatus.Status handleUpdateStreamError(Throwable ex, long requestId) {
        Throwable cause = Exceptions.unwrap(ex);
        if (cause instanceof StoreException.DataNotFoundException) {
            return UpdateStreamStatus.Status.STREAM_NOT_FOUND;
        } else if (cause instanceof TimeoutException) {
            throw new CompletionException(cause);
        } else {
            log.warn(requestId, "Update stream failed due to ", cause);
            return UpdateStreamStatus.Status.FAILURE;
        }
    }

    private DeleteStreamStatus.Status handleDeleteStreamError(Throwable ex, long requestId) {
        Throwable cause = Exceptions.unwrap(ex);
        if (cause instanceof StoreException.DataNotFoundException) {
            return DeleteStreamStatus.Status.STREAM_NOT_FOUND;
        } else if (cause instanceof TimeoutException) {
            throw new CompletionException(cause);
        } else {
            log.warn(requestId, "Delete stream failed.", ex);
            return DeleteStreamStatus.Status.FAILURE;
        }
    }

    public CompletableFuture<Void> notifyTxnCommit(final String scope, final String stream,
                                                   final List<Long> segments, final UUID txnId) {
        Timer timer = new Timer();
        return Futures.allOf(segments.stream()
                .parallel()
                .map(segment -> notifyTxnCommit(scope, stream, segment, txnId))
                .collect(Collectors.toList()))
                .thenRun(() -> TransactionMetrics.getInstance().commitTransactionSegments(timer.getElapsed()));
    }

    private CompletableFuture<Controller.TxnStatus> notifyTxnCommit(final String scope, final String stream,
                                                                    final long segmentNumber, final UUID txnId) {
        return TaskStepsRetryHelper.withRetries(() -> segmentHelper.commitTransaction(scope,
                stream,
                segmentNumber,
                segmentNumber,
                txnId,
                this.retrieveDelegationToken()), executor);
    }

    public CompletableFuture<Void> notifyTxnAbort(final String scope, final String stream,
                                                  final List<Long> segments, final UUID txnId) {
        Timer timer = new Timer();
        return Futures.allOf(segments.stream()
                .parallel()
                .map(segment -> notifyTxnAbort(scope, stream, segment, txnId))
                .collect(Collectors.toList()))
                .thenRun(() -> TransactionMetrics.getInstance().abortTransactionSegments(timer.getElapsed()));
    }

    private CompletableFuture<Controller.TxnStatus> notifyTxnAbort(final String scope, final String stream,
                                                                   final long segmentNumber, final UUID txnId) {
        return TaskStepsRetryHelper.withRetries(() -> segmentHelper.abortTransaction(scope,
                stream,
                segmentNumber,
                txnId,
                this.retrieveDelegationToken()), executor);
    }

    public CompletableFuture<Map<Long, Long>> getCurrentSegmentSizes(String scope, String stream, List<Long> segments) {
        return Futures.allOfWithResults(segments.stream().collect(
                Collectors.toMap(x -> x, x -> getSegmentOffset(scope, stream, x, this.retrieveDelegationToken()))));
    }

    @Override
    public TaskBase copyWithContext(Context context) {
        return new StreamMetadataTasks(streamMetadataStore,
                bucketStore, 
                taskMetadataStore,
                segmentHelper,
                executor,
                eventExecutor,
                context,
                authHelper,
                requestTracker);
    }

    @Override
    public void close() throws Exception {
        if (eventHelper != null) {
            eventHelper.close();
        }

    }

    public String retrieveDelegationToken() {
        return authHelper.retrieveMasterToken();
    }

    @VisibleForTesting
    public void setCompletionTimeoutMillis(long timeoutMillis) {
        eventHelper.setCompletionTimeoutMillis(timeoutMillis);
    }
}

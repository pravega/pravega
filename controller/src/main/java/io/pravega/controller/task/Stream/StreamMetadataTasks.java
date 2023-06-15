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
package io.pravega.controller.task.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.control.impl.ModelHelper;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.Timer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.TagLogger;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.controller.metrics.StreamMetrics;
import io.pravega.controller.metrics.TransactionMetrics;
import io.pravega.controller.retryable.RetryableException;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.WireCommandFailedException;
import io.pravega.controller.server.eventProcessor.ControllerEventProcessors;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.stream.AbstractStreamMetadataStore;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.CreateStreamResponse;
import io.pravega.controller.store.stream.EpochTransitionOperationExceptions;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.ReaderGroupState;
import io.pravega.controller.store.stream.State;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;
import io.pravega.controller.store.stream.records.ReaderGroupConfigRecord;
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
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateReaderGroupResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteReaderGroupStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.ReaderGroupConfigResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.ReaderGroupConfiguration;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScaleStatusResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentRange;
import io.pravega.controller.stream.api.grpc.v1.Controller.StreamCut;
import io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo;
import io.pravega.controller.stream.api.grpc.v1.Controller.SubscribersResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.UpdateReaderGroupResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.UpdateSubscriberStatus;
import io.pravega.controller.task.EventHelper;
import io.pravega.controller.task.Task;
import io.pravega.controller.task.TaskBase;
import io.pravega.controller.util.Config;
import io.pravega.controller.util.RetryHelper;
import io.pravega.shared.NameUtils;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.shared.controller.event.CreateReaderGroupEvent;
import io.pravega.shared.controller.event.DeleteReaderGroupEvent;
import io.pravega.shared.controller.event.DeleteScopeEvent;
import io.pravega.shared.controller.event.DeleteStreamEvent;
import io.pravega.shared.controller.event.RGStreamCutRecord;
import io.pravega.shared.controller.event.ScaleOpEvent;
import io.pravega.shared.controller.event.SealStreamEvent;
import io.pravega.shared.controller.event.TruncateStreamEvent;
import io.pravega.shared.controller.event.UpdateReaderGroupEvent;
import io.pravega.shared.controller.event.UpdateStreamEvent;
import io.pravega.shared.protocol.netty.WireCommands;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.io.Serializable;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.pravega.controller.task.Stream.TaskStepsRetryHelper.withRetries;
import static io.pravega.shared.NameUtils.getQualifiedStreamSegmentName;

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
    private static final int READER_GROUP_OPERATION_MAX_RETRIES = 10;
    private static final int SCOPE_DELETION_MAX_RETRIES = 10;
    private static final long READER_GROUP_SEGMENT_ROLLOVER_SIZE_BYTES = 4 * 1024 * 1024; // 4MB
    private final AtomicLong retentionFrequencyMillis;

    private final StreamMetadataStore streamMetadataStore;
    private final BucketStore bucketStore;
    private final SegmentHelper segmentHelper;

    private final GrpcAuthHelper authHelper;
    private final ScheduledExecutorService eventExecutor;
    private final CompletableFuture<EventHelper> eventHelperFuture;
    private final AtomicReference<Supplier<Long>> retentionClock;

    @GuardedBy("lock")
    private EventHelper eventHelper = null;
    @GuardedBy("lock")
    private boolean toSetEventHelper = true;
    private final Object lock = new Object();

    public StreamMetadataTasks(final StreamMetadataStore streamMetadataStore,
                               BucketStore bucketStore, final TaskMetadataStore taskMetadataStore,
                               final SegmentHelper segmentHelper, final ScheduledExecutorService executor,
                               final ScheduledExecutorService eventExecutor, final String hostId,
                               GrpcAuthHelper authHelper, final long retentionFrequencyMillis) {
        this(streamMetadataStore, bucketStore, taskMetadataStore, segmentHelper, executor, eventExecutor, new Context(hostId),
                authHelper);
        this.retentionFrequencyMillis.set(retentionFrequencyMillis);
    }

    @VisibleForTesting
    public StreamMetadataTasks(final StreamMetadataStore streamMetadataStore,
                               BucketStore bucketStore, final TaskMetadataStore taskMetadataStore,
                               final SegmentHelper segmentHelper, final ScheduledExecutorService executor,
                               final String hostId, GrpcAuthHelper authHelper) {
        this(streamMetadataStore, bucketStore, taskMetadataStore, segmentHelper, executor, executor, new Context(hostId),
             authHelper);
    }

    @VisibleForTesting
    public StreamMetadataTasks(final StreamMetadataStore streamMetadataStore,
                               BucketStore bucketStore, final TaskMetadataStore taskMetadataStore,
                               final SegmentHelper segmentHelper, final ScheduledExecutorService executor,
                               final String hostId, GrpcAuthHelper authHelper,
                               final EventHelper helper) {
        this(streamMetadataStore, bucketStore, taskMetadataStore, segmentHelper, executor, executor, new Context(hostId),
                authHelper);
        this.eventHelperFuture.complete(helper);
    }

    private StreamMetadataTasks(final StreamMetadataStore streamMetadataStore,
                                BucketStore bucketStore, final TaskMetadataStore taskMetadataStore,
                                final SegmentHelper segmentHelper, final ScheduledExecutorService executor,
                                final ScheduledExecutorService eventExecutor, final Context context,
                                GrpcAuthHelper authHelper) {
        super(taskMetadataStore, executor, context);
        this.eventExecutor = eventExecutor;
        this.streamMetadataStore = streamMetadataStore;
        this.bucketStore = bucketStore;
        this.segmentHelper = segmentHelper;
        this.authHelper = authHelper;
        this.retentionFrequencyMillis = new AtomicLong(Duration.ofMinutes(Config.MINIMUM_RETENTION_FREQUENCY_IN_MINUTES).toMillis());
        this.retentionClock = new AtomicReference<>(System::currentTimeMillis);
        this.eventHelperFuture = new CompletableFuture<>();
        this.setReady();
    }


    public void initializeStreamWriters(final EventStreamClientFactory clientFactory,
                                        final String streamName) {

        EventHelper e = null;
        synchronized (lock) {
            if (toSetEventHelper) {
                this.eventHelper = new EventHelper(clientFactory.createEventWriter(streamName,
                        ControllerEventProcessors.CONTROLLER_EVENT_SERIALIZER,
                        EventWriterConfig.builder().enableConnectionPooling(true).retryAttempts(Integer.MAX_VALUE).build()),
                        this.executor, this.eventExecutor, this.context.getHostId(),
                        ((AbstractStreamMetadataStore) this.streamMetadataStore).getHostTaskIndex());
                toSetEventHelper = false;
                e = eventHelper;
            }
        }

        if (e != null) {
            eventHelperFuture.complete(e);
        }
    }

    /**
     * Method to create a State Synchronizer Stream for a Reader Group.
     *
     * @param scope           scope.
     * @param stream          State Synchronizer stream name.
     * @param config          stream configuration.
     * @param createTimestamp creation timestamp.
     * @param numOfRetries    number of retries for LockFailedException
     * @param requestId       requestId
     * @return CompletableFuture which when completed will have creation status for the stream.
     */
    public CompletableFuture<CreateStreamStatus.Status> createRGStream(String scope, String stream, StreamConfiguration config,
                                                                       long createTimestamp, int numOfRetries, long requestId) {
        Preconditions.checkNotNull(config, "streamConfig");
        Preconditions.checkArgument(createTimestamp >= 0);
        NameUtils.validateStreamName(stream);
        OperationContext context = streamMetadataStore.createStreamContext(scope, stream, requestId);

        return Futures.exceptionallyExpecting(streamMetadataStore.getState(scope, stream, true, context, executor),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, State.UNKNOWN)
                .thenCompose(state -> {
                    if (state.equals(State.UNKNOWN) || state.equals(State.CREATING)) {
                        log.debug(requestId, "Creating StateSynchronizer Stream {}", stream);
                        return createStreamRetryOnLockFailure(scope,
                                stream,
                                config,
                                createTimestamp,
                                numOfRetries,
                                requestId);
                    } else {
                        return CompletableFuture.completedFuture(CreateStreamStatus.Status.STREAM_EXISTS);
                    }
                });
    }

    /**
    * Method to create stream with retries for lock failed exception.
    *
    * @param scope           scope.
    * @param stream          stream name.
    * @param config          stream configuration.
    * @param createTimestamp creation timestamp.
    * @param numOfRetries    number of retries for LockFailedException
     * @param requestId       requestId
    * @return CompletableFuture which when completed will have creation status for the stream.
    */
    public CompletableFuture<CreateStreamStatus.Status> createStreamRetryOnLockFailure(String scope, String stream, StreamConfiguration config,
                                                                                       long createTimestamp, int numOfRetries,
                                                                                       long requestId) {
        return RetryHelper.withRetriesAsync(() ->  createStream(scope, stream, config, createTimestamp, requestId),
                e -> Exceptions.unwrap(e) instanceof LockFailedException, numOfRetries, executor)
                .exceptionally(e -> {
                    log.warn(requestId, "createStream threw exception {}", e.getCause().getMessage());
                    Throwable unwrap = Exceptions.unwrap(e);
                    if (unwrap instanceof RetriesExhaustedException) {
                        throw new CompletionException(unwrap.getCause());
                    } else {
                        throw new CompletionException(unwrap);
                    }
                });
    }

    /**
     * Get Reader Group Configuration.
     *
     * @param scope  Reader Group scope.
     * @param rgName Reader Group name.
     * @param requestId requestid.
     * @return creation status.
     */
    public CompletableFuture<ReaderGroupConfigResponse> getReaderGroupConfig(final String scope, final String rgName,
                                                                             long requestId) {
        OperationContext context = streamMetadataStore.createRGContext(scope, rgName, requestId);
        return RetryHelper.withRetriesAsync(() -> {
          // 1. check if RG with this name exists...
          return streamMetadataStore.checkReaderGroupExists(scope, rgName, context, executor)
             .thenCompose(exists -> {
               if (!exists) {
                  return CompletableFuture.completedFuture(ReaderGroupConfigResponse.newBuilder()
                                    .setConfig(ReaderGroupConfiguration.getDefaultInstance())
                                    .setStatus(ReaderGroupConfigResponse.Status.RG_NOT_FOUND).build());
               }
               return streamMetadataStore.getReaderGroupId(scope, rgName, context, executor)
                    .thenCompose(rgId -> streamMetadataStore.getReaderGroupConfigRecord(scope, rgName, context, executor)
                            .thenApply(configRecord -> ReaderGroupConfigResponse.newBuilder()
                                .setConfig(getRGConfigurationFromRecord(scope, rgName, configRecord.getObject(), rgId.toString()))
                                .setStatus(ReaderGroupConfigResponse.Status.SUCCESS).build()));
       });
      }, e -> Exceptions.unwrap(e) instanceof RetryableException, READER_GROUP_OPERATION_MAX_RETRIES, executor);
    }

    private ReaderGroupConfiguration getRGConfigurationFromRecord(final String scope, final String rgName,
                                                                  final ReaderGroupConfigRecord record, final String readerGroupId) {
        List<StreamCut> startStreamCuts = record.getStartingStreamCuts().entrySet().stream()
                .map(e -> StreamCut.newBuilder()
                        .setStreamInfo(StreamInfo.newBuilder().setStream(Stream.of(e.getKey()).getStreamName())
                                .setScope(Stream.of(e.getKey()).getScope()).build())
                        .putAllCut(e.getValue().getStreamCut()).build()).collect(Collectors.toList());
        List<StreamCut> endStreamCuts = record.getEndingStreamCuts().entrySet().stream()
                .map(e -> StreamCut.newBuilder()
                        .setStreamInfo(StreamInfo.newBuilder().setStream(Stream.of(e.getKey()).getStreamName())
                                                              .setScope(Stream.of(e.getKey()).getScope()).build())
                        .putAllCut(e.getValue().getStreamCut()).build()).collect(Collectors.toList());
        return ReaderGroupConfiguration.newBuilder().setScope(scope).setReaderGroupName(rgName)
          .setGroupRefreshTimeMillis(record.getGroupRefreshTimeMillis())
          .setAutomaticCheckpointIntervalMillis(record.getAutomaticCheckpointIntervalMillis())
          .setMaxOutstandingCheckpointRequest(record.getMaxOutstandingCheckpointRequest())
          .setRetentionType(record.getRetentionTypeOrdinal())
          .setGeneration(record.getGeneration())
          .addAllStartingStreamCuts(startStreamCuts)
          .addAllEndingStreamCuts(endStreamCuts)
          .setReaderGroupId(readerGroupId)
          .build();
    }

    /**
     * Create Reader Group.
     *
     * @param scope           Reader Group scope.
     * @param rgName          Reader Group name.
     * @param config          Reader Group config.
     * @param createTimestamp Reader Group creation timestamp.
     * @param requestId       requestId
     * @return creation status.
     */
    public CompletableFuture<CreateReaderGroupResponse> createReaderGroup(final String scope, final String rgName,
                                                                          final ReaderGroupConfig config, long createTimestamp,
                                                                          final long requestId) {
    OperationContext context = streamMetadataStore.createRGContext(scope, rgName, requestId);
    return RetryHelper.withRetriesAsync(() -> {
      // 1. check if scope with this name exists...
      return streamMetadataStore.checkScopeExists(scope, context, executor)
         .thenCompose(exists -> {
         if (!exists) {
                  return CompletableFuture.completedFuture(Controller.CreateReaderGroupResponse.newBuilder()
                                  .setStatus(CreateReaderGroupResponse.Status.SCOPE_NOT_FOUND).build());
         }
         //2. check state of the ReaderGroup, if found
         return isRGCreationComplete(scope, rgName, context)
                 .thenCompose(complete -> {
                     if (!complete) {
                         //3. Create Reader Group Metadata inside Scope and submit the event
                         return validateReaderGroupId(config)
                                 .thenCompose(conf -> eventHelperFuture.thenCompose(eventHelper ->
                                         eventHelper.addIndexAndSubmitTask(buildCreateRGEvent(scope, rgName, conf,
                                                 requestId, createTimestamp),
                                 () -> streamMetadataStore.addReaderGroupToScope(scope, rgName, conf.getReaderGroupId(),
                                         context, executor))
                                         .thenCompose(x -> eventHelper.checkDone(() -> isRGCreated(scope, rgName, context))
                                         .thenCompose(done -> buildCreateSuccessResponse(scope, rgName, context)))));
                     }
                     log.info(requestId, "Reader Group {} already exists", NameUtils.getScopedReaderGroupName(scope, rgName));
                     return buildCreateSuccessResponse(scope, rgName, context);
                 });
         });
        }, e -> Exceptions.unwrap(e) instanceof RetryableException, READER_GROUP_OPERATION_MAX_RETRIES, executor);
    }

    private CompletableFuture<ReaderGroupConfig> validateReaderGroupId(ReaderGroupConfig config) {
        if (ReaderGroupConfig.DEFAULT_UUID.equals(config.getReaderGroupId())) {
            return CompletableFuture.completedFuture(ReaderGroupConfig.cloneConfig(config, UUID.randomUUID(), 0L));
        }
        return CompletableFuture.completedFuture(config);
    }

    public CompletableFuture<Boolean> isRGCreationComplete(String scope, String rgName, OperationContext context) {
        return Futures.exceptionallyExpecting(streamMetadataStore
                        .getReaderGroupState(scope, rgName, true, context, executor),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, ReaderGroupState.UNKNOWN)
                      .thenCompose(state -> {
                          if (state.equals(ReaderGroupState.UNKNOWN) || state.equals(ReaderGroupState.CREATING)) {
                              return CompletableFuture.completedFuture(Boolean.FALSE);
                          }
                          return CompletableFuture.completedFuture(Boolean.TRUE);
                      });
    }

    private CompletableFuture<CreateReaderGroupResponse> buildCreateSuccessResponse(final String scope, final String rgName,
                                                                                    OperationContext context) {
        return streamMetadataStore.getReaderGroupId(scope, rgName, context, executor)
                .thenCompose(rgId -> streamMetadataStore.getReaderGroupConfigRecord(scope, rgName, context, executor)
                        .thenApply(cfgRecord -> Controller.CreateReaderGroupResponse.newBuilder()
                                .setConfig(io.pravega.controller.server.rest.ModelHelper.encodeReaderGroupConfigRecord(scope, rgName, cfgRecord.getObject(), rgId))
                                .setStatus(CreateReaderGroupResponse.Status.SUCCESS)
                                .build()));
    }

    private CreateReaderGroupEvent buildCreateRGEvent(String scope, String rgName, ReaderGroupConfig config,
                                                      final long requestId, final long createTimestamp) {
        Map<String, RGStreamCutRecord> startStreamCuts = config.getStartingStreamCuts().entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey().getScopedName(),
                        e -> new RGStreamCutRecord(ImmutableMap.copyOf(ModelHelper.getStreamCutMap(e.getValue())))));
        Map<String, RGStreamCutRecord> endStreamCuts = config.getEndingStreamCuts().entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey().getScopedName(),
                        e -> new RGStreamCutRecord(ImmutableMap.copyOf(ModelHelper.getStreamCutMap(e.getValue())))));
        return new CreateReaderGroupEvent(requestId, scope, rgName, config.getGroupRefreshTimeMillis(),
                config.getAutomaticCheckpointIntervalMillis(), config.getMaxOutstandingCheckpointRequest(),
                config.getRetentionType().ordinal(), config.getGeneration(), config.getReaderGroupId(),
                startStreamCuts, endStreamCuts, createTimestamp);
    }

    public CompletableFuture<CreateReaderGroupResponse.Status> createReaderGroupTasks(final String scope, final String readerGroup,
                                                                                      final ReaderGroupConfig config,
                                                                                      final long createTimestamp,
                                                                                      final long requestId) {
        OperationContext context = streamMetadataStore.createRGContext(scope, readerGroup, requestId);
        return createReaderGroupTasks(scope, readerGroup, config, createTimestamp, context);
    }

    public CompletableFuture<CreateReaderGroupResponse.Status> createReaderGroupTasks(final String scope, final String readerGroup,
                                                                                      final ReaderGroupConfig config,
                                                                                      final long createTimestamp,
                                                                                      final OperationContext context) {
        Preconditions.checkNotNull(context, "operation context not null");
        return streamMetadataStore.createReaderGroup(scope, readerGroup, config, createTimestamp, context, executor)
                .thenCompose(v -> {
                    if (!ReaderGroupConfig.StreamDataRetention.NONE.equals(config.getRetentionType())) {
                        String scopedRGName = NameUtils.getScopedReaderGroupName(scope, readerGroup);
                        // update Stream metadata tables, only if RG is a Subscriber
                        Iterator<String> streamIter = config.getStartingStreamCuts().keySet().stream()
                                                            .map(Stream::getScopedName).iterator();
                        return Futures.loop(streamIter::hasNext, () -> {
                            Stream stream = Stream.of(streamIter.next());

                            return streamMetadataStore.addSubscriber(stream.getScope(),
                                    stream.getStreamName(), scopedRGName, config.getGeneration(), context, executor);
                        }, executor);
                    }
                    return CompletableFuture.completedFuture(null);
                }).thenCompose(x -> createRGStream(scope, NameUtils.getStreamForReaderGroup(readerGroup),
                StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1))
                        .rolloverSizeBytes(READER_GROUP_SEGMENT_ROLLOVER_SIZE_BYTES).build(),
                System.currentTimeMillis(), 10, getRequestId(context))
                .thenCompose(createStatus -> {
                    if (createStatus.equals(Controller.CreateStreamStatus.Status.STREAM_EXISTS)
                            || createStatus.equals(Controller.CreateStreamStatus.Status.SUCCESS)) {
                        return streamMetadataStore.getVersionedReaderGroupState(scope, readerGroup, true,
                                context, executor)
                                .thenCompose(newstate -> streamMetadataStore.updateReaderGroupVersionedState(scope, readerGroup,
                                        ReaderGroupState.ACTIVE, newstate, context, executor))
                                .thenApply(v1 -> CreateReaderGroupResponse.Status.SUCCESS);
                    }
                    return Futures.failedFuture(new IllegalStateException(
                            String.format("Error creating StateSynchronizer Stream for Reader Group %s: %s",
                            readerGroup, createStatus.toString())));
                })).exceptionally(ex -> {
            log.warn(getRequestId(context), "Error creating StateSynchronizer Stream:{} for Reader Group: {}. Exception: {} ",
                    NameUtils.getStreamForReaderGroup(readerGroup), readerGroup, ex.getMessage());
            Throwable cause = Exceptions.unwrap(ex);
            throw new CompletionException(cause);
        });
    }

    public CompletableFuture<CreateReaderGroupResponse> createReaderGroupInternal(final String scope, final String rgName,
                                                                                  final ReaderGroupConfig config,
                                                                                  final long createTimestamp,
                                                                                  long requestId) {
        Preconditions.checkNotNull(scope, "ReaderGroup scope is null");
        Preconditions.checkNotNull(rgName, "ReaderGroup name is null");
        Preconditions.checkNotNull(config, "ReaderGroup config is null");
        Preconditions.checkArgument(createTimestamp >= 0);
        try {
            NameUtils.validateReaderGroupName(rgName);
        } catch (IllegalArgumentException | NullPointerException e) {
            return CompletableFuture.completedFuture(CreateReaderGroupResponse.newBuilder()
                    .setStatus(CreateReaderGroupResponse.Status.INVALID_RG_NAME).build());
        }
        OperationContext context = streamMetadataStore.createRGContext(scope, rgName, requestId);
        return RetryHelper.withRetriesAsync(() -> {
            // 1. check if scope with this name exists...
            return streamMetadataStore.checkScopeExists(scope, context, executor)
                    .thenCompose(exists -> {
                        if (!exists) {
                            return CompletableFuture.completedFuture(CreateReaderGroupResponse.newBuilder()
                                    .setStatus(CreateReaderGroupResponse.Status.SCOPE_NOT_FOUND).build());
                        }
                        //2. check state of the ReaderGroup
                        return isRGCreationComplete(scope, rgName, context)
                                .thenCompose(complete -> {
                                    if (!complete) {
                                        return validateReaderGroupId(config)
                                        .thenCompose(conf -> streamMetadataStore.addReaderGroupToScope(scope, rgName,
                                                conf.getReaderGroupId(),
                                                context, executor)
                                                .thenCompose(x -> createReaderGroupTasks(scope, rgName, conf, createTimestamp,
                                                        context))
                                                .thenCompose(status -> {
                                                    if (CreateReaderGroupResponse.Status.SUCCESS.equals(status)) {
                                                        return buildCreateSuccessResponse(scope, rgName, context);
                                                    } else {
                                                        return CompletableFuture.completedFuture(
                                                                CreateReaderGroupResponse.newBuilder()
                                                                .setStatus(status).build());
                                                    }
                                                }));
                                    }
                                    return buildCreateSuccessResponse(scope, rgName, context);
                                });
                    });
        }, e -> Exceptions.unwrap(e) instanceof RetryableException, 10, executor);
    }

    private CompletableFuture<Boolean> isRGCreated(String scope, String rgName, OperationContext context) {
        return Futures.exceptionallyExpecting(streamMetadataStore.getReaderGroupState(scope, rgName, true,
                context, executor),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, ReaderGroupState.UNKNOWN)
                .thenApply(state -> {
                    log.debug(context.getRequestId(), "ReaderGroup State is {}", state.toString());
                    return ReaderGroupState.ACTIVE.equals(state);
                });
    }

    /**
     * Update Reader Group Configuration.
     *
     * @param scope           Reader Group scope.
     * @param rgName          Reader Group name.
     * @param config          New Reader Group config.
     * @param requestId       request id.
     * @return updation status.
     */
    public CompletableFuture<UpdateReaderGroupResponse> updateReaderGroup(final String scope, final String rgName,
                                                                          final ReaderGroupConfig config, long requestId) {
        final OperationContext context = streamMetadataStore.createRGContext(scope, rgName, requestId);
        return RetryHelper.withRetriesAsync(() -> {
            // 1. check if Reader Group exists...
            return streamMetadataStore.checkReaderGroupExists(scope, rgName, context, executor)
               .thenCompose(exists -> {
               if (!exists) {
                   UpdateReaderGroupResponse response = UpdateReaderGroupResponse.newBuilder()
                           .setStatus(UpdateReaderGroupResponse.Status.RG_NOT_FOUND)
                           .setGeneration(config.getGeneration()).build();
                  return CompletableFuture.completedFuture(response);
               }
               //2. check for generation && ID match with existing config
               return streamMetadataStore.getReaderGroupConfigRecord(scope, rgName, context, executor)
                      .thenCompose(rgConfigRecord -> {
                        if (rgConfigRecord.getObject().getGeneration() != config.getGeneration()) {
                            UpdateReaderGroupResponse response = UpdateReaderGroupResponse.newBuilder()
                                    .setStatus(UpdateReaderGroupResponse.Status.INVALID_CONFIG)
                                    .setGeneration(config.getGeneration()).build();
                            return CompletableFuture.completedFuture(response);
                        }
                        if (!rgConfigRecord.getObject().isUpdating()) {
                          return streamMetadataStore.getReaderGroupId(scope, rgName, context, executor)
                             .thenCompose(rgId -> {
                             if (!config.getReaderGroupId().equals(rgId)) {
                                 UpdateReaderGroupResponse response = UpdateReaderGroupResponse.newBuilder()
                                         .setStatus(UpdateReaderGroupResponse.Status.INVALID_CONFIG)
                                         .setGeneration(config.getGeneration()).build();
                                 return CompletableFuture.completedFuture(response);
                             }
                             ImmutableSet<String> removeStreams = getStreamsToBeUnsubscribed(rgConfigRecord.getObject(),
                                     config);
                             boolean isTransition = isTransitionToOrFromSubscriber(rgConfigRecord.getObject(), config);
                             UpdateReaderGroupEvent event = new UpdateReaderGroupEvent(scope, rgName, requestId, rgId,
                                                                 rgConfigRecord.getObject().getGeneration() + 1,
                                     isTransition, removeStreams);
                             //3. Create Reader Group Metadata and submit event
                             return eventHelperFuture.thenCompose(eventHelper -> eventHelper.addIndexAndSubmitTask(event,
                                    () -> streamMetadataStore.startRGConfigUpdate(scope, rgName, config, context, executor))
                                          .thenCompose(x -> eventHelper.checkDone(() -> isRGUpdated(scope, rgName, executor,
                                                  context))
                                          .thenCompose(y -> streamMetadataStore.getReaderGroupConfigRecord(scope, rgName,
                                                  context, executor)
                                          .thenApply(configRecord -> {
                                              UpdateReaderGroupResponse response = UpdateReaderGroupResponse.newBuilder()
                                                      .setStatus(UpdateReaderGroupResponse.Status.SUCCESS)
                                                      .setGeneration(configRecord.getObject().getGeneration()).build();
                                              return response;
                                          }))));
                             });
                        } else {
                            log.error(requestId, "Reader group config update failed as another update was in progress.");
                            UpdateReaderGroupResponse response = UpdateReaderGroupResponse.newBuilder()
                                    .setStatus(UpdateReaderGroupResponse.Status.FAILURE)
                                    .setGeneration(config.getGeneration()).build();
                          return CompletableFuture.completedFuture(response);
                        }
                      });
               });
        }, e -> Exceptions.unwrap(e) instanceof RetryableException, READER_GROUP_OPERATION_MAX_RETRIES, executor);
    }

    private boolean isTransitionToOrFromSubscriber(final ReaderGroupConfigRecord currentConfig,
                                                   final ReaderGroupConfig newConfig) {
        if (ReaderGroupConfig.StreamDataRetention.NONE
                .equals(ReaderGroupConfig.StreamDataRetention.values()[currentConfig.getRetentionTypeOrdinal()])
                && ReaderGroupConfig.StreamDataRetention.NONE.equals(newConfig.getRetentionType())) {
            // if both existing and new configs have StreamDataRetention.NONE
            // we're not transitioning this ReaderGroup from Subscriber to non-Subscriber or vice versa
            return false;
        }
        return true;
    }

    private ImmutableSet<String> getStreamsToBeUnsubscribed(final ReaderGroupConfigRecord currentConfig,
                                                            final ReaderGroupConfig newConfig) {
        if (isNonSubscriberToSubscriberTransition(currentConfig, newConfig)) {
            // changing from a non-subscriber to subscriber reader group
            // so we just need to add RG as subscriber to Streams in the new config
            return ImmutableSet.of();
        } else if (isSubscriberToNonSubscriberTransition(currentConfig, newConfig)) {
            // changing from subscriber to non-subscriber
            // unsubscribe from all streams from current config
            ImmutableSet.Builder<String> streamsToBeUnsubscribedBuilder = ImmutableSet.builder();
            currentConfig.getStartingStreamCuts().keySet().forEach(streamsToBeUnsubscribedBuilder::add);
            return streamsToBeUnsubscribedBuilder.build();
        } else {
            final Set<String> currentConfigStreams = currentConfig.getStartingStreamCuts().keySet();
            final Set<String> newConfigStreams = newConfig.getStartingStreamCuts().keySet().stream()
                                                          .map(Stream::getScopedName).collect(Collectors.toSet());
            ImmutableSet.Builder<String> setBuilder = ImmutableSet.builder();
            currentConfigStreams.stream()
                    .filter(s -> !newConfigStreams.contains(s)).forEach(setBuilder::add);
            return setBuilder.build();
        }

    }

    private boolean isNonSubscriberToSubscriberTransition(final ReaderGroupConfigRecord currentConfig,
                                                          final ReaderGroupConfig newConfig) {
        if (ReaderGroupConfig.StreamDataRetention.NONE
                .equals(ReaderGroupConfig.StreamDataRetention.values()[currentConfig.getRetentionTypeOrdinal()])
                && (!ReaderGroupConfig.StreamDataRetention.NONE.equals(newConfig.getRetentionType()))) {
            return true;
        }
        return false;
    }

    private boolean isSubscriberToNonSubscriberTransition(final ReaderGroupConfigRecord currentConfig,
                                                          final ReaderGroupConfig newConfig) {
        if (!ReaderGroupConfig.StreamDataRetention.NONE
                .equals(ReaderGroupConfig.StreamDataRetention.values()[currentConfig.getRetentionTypeOrdinal()])
                && (ReaderGroupConfig.StreamDataRetention.NONE.equals(newConfig.getRetentionType()))) {
            return true;
        }
        return false;
    }

    private CompletableFuture<Boolean> isRGUpdated(String scope, String rgName, Executor executor, OperationContext context) {
            return streamMetadataStore.getReaderGroupConfigRecord(scope, rgName, context, executor)
                    .thenCompose(rgConfigRecord -> {
                        log.debug(context.getRequestId(), "Is ReaderGroup Config update complete ? {}",
                                rgConfigRecord.getObject().isUpdating());
                        return CompletableFuture.completedFuture(!rgConfigRecord.getObject().isUpdating());
                   });
    }

    /**
     * Delete Reader Group.
     *
     * @param scope           Reader Group scope.
     * @param rgName          Reader Group name.
     * @param readerGroupId   Reader Group unique identifier.
     * @param requestId       request id.
     * @return deletion status.
     */
    public CompletableFuture<DeleteReaderGroupStatus.Status> deleteReaderGroup(final String scope, final String rgName,
                                                                               final String readerGroupId,
                                                                               final long requestId) {
        final OperationContext context = streamMetadataStore.createRGContext(scope, rgName, requestId);
        return RetryHelper.withRetriesAsync(() ->
                streamMetadataStore.checkReaderGroupExists(scope, rgName, context, executor)
                   .thenCompose(exists -> {
                    if (!exists) {
                       return CompletableFuture.completedFuture(DeleteReaderGroupStatus.Status.RG_NOT_FOUND);
                    }
                    return streamMetadataStore.getReaderGroupId(scope, rgName, context, executor)
                           .thenCompose(rgId -> {
                               if (!rgId.equals(UUID.fromString(readerGroupId))) {
                                   return CompletableFuture.completedFuture(DeleteReaderGroupStatus.Status.RG_NOT_FOUND);
                               }
                               return eventHelperFuture.thenCompose(eventHelper -> streamMetadataStore.getReaderGroupConfigRecord(
                                       scope, rgName, context, executor)
                                       .thenCompose(configRecord -> streamMetadataStore.getVersionedReaderGroupState(
                                               scope, rgName, true, context, executor)
                                               .thenCompose(versionedState -> eventHelper.addIndexAndSubmitTask(
                                                       new DeleteReaderGroupEvent(scope, rgName, requestId, rgId),
                                                       () -> startReaderGroupDelete(scope, rgName, versionedState, context))
                                                       .thenCompose(x -> eventHelper.checkDone(() -> isRGDeleted(scope,
                                                               rgName, context))
                                                               .thenApply(done -> DeleteReaderGroupStatus.Status.SUCCESS)))));
                           });
                }), e -> Exceptions.unwrap(e) instanceof RetryableException, READER_GROUP_OPERATION_MAX_RETRIES, executor);
    }

    private CompletableFuture<Boolean> isRGDeleted(String scope, String rgName, OperationContext context) {
        return streamMetadataStore.checkReaderGroupExists(scope, rgName, context, executor).thenApply(exists -> !exists);
    }

    private CompletableFuture<Void> startReaderGroupDelete(final String scope, final String rgName, VersionedMetadata<ReaderGroupState> currentState,
                                                           OperationContext context) {
        return Futures.toVoid(streamMetadataStore.updateReaderGroupVersionedState(scope, rgName,
           ReaderGroupState.DELETING, currentState, context, executor));
    }

    /**
     * Delete Scope Recursively.
     *
     * @param scope           scope to be deleted.
     * @param requestId       request id.
     * @return deletion status.
     */
    public CompletableFuture<DeleteScopeStatus.Status> deleteScopeRecursive(final String scope,
                                                                              final long requestId) {
        final OperationContext context = streamMetadataStore.createScopeContext(scope, requestId);
        return RetryHelper.withRetriesAsync(() -> streamMetadataStore.isScopeSealed(scope, context, executor)
                .thenCompose(sealed -> {
                    if (sealed) {
                        log.debug(requestId, "Another Scope deletion API call for {} is already in progress", scope);
                        return CompletableFuture.completedFuture(DeleteScopeStatus.Status.SUCCESS);
                    } else {
                        return streamMetadataStore.checkScopeExists(scope, context, executor).thenCompose(scopeExist -> {
                            if (!scopeExist) {
                                log.info(requestId, "Requested Scope {} doesn't exist", scope);
                                return CompletableFuture.completedFuture(DeleteScopeStatus.Status.SUCCESS);
                            }
                            return streamMetadataStore.getScopeId(scope, context, executor).thenCompose(scopeId -> {
                                DeleteScopeEvent deleteEvent = new DeleteScopeEvent(scope, requestId, scopeId);
                                return eventHelper.addIndexAndSubmitTask(deleteEvent,
                                                () -> streamMetadataStore.sealScope(scope, context, executor))
                                        .thenCompose(x -> eventHelper.checkDone(() -> isScopeDeletionComplete(scope, context)))
                                        .thenApply(y -> DeleteScopeStatus.Status.SUCCESS);
                            });
                        });
                    }
                }), e -> Exceptions.unwrap(e) instanceof RetryableException, SCOPE_DELETION_MAX_RETRIES, executor
        ).exceptionally(ex -> handleDeleteScopeError(ex, requestId, scope));
    }

    /**
     * Create stream.
     *
     * @param scope           scope.
     * @param stream          stream name.
     * @param config          stream configuration.
     * @param createTimestamp creation timestamp.
     * @param requestId       requestId.
     * @return creation status.
     */
    @Task(name = "createStream", version = "1.0", resource = "{scope}/{stream}")
    public CompletableFuture<CreateStreamStatus.Status> createStream(String scope, String stream, StreamConfiguration config,
                                                                     long createTimestamp, long requestId) {
        log.debug(requestId, "createStream with resource called.");
        OperationContext context = streamMetadataStore.createStreamContext(scope, stream, requestId);

            return execute(
                    new Resource(scope, stream),
                    new Serializable[]{scope, stream, config, createTimestamp, requestId},
                    () -> createStreamBody(scope, stream, config, createTimestamp, context));
    }

    /**
     * Update stream's configuration.
     *
     * @param scope      scope.
     * @param stream     stream name.
     * @param newConfig  modified stream configuration.
     * @param requestId  requestId
     * @return update status.
     */
    public CompletableFuture<UpdateStreamStatus.Status> updateStream(String scope, String stream, StreamConfiguration newConfig,
                                                                     long requestId) {
        final OperationContext context = streamMetadataStore.createStreamContext(scope, stream, requestId);

        return streamMetadataStore.getState(scope, stream, true, context, executor)
                .thenCompose(state -> {
                    if (state.equals(State.SEALED)) {
                        log.error(requestId, "Cannot update a sealed stream {}/{}", scope, stream);
                        return CompletableFuture.completedFuture(UpdateStreamStatus.Status.STREAM_SEALED);
                    }
                    // 1. get configuration
                    return streamMetadataStore.getConfigurationRecord(scope, stream, context, executor)
                            .thenCompose(configProperty -> {
                                // 2. post event to start update workflow
                                if (!configProperty.getObject().isUpdating()) {
                                    return eventHelperFuture.thenCompose(eventHelper -> eventHelper.addIndexAndSubmitTask(
                                                    new UpdateStreamEvent(scope, stream, requestId),
                                                    // 3. update new configuration in the store with updating flag = true
                                                    // if attempt to update fails, we bail out with no harm done
                                                    () -> streamMetadataStore.startUpdateConfiguration(scope, stream, newConfig,
                                                            context, executor))
                                            // 4. wait for update to complete
                                            .thenCompose(y -> eventHelper.checkDone(() -> isUpdated(scope, stream, newConfig, context))
                                                    .thenApply(z -> UpdateStreamStatus.Status.SUCCESS)));
                                } else {
                                    log.error(requestId, "Another update in progress for {}/{}",
                                            scope, stream);
                                    return CompletableFuture.completedFuture(UpdateStreamStatus.Status.FAILURE);
                                }
                            });
                })
                .exceptionally(ex -> {
                    final String message = "Exception updating stream configuration {}";
                    return handleUpdateStreamError(ex, requestId, message, NameUtils.getScopedStreamName(scope, stream));
                });
    }

    @VisibleForTesting
    CompletableFuture<Boolean> isUpdated(String scope, String stream, StreamConfiguration newConfig, OperationContext context) {
        CompletableFuture<State> stateFuture = streamMetadataStore.getState(scope, stream, true, context, executor);
        CompletableFuture<StreamConfigurationRecord> configPropertyFuture
                = streamMetadataStore.getConfigurationRecord(scope, stream, context, executor)
                                     .thenApply(VersionedMetadata::getObject);
        return CompletableFuture.allOf(stateFuture, configPropertyFuture)
                                .thenApply(v -> {
                                    State state = stateFuture.join();
                                    StreamConfigurationRecord configProperty = configPropertyFuture.join();
                                    // if property is updating and doesn't match our request, it's a subsequent update
                                    if (configProperty.isUpdating()) {
                                        return !configProperty.getStreamConfiguration().equals(newConfig);
                                    } else {
                                        // if stream is sealed then update should not be allowed
                                        if (state.equals(State.SEALED)) {
                                            log.error("Cannot update a sealed stream {}/{}", scope, stream);
                                            throw new UnsupportedOperationException("Cannot update a sealed stream: " + NameUtils.getScopedStreamName(scope, stream));
                                        }
                                        // if update-barrier is not updating, then update is complete if property matches our expectation
                                        // and state is not updating
                                        return !(configProperty.getStreamConfiguration().equals(newConfig) &&
                                                state.equals(State.UPDATING));
                                    }
                                });
    }

    /**
     * Get list of subscribers for a Stream.
     * Subscribers are ReaderGroups reading from a Stream such that their reads impact data retention in the Stream.
     * @param scope      scope.
     * @param stream     stream name.
     * @param requestId  request id
     * @return update status.
     */
    public CompletableFuture<SubscribersResponse> listSubscribers(String scope, String stream, long requestId) {
        final OperationContext context = streamMetadataStore.createStreamContext(scope, stream, requestId);
        return streamMetadataStore.checkStreamExists(scope, stream, context, executor)
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
                                log.error(requestId, "Exception trying to get list of subscribers for Stream {}. Cause {}",
                                        NameUtils.getScopedStreamName(scope, stream), ex);
                                Throwable cause = Exceptions.unwrap(ex);
                                if (cause instanceof TimeoutException) {
                                    throw new CompletionException(cause);
                                } else {
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
     * @param readerGroupId  Unique Id (UUID) of Reader Group.
     * @param generation  Generation of Subscriber publishing the StreamCut.
     * @param truncationStreamCut  Truncation StreamCut.
     * @param requestId  request id
     * @return update status.
     */
    public CompletableFuture<UpdateSubscriberStatus.Status> updateSubscriberStreamCut(String scope, String stream,
                                                                             String subscriber, String readerGroupId, long generation, ImmutableMap<Long, Long> truncationStreamCut,
                                                                             long requestId) {
        final OperationContext context = streamMetadataStore.createStreamContext(scope, stream, requestId);

        return RetryHelper.withRetriesAsync(() -> streamMetadataStore.checkStreamExists(scope, stream, context, executor)
                .thenCompose(exists -> {
                // 1. Check Stream exists.
                if (!exists) {
                   return CompletableFuture.completedFuture(UpdateSubscriberStatus.Status.STREAM_NOT_FOUND);
                }
                return Futures.exceptionallyExpecting(streamMetadataStore.getSubscriber(scope, stream, subscriber, context, executor),
                       e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, null)
                       .thenCompose(subscriberRecord -> {
                       // 2. Check Subscriber exists in Stream metadata.
                       if (subscriberRecord == null) {
                           return CompletableFuture.completedFuture(UpdateSubscriberStatus.Status.SUBSCRIBER_NOT_FOUND);
                       }
                       final List<String> subscriberScopedName = NameUtils.extractScopedNameTokens(subscriber);
                       return streamMetadataStore.getReaderGroupId(subscriberScopedName.get(0),
                               subscriberScopedName.get(1), context, executor)
                               .thenCompose(rgId -> {
                                 // 3. Check if subscriber Id matches
                                 if (!rgId.equals(UUID.fromString(readerGroupId))) {
                                       return CompletableFuture.completedFuture(UpdateSubscriberStatus.Status.SUBSCRIBER_NOT_FOUND);
                                 }
                                 if (subscriberRecord.getObject().getGeneration() != generation) {
                                       return CompletableFuture.completedFuture(UpdateSubscriberStatus.Status.GENERATION_MISMATCH);
                                 }
                                 // 4. Update streamcut
                                 return streamMetadataStore.updateSubscriberStreamCut(scope, stream, subscriber,
                                         generation, truncationStreamCut, subscriberRecord, context, executor)
                                          .thenApply(x -> UpdateSubscriberStatus.Status.SUCCESS)
                                          .exceptionally(ex -> {
                                              log.error(requestId,
                                                      "Exception updating StreamCut for Subscriber {} on Stream {}. Cause:{}",
                                                      subscriber, NameUtils.getScopedStreamName(scope, stream), ex);
                                              Throwable cause = Exceptions.unwrap(ex);
                                              if (cause instanceof StoreException.OperationNotAllowedException) {
                                                    return UpdateSubscriberStatus.Status.STREAM_CUT_NOT_VALID;
                                              } else if (cause instanceof TimeoutException) {
                                                   throw new CompletionException(cause);
                                              } else {
                                                   return UpdateSubscriberStatus.Status.FAILURE;
                                              }
                                          });
                                   });
                        });
        }), e -> Exceptions.unwrap(e) instanceof RetryableException, SUBSCRIBER_OPERATION_RETRIES, executor);
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
                                             final long recordingTime, final OperationContext contextOpt,
                                             final String delegationToken) {
        Preconditions.checkNotNull(policy);
        final OperationContext context = contextOpt != null ? contextOpt :
                streamMetadataStore.createStreamContext(scope, stream, ControllerService.nextRequestId());

        return streamMetadataStore.getRetentionSet(scope, stream, context, executor)
                .thenCompose(retentionSet -> {
                    StreamCutReferenceRecord latestCut = retentionSet.getLatest();

                    return generateStreamCutIfRequired(scope, stream, latestCut, recordingTime, context, delegationToken)
                            .thenCompose(newRecord -> truncate(scope, stream, policy, context, retentionSet, newRecord));
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
                          .thenCompose(previousRecord -> generateStreamCut(scope, stream, previousRecord, context,
                                  delegationToken)
                                  .thenCompose(newRecord ->
                                          streamMetadataStore.addStreamCutToRetentionSet(scope, stream, newRecord,
                                                  context, executor)
                                                 .thenApply(x -> {
                                                     log.debug(context.getRequestId(),
                                                             "New streamCut generated for stream {}/{}",
                                                             scope, stream);
                                                     return newRecord;
                                                 })));
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    private CompletableFuture<Void> truncate(String scope, String stream, RetentionPolicy policy, OperationContext context,
                                             RetentionSet retentionSet, StreamCutRecord newRecord) {
        RetentionSet updatedRetentionSet = newRecord == null ? retentionSet :
                RetentionSet.addReferenceToStreamCutIfLatest(retentionSet, newRecord);
        return truncateInternal(scope, stream, context, policy, updatedRetentionSet);
    }

    private CompletableFuture<Void> truncateInternal(String scope, String stream, OperationContext context,
                                                     RetentionPolicy policy, RetentionSet retentionSet) {
        long requestId = context.getRequestId();
        return streamMetadataStore.listSubscribers(scope, stream, context, executor)
                           .thenCompose(list -> Futures.allOfWithResults(list.stream().map(x ->
                                   streamMetadataStore.getSubscriber(scope, stream, x, context, executor))
                                                                             .collect(Collectors.toList())))
                           .thenCompose(subscribers -> {
                               // convert all streamcuts to include the segment range
                               return Futures.allOfWithResults(subscribers.stream().map(x -> {
                                   ImmutableSet<Map.Entry<Long, Long>> entries = x.getObject().getTruncationStreamCut().entrySet();

                                   return Futures.keysAllOfWithResults(entries.stream().collect(Collectors.toMap(
                                           y -> streamMetadataStore.getSegment(scope, stream, y.getKey(), context, executor),
                                           Map.Entry::getValue)));
                               }).collect(Collectors.toList()));
                           })
                           .thenApply(this::computeSubscribersLowerBound)
                .thenCompose(lowerBound -> {
                    CompletableFuture<Map<Long, Long>> toTruncateAt;
                    if (policy.getRetentionType().equals(RetentionPolicy.RetentionType.SIZE)) {
                        toTruncateAt = getTruncationStreamCutBySizeLimit(scope, stream, context, policy, retentionSet, lowerBound);
                    } else {
                        toTruncateAt = getTruncationStreamCutByTimeLimit(scope, stream, context, policy, retentionSet, lowerBound);
                    }
                    return toTruncateAt.thenCompose(truncationStreamCut -> {
                        if (truncationStreamCut == null || truncationStreamCut.isEmpty()) {
                            log.debug(context.getRequestId(),
                                    "no truncation record could be compute that satisfied retention policy");
                            return CompletableFuture.completedFuture(null);
                        }
                        return startTruncation(scope, stream, truncationStreamCut, context)
                                .thenCompose(started -> {
                                    if (started) {
                                        return streamMetadataStore.findStreamCutReferenceRecordBefore(
                                                scope, stream, truncationStreamCut, retentionSet, context, executor)
                                                      .thenCompose(ref -> {
                                                          if (ref != null) {
                                                              return streamMetadataStore.deleteStreamCutBefore(
                                                                      scope, stream, ref, context, executor);
                                                          } else {
                                                              return CompletableFuture.completedFuture(null);
                                                          }
                                                      });
                                    } else {
                                        throw new RuntimeException("Could not start truncation");
                                    }
                                }).exceptionally(e -> {
                                    if (Exceptions.unwrap(e) instanceof IllegalArgumentException) {
                                        // This is ignorable exception. Throwing this will cause unnecessary retries and
                                        // exceptions logged.
                                        log.debug(requestId,
                                                "Cannot truncate at given streamCut because it intersects with existing truncation point");
                                        return null;
                                    } else {
                                        throw new CompletionException(e);
                                    }
                                });
                    });
                });
    }

    private CompletableFuture<Map<Long, Long>> getTruncationStreamCutBySizeLimit(String scope, String stream,
                                                                                 OperationContext context,
                                                                                 RetentionPolicy policy,
                                                                                 RetentionSet retentionSet,
                                                                                 Map<Long, Long> lowerBound) {

        // 1. if lowerbound.size < max and lowerbound.size > min truncate at lowerbound
        // 2. if lowerbound.size < min, truncate at streamcut less than (behind/before) lowerbound that satisfies the policy. 
        // 3. if lowerbound.size > max, truncate at max
        long currentSize = retentionSet.getLatest().getRecordingSize();
        // we get the streamcuts from retentionset that satisfy the min and max bounds with min pointing to most recent 
        // streamcut to satisfy both min and max bounds while max refering to oldest such streamcut in retention set.  
        Map.Entry<StreamCutReferenceRecord, StreamCutReferenceRecord> limits = getBoundStreamCuts(policy, retentionSet,
                x -> currentSize - x.getRecordingSize());

        // if lowerbound is empty simply return min
        if (lowerBound == null || lowerBound.isEmpty()) {
            return Optional.ofNullable(limits.getValue())
                    .map(x -> streamMetadataStore.getStreamCutRecord(scope, stream, x, context, executor)
                                                 .thenApply(StreamCutRecord::getStreamCut))
                    .orElse(CompletableFuture.completedFuture(null));
        }

        return streamMetadataStore.getSizeTillStreamCut(scope, stream, lowerBound, Optional.empty(), context, executor)
                .thenCompose(sizeTillLB -> {
                    long retainedSizeLB = currentSize - sizeTillLB;
                    // if retainedSize is less than (behind/before) min size then we need to truncate at min or the most
                    // recent streamcut strictly less than (behind/before) lowerbound.
                    if (retainedSizeLB < policy.getRetentionParam()) {
                        // if no overlap with min then truncate at min
                        // else truncate at streamcut before lb
                        return Optional.ofNullable(limits.getValue()).map(x ->
                                streamMetadataStore.getStreamCutRecord(scope, stream, limits.getValue(), context, executor)
                                   .thenCompose(limitMin -> {
                                       return streamMetadataStore.compareStreamCut(scope, stream, limitMin.getStreamCut(),
                                               lowerBound, context, executor)
                                         .thenCompose(compareWithMin -> {
                                             switch (compareWithMin) {
                                                 case Before: // min less than (behind/before) lowerbound. truncate at min
                                                     return CompletableFuture.completedFuture(limitMin.getStreamCut());
                                                 default:
                                                     // we cannot have min greater (ahead of/after) than lowerbound as
                                                     // retainedSizeLB < min.
                                                     // so this is the overlapping case. we need to find streamcut before lowerbound
                                                     // and truncate at it.
                                                     // since this is a caught up bound (meaning truncating at subscribers
                                                     // LB will break min policy),
                                                     // we cannot force truncate at the max if max overlapped with LB.
                                                     // so we will find the streamcut from retention set lessThan (behind/before)
                                                     // LB (and since it overlaps with min 
                                                     // so it will definitely be lessThan (behind/before) min). 
                                                     return getStreamcutBeforeLowerbound(scope, stream, context, retentionSet,
                                                             lowerBound);
                                                 }
                                             });
                                   })).orElse(CompletableFuture.completedFuture(null));
                    } else {
                        // if retained size is less than (behind/before) max allowed, then truncate the stream
                        // at subscriber lower bound.
                        if (retainedSizeLB < policy.getRetentionMax()) {
                            return CompletableFuture.completedFuture(lowerBound);
                        } else { // greater (ahead of/after) than max. truncate at max. 
                            // let there be data loss. its a lagging reader.. but if there is no streamcut in 
                            // retentionset that satisfied the min and max criteria, then we should simply truncate at 
                            // least at the lowerbound 
                            return Optional.ofNullable(limits.getKey())
                                           .filter(x -> currentSize - x.getRecordingSize() < retainedSizeLB)
                                           .map(x -> streamMetadataStore.getStreamCutRecord(scope, stream, x, context, executor)
                                                                        .thenApply(StreamCutRecord::getStreamCut))
                                    .orElse(CompletableFuture.completedFuture(lowerBound));
                        }
                    }
                });
    }

    private CompletableFuture<Map<Long, Long>> getTruncationStreamCutByTimeLimit(String scope, String stream, OperationContext context,
                                                                                 RetentionPolicy policy, RetentionSet retentionSet,
                                                                                 Map<Long, Long> lowerBound) {
        long currentTime = retentionClock.get().get();

        // we get the streamcuts from retentionset that satisfy the min and max bounds with min pointing to most recent 
        // streamcut to satisfy both min and max bounds while max refering to oldest such streamcut in retention set.
        // limits.key will refer to max and limit.value will refer to min. 
        Map.Entry<StreamCutReferenceRecord, StreamCutReferenceRecord> limits = getBoundStreamCuts(policy, retentionSet, 
                x -> currentTime - x.getRecordingTime());
        // if subscriber lowerbound is greater than (ahead of/after) streamcut corresponding to the max time and is less than 
        // (behind/before) stream cut for min time  from the retention set then we can safely truncate at lowerbound. 
        // Else we will truncate at the max time bound if it exists
        // 1. if LB is greater than (ahead of/after) min => truncate at min
        // 2. if LB is less than (behind/before) max => truncate at max
        // 3. if LB is less than (behind/before) min && LB is greater than (ahead of/after) max => truncate at LB
        // 4. if LB is less than (behind/before) min && overlaps max => truncate at max
        // 5. if LB overlaps with min and max ==> so its got both recent data and older data. 
        //      we will truncate at a streamcut less than (behind/before) max in this case. 
  
        CompletableFuture<StreamCutRecord> limitMinFuture = limits.getValue() == null ? CompletableFuture.completedFuture(null) :
                streamMetadataStore.getStreamCutRecord(scope, stream, limits.getValue(), context, executor);

        // if lowerbound is empty simply return min
        if (lowerBound == null || lowerBound.isEmpty()) {
            return limitMinFuture.thenApply(min -> Optional.ofNullable(min).map(StreamCutRecord::getStreamCut).orElse(null));
        }
        Optional<StreamCutReferenceRecord> maxBoundRef = retentionSet
                .getRetentionRecords().stream()
                .filter(x -> currentTime - x.getRecordingTime() >= policy.getRetentionMax())
                .max(Comparator.comparingLong(StreamCutReferenceRecord::getRecordingTime));

        CompletableFuture<StreamCutRecord> limitMaxFuture = limits.getKey() == null ? CompletableFuture.completedFuture(null) :
                streamMetadataStore.getStreamCutRecord(scope, stream, limits.getKey(), context, executor);
        CompletableFuture<StreamCutRecord> maxBoundFuture = maxBoundRef.map(x -> streamMetadataStore.getStreamCutRecord(
                scope, stream, x, context, executor))
                                                                    .orElse(CompletableFuture.completedFuture(null));
        return CompletableFuture.allOf(limitMaxFuture, limitMinFuture, maxBoundFuture)
                .thenCompose(v -> {
                    StreamCutRecord limitMax = limitMaxFuture.join();
                    StreamCutRecord limitMin = limitMinFuture.join();
                    StreamCutRecord maxBound = maxBoundFuture.join();
                    if (limitMin != null) {
                        return streamMetadataStore.compareStreamCut(scope, stream, limitMin.getStreamCut(), lowerBound,
                                context, executor)
                                  .thenCompose(compareWithMin -> {
                                      switch (compareWithMin) {
                                          case EqualOrAfter:
                                              // if min is not null, truncate at lb or max.
                                              // if lb is conclusively greaterthan (ahead/after) streamcut outside of maxbound 
                                              // then we truncate at limitmax, else we truncate at lb.
                                              // if it overlaps with limitmax, then we truncate at maxbound
                                              return truncateAtLowerBoundOrMax(scope, stream, context, lowerBound,
                                                      limitMax, maxBound);
                                          case Overlaps:
                                              // min overlaps with lb. cannot truncate at min or lb. 
                                              // and we cannot force truncate at max either if it overlaps with lowerbound.
                                              // so we will choose a streamcut before lb, which will definitely be before
                                              // min as min overlaps with lb
                                              // and we are choosing from retention set. 
                                              return getStreamcutBeforeLowerbound(scope, stream, context, retentionSet,
                                                      lowerBound);
                                          case Before:
                                              // min is less than (behind/before) lb. truncate at min
                                              return CompletableFuture.completedFuture(limitMin.getStreamCut());
                                          default:
                                              throw new IllegalArgumentException("Invalid Compare streamcut response");
                                      }
                                  });
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    private CompletableFuture<Map<Long, Long>> truncateAtLowerBoundOrMax(String scope, String stream, OperationContext context, 
                                                                         Map<Long, Long> lowerBound, StreamCutRecord limitMax,
                                                                         StreamCutRecord maxBound) {
        // if maxbound == null, truncate at lowerbound. 
        // if lowerbound is greater than (ahead of/after) maxbound, truncate at lowerbound. 
        // if lowerbound is eq or overlapping with maxbound, it certainly has events from before the time of interest.
        // we will truncate at maxbound.
        if (maxBound == null) {
            return CompletableFuture.completedFuture(lowerBound);
        } else {
            return streamMetadataStore.compareStreamCut(scope, stream, lowerBound, maxBound.getStreamCut(), context, executor)
                                  .thenCompose(compareWithMax -> {
                                      switch (compareWithMax) {
                                          case EqualOrAfter:  // lowerbound greater than (ahead of/after) max..
                                              // truncate at lowerbound
                                              return CompletableFuture.completedFuture(lowerBound);
                                          case Before:  // lowerbound is strictly less than (behind/before) lb,
                                              // truncate at limitmax
                                              return CompletableFuture.completedFuture(limitMax.getStreamCut());
                                          default:  // lowerbound overlaps with maxbound, truncating at maxbound is safe. 
                                              // we definitely lose older data and retain possibly newer data from lowerbound. 
                                              return CompletableFuture.completedFuture(maxBound.getStreamCut());
                                          }
                                  });
        }
    }

    private CompletableFuture<Map<Long, Long>> getStreamcutBeforeLowerbound(String scope, String stream, OperationContext context, 
                                                                            RetentionSet retentionSet, Map<Long, Long> lowerBound) {
        return streamMetadataStore.findStreamCutReferenceRecordBefore(scope, stream,
                lowerBound, retentionSet, context, executor)
                                  .thenCompose(refRecord -> Optional.ofNullable(refRecord).map(ref ->
                                          streamMetadataStore.getStreamCutRecord(scope, stream,
                                          ref, context, executor).thenApply(StreamCutRecord::getStreamCut))
                                                                    .orElse(CompletableFuture.completedFuture(null)));
    }

    private Map<Long, Long> computeSubscribersLowerBound(List<Map<StreamSegmentRecord, Long>> subscribers) {
        // loop over all streamcuts and for each segment in new streamcut:
        // if new segment is predecessor of any segment in the bound then replace the successor segment with
        // this segment.
        Map<StreamSegmentRecord, Long> lowerBound = new HashMap<>();
        subscribers.forEach(streamCut -> streamCut.forEach((segment, offset) -> {
            if (lowerBound.containsKey(segment)) {
                if (lowerBound.get(segment) > offset) {
                    lowerBound.put(segment, offset);
                }
            } else {
                Map<StreamSegmentRecord, Long> predecessors = new HashMap<>();
                Map<StreamSegmentRecord, Long> successors = new HashMap<>();
                lowerBound.forEach((s, o) -> {
                    if (s.overlaps(segment)) {
                        if (s.segmentId() < segment.segmentId()) {
                            predecessors.put(s, o);
                        } else {
                            successors.put(s, o);
                        }
                    }
                });

                if (successors.isEmpty() && predecessors.isEmpty()) {
                    lowerBound.put(segment, offset);
                } else {
                    if (offset < 0) {
                        // 1. if it is a future segment and its predecessors dont cover entire range, add it to lowerbound
                        // union of all predecessors of segment < segment.range
                        TreeMap<Double, Double> range = new TreeMap<>();
                        predecessors.keySet().forEach(x -> {
                            range.put(x.getKeyStart(), x.getKeyEnd());
                        });
                        if (!checkCoverage(segment.getKeyStart(), segment.getKeyEnd(), range)) {
                            lowerBound.put(segment, offset);
                        }
                    } else if (predecessors.isEmpty()) {
                        // since its a non future segment, its predecessors cannot be present.
                        lowerBound.put(segment, offset);
                        // include this segment. then remove its affected successors from lower bound.
                        // 1. remove its non future segment successors
                        // 2. remove its future segment successors if their entire range is covered by inclusion of this segment.
                        successors.forEach((s, o) -> {
                            if (o >= 0) {
                                lowerBound.remove(s);
                            } else {
                                // union of all predecessors of `s` and `segment` >= s.range
                                TreeMap<Double, Double> range = new TreeMap<>();
                                lowerBound.keySet().forEach(x -> {
                                    if (x.overlaps(s) && x.segmentId() < s.segmentId()) {
                                        range.put(x.getKeyStart(), x.getKeyEnd());
                                    }
                                });
                                if (checkCoverage(s.getKeyStart(), s.getKeyEnd(), range)) {
                                    lowerBound.remove(s);
                                }
                            }
                        });
                    }
                }
            }
        }));
        // example::
        // | s0 | s3      |
        // |    | s4 |    | s6
        // | s1      | s5 |
        // | s2      |    |
        // valid stream cuts: { s0/off, s5/-1 }, { s0/off, s2/off, s5/-1 }
        // lower bound = { s0/off, s2/off, s5/-1 }
        // valid stream cuts: { s0/off, s5/-1 }, { s0/off, s2/off, s5/-1 }, { s0/off, s1/off, s2/off }
        // lower bound = { s0/off, s1/off, s2/off }
        return lowerBound.entrySet().stream().collect(Collectors.toMap(x -> x.getKey().segmentId(), Map.Entry::getValue));
    }

    private boolean checkCoverage(double keyStart, double keyEnd, TreeMap<Double, Double> range) {
        // range covered
        AtomicReference<Double> previous = new AtomicReference<>();
        AtomicReference<Double> toCheckStart = new AtomicReference<>(keyStart);
        AtomicBoolean covered = new AtomicBoolean(true);
        for (Map.Entry<Double, Double> entry : range.entrySet()) {
            if (toCheckStart.get() >= keyEnd) {
                covered.set(true);
                break;
            }
            if (previous.get() == null) {
                previous.set(entry.getValue());
                if (keyStart < entry.getKey()) {
                    covered.set(false);
                    break;
                } else {
                    // we have atleast covered till "end" of this range.
                    toCheckStart.set(entry.getValue());
                }
            } else {
                if (previous.get() < entry.getKey()) {
                    // if this gap is part of range we are looking for then this cannot be covered.
                    if (StreamSegmentRecord.overlaps(new AbstractMap.SimpleEntry<>(previous.get(), entry.getKey()),
                            new AbstractMap.SimpleEntry<>(toCheckStart.get(), keyEnd))) {
                        covered.set(false);
                        break;
                    }
                } else {
                    // move to check start to the end of the range.
                    toCheckStart.set(Math.max(previous.get(), entry.getValue()));
                }
            }
        }
        covered.compareAndSet(true, toCheckStart.get() >= keyEnd);
        return covered.get();
    }

    private Map.Entry<StreamCutReferenceRecord, StreamCutReferenceRecord> getBoundStreamCuts(
            RetentionPolicy policy, RetentionSet retentionSet, Function<StreamCutReferenceRecord, Long> delta) {
        AtomicReference<StreamCutReferenceRecord> max = new AtomicReference<>();
        AtomicReference<StreamCutReferenceRecord> min = new AtomicReference<>();

        // We loop through all the streamcuts in the retention set and find two streamcuts that satisfy min 
        // and max bounds in the policy. The policy can be either size or time based and the caller passes a delta function
        // that is applied on each stream cut which tells us the size/time worth of data that is retained if truncated at
        // a particular cut. 
        // Do note that if min is NOT satisfied by a streamcut then it implicitly does not satisfy max either. 
        // However, satisfying min is no guarantee that the same streamcut satisfies the max policy as well. 
        // So it is possible that all streamcuts in retentionset do not satisfy max while each satisfying min. In this case
        // we choose the most recent streamcut as max (which was also the min). 
        AtomicLong maxSoFar = new AtomicLong(Long.MIN_VALUE);
        AtomicLong minSoFar = new AtomicLong(Long.MAX_VALUE);
        retentionSet.getRetentionRecords().forEach(x -> {
            long value = delta.apply(x);
            if (value >= policy.getRetentionParam() && value <= policy.getRetentionMax() && value > maxSoFar.get()) {
                max.set(x);
                maxSoFar.set(value);
            }
            if (value >= policy.getRetentionParam() && value < minSoFar.get()) {
                min.set(x);
                minSoFar.set(value);
            }
        });
        if (max.get() == null) { 
            // if we are unable to find a streamcut that satisfies max policy constraint, but there is 
            // a min streamcut bound which was actually beyond the max constraint, we will set max to min. 
            max.set(min.get());
        }
        return new AbstractMap.SimpleEntry<>(max.get(), min.get());
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
    public CompletableFuture<StreamCutRecord> generateStreamCut(final String scope, final String stream,
                                                                final StreamCutRecord previous,
                                                                final OperationContext contextOpt, String delegationToken) {
        final OperationContext context = contextOpt != null ? contextOpt :
                streamMetadataStore.createStreamContext(scope, stream, ControllerService.nextRequestId());

        return streamMetadataStore.getActiveSegments(scope, stream, context, executor)
                .thenCompose(activeSegments -> Futures.allOfWithResults(activeSegments
                        .stream()
                        .parallel()
                        .collect(Collectors.toMap(x -> x, x -> getSegmentOffset(scope, stream, x.segmentId(),
                                delegationToken, context.getRequestId())))))
                .thenCompose(map -> {
                    final long generationTime = retentionClock.get().get();
                    ImmutableMap.Builder<Long, Long> builder = ImmutableMap.builder();
                    map.forEach((key, value) -> builder.put(key.segmentId(), value));
                    ImmutableMap<Long, Long> streamCutMap = builder.build();
                    return streamMetadataStore.getSizeTillStreamCut(scope, stream, streamCutMap,
                            Optional.ofNullable(previous), context, executor)
                                              .thenApply(sizeTill -> new StreamCutRecord(generationTime, sizeTill, streamCutMap));
                });
    }

    /**
     * Truncate a stream.
     *
     * @param scope      scope.
     * @param stream     stream name.
     * @param streamCut  stream cut.
     * @param requestId  requestId
     * @return update status.
     */
    public CompletableFuture<UpdateStreamStatus.Status> truncateStream(final String scope, final String stream,
                                                                       final Map<Long, Long> streamCut,
                                                                       final long requestId) {
        final OperationContext context = streamMetadataStore.createStreamContext(scope, stream, requestId);

        return streamMetadataStore.getState(scope, stream, true, context, executor)
                .thenCompose(state -> {
                    if (state.equals(State.SEALED)) {
                        log.error(requestId, "Cannot truncate a sealed stream {}/{}", scope, stream);
                        return CompletableFuture.completedFuture(UpdateStreamStatus.Status.STREAM_SEALED);
                    }
                    // 1. get stream cut
                    return eventHelperFuture.thenCompose(eventHelper -> startTruncation(scope, stream, streamCut, context) // 1. get stream cut
                                    // 4. check for truncation to complete
                                    .thenCompose(truncationStarted -> {
                                        if (truncationStarted) {
                                            return eventHelper.checkDone(() -> isTruncated(scope, stream, streamCut, context), 1000L)
                                                    .thenApply(y -> UpdateStreamStatus.Status.SUCCESS);
                                        } else {
                                            log.error(requestId, "Unable to start truncation for {}/{}", scope, stream);
                                            return CompletableFuture.completedFuture(UpdateStreamStatus.Status.FAILURE);
                                        }
                                    }));
                })
                .exceptionally(ex -> {
                    final String message = "Exception thrown in trying to truncate stream";
                    return handleUpdateStreamError(ex, requestId, message, NameUtils.getScopedStreamName(scope, stream));
                });
    }

    public CompletableFuture<Boolean> startTruncation(String scope, String stream, Map<Long, Long> streamCut,
                                                       OperationContext contextOpt) {
        final OperationContext context = contextOpt != null ? contextOpt :
                streamMetadataStore.createStreamContext(scope, stream, ControllerService.nextRequestId());
        long requestId = context.getRequestId();
        return streamMetadataStore.getTruncationRecord(scope, stream, context, executor)
                .thenCompose(property -> {
                    if (!property.getObject().isUpdating()) {
                        // 2. post event with new stream cut if no truncation is ongoing
                        return eventHelperFuture.thenCompose(eventHelper -> eventHelper.addIndexAndSubmitTask(
                                new TruncateStreamEvent(scope, stream, requestId),
                                // 3. start truncation by updating the metadata
                                () -> streamMetadataStore.startTruncation(scope, stream, streamCut,
                                        context, executor))
                                .thenApply(x -> {
                                    log.debug(requestId, "Started truncation request for stream {}/{}", scope, stream);
                                    return true;
                                }));
                    } else {
                        log.error(requestId, "Another truncation in progress for {}/{}", scope, stream);
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
                                        // if stream is sealed then truncate should not be allowed
                                        if (state.equals(State.SEALED)) {
                                            log.error("Cannot truncate a sealed stream {}/{}", scope, stream);
                                            throw new UnsupportedOperationException("Cannot truncate a sealed stream: " + NameUtils.getScopedStreamName(scope, stream));
                                        }
                                        // if truncate-barrier is not updating, then truncate is complete if property
                                        // matches our expectation and state is not updating
                                        return !(truncationRecord.getStreamCut().equals(streamCut) && state.equals(State.TRUNCATING));
                                    }
                                });
    }

    /**
     * Seal a stream.
     *
     * @param scope      scope.
     * @param stream     stream name.
     * @param requestId  requestId
     * @return update status.
     */
    public CompletableFuture<UpdateStreamStatus.Status> sealStream(String scope, String stream, long requestId) {
        final OperationContext context = streamMetadataStore.createStreamContext(scope, stream, requestId);
        return sealStream(scope, stream, context);
    }

    public CompletableFuture<UpdateStreamStatus.Status> sealStream(String scope, String stream, OperationContext context) {
        return sealStream(scope, stream, context, 10);
    }

    @VisibleForTesting
    CompletableFuture<UpdateStreamStatus.Status> sealStream(String scope, String stream, OperationContext context, int retryCount) {
        long requestId = context.getRequestId();
        // 1. post event for seal.
        SealStreamEvent event = new SealStreamEvent(scope, stream, requestId);
        return eventHelperFuture.thenCompose(eventHelper -> eventHelper.addIndexAndSubmitTask(event,
                // 2. set state to sealing
                () -> RetryHelper.withRetriesAsync(() -> streamMetadataStore.getVersionedState(scope, stream, context, executor)
                .thenCompose(state -> {
                    if (state.getObject().equals(State.SEALED)) {
                        return CompletableFuture.completedFuture(state);
                    } else {
                        return streamMetadataStore.updateVersionedState(scope, stream, State.SEALING, state, context, executor);
                    }
                }), RetryHelper.RETRYABLE_PREDICATE.or(e -> Exceptions.unwrap(e) instanceof
                        StoreException.OperationNotAllowedException), retryCount, executor))
                // 3. return with seal initiated.
                .thenCompose(result -> {
                    if (result.getObject().equals(State.SEALED) || result.getObject().equals(State.SEALING)) {
                        return eventHelper.checkDone(() -> isSealed(scope, stream, context))
                                .thenApply(x -> UpdateStreamStatus.Status.SUCCESS);
                    } else {
                        return CompletableFuture.completedFuture(UpdateStreamStatus.Status.FAILURE);
                    }
                }))
                .exceptionally(ex -> {
                    final String message = "Exception thrown in trying to notify sealed segments.";
                    return handleUpdateStreamError(ex, requestId, message, NameUtils.getScopedStreamName(scope, stream));
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
     * @param requestId  requestid
     * @return delete status.
     */
    public CompletableFuture<DeleteStreamStatus.Status> deleteStream(final String scope, final String stream,
                                                                     final long requestId) {
        final OperationContext context = streamMetadataStore.createStreamContext(scope, stream, requestId);
        return deleteStream(scope, stream, context);
    }

    public CompletableFuture<DeleteStreamStatus.Status> deleteStream(final String scope, final String stream,
                                                                     final OperationContext context) {
        Preconditions.checkNotNull(context, "Operation Context is null");
        // We can delete streams only if they are sealed. However, for partially created streams, they could be in different
        // stages of partial creation and we should be able to clean them up.
        // Case 1: A partially created stream may just have some initial metadata created, in which case the Stream's state may not
        // have been set up it may be present under the scope.
        // In this case we can simply delete all metadata for the stream directly.
        // Case 2: A partially created stream could be in state CREATING, in which case it would definitely have metadata created
        // and possibly segments too. This requires same clean up as for a sealed stream - metadata + segments.
        // So we will submit delete workflow.
        long requestId = context.getRequestId();
        return eventHelperFuture.thenCompose(eventHelper -> Futures.exceptionallyExpecting(
                streamMetadataStore.getState(scope, stream, false, context, executor),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, State.UNKNOWN)
                .thenCompose(state -> {
                    if (State.SEALED.equals(state) || State.CREATING.equals(state)) {
                        return streamMetadataStore.getCreationTime(scope, stream, context, executor)
                                                  .thenApply(time -> new DeleteStreamEvent(scope, stream, requestId, time))
                                                  .thenCompose(eventHelper::writeEvent)
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
                        return eventHelper.checkDone(() -> isDeleted(scope, stream, context))
                                .thenApply(x -> DeleteStreamStatus.Status.SUCCESS);
                    } else {
                        return CompletableFuture.completedFuture(DeleteStreamStatus.Status.STREAM_NOT_SEALED);
                    }
                }))
                .exceptionally(ex -> {
                    return handleDeleteStreamError(ex, requestId, NameUtils.getScopedStreamName(scope, stream));
                });
    }

    private CompletableFuture<Boolean> isDeleted(String scope, String stream, OperationContext context) {
        return streamMetadataStore.checkStreamExists(scope, stream, context, executor)
                .thenApply(x -> !x);
    }

    private CompletableFuture<Boolean> isScopeDeletionComplete(String scope, OperationContext context) {
        // The last step of Recursive Delete Scope is to remove entry of scope from Deleting_Scopes_Table
        // Method will return true if the entry is removed from Scopes Table
        return streamMetadataStore.isScopeSealed(scope, context, executor)
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
     * @param requestId      request id
     * @return returns the newly created segments.
     */
    public CompletableFuture<ScaleResponse> manualScale(String scope, String stream, List<Long> segmentsToSeal,
                                                        List<Map.Entry<Double, Double>> newRanges, long scaleTimestamp,
                                                        long requestId) {
        final OperationContext context = streamMetadataStore.createStreamContext(scope, stream, requestId);

        ScaleOpEvent event = new ScaleOpEvent(scope, stream, segmentsToSeal, newRanges, true, scaleTimestamp,
                requestId);

        return eventHelperFuture.thenCompose(eventHelper -> eventHelper.addIndexAndSubmitTask(event,
                () -> streamMetadataStore.submitScale(scope, stream, segmentsToSeal, new ArrayList<>(newRanges),
                        scaleTimestamp, null, context, executor))
                        .handle((startScaleResponse, e) -> {
                            ScaleResponse.Builder response = ScaleResponse.newBuilder();

                            if (e != null) {
                                Throwable cause = Exceptions.unwrap(e);
                                if (cause instanceof EpochTransitionOperationExceptions.PreConditionFailureException) {
                                    response.setStatus(ScaleResponse.ScaleStreamStatus.PRECONDITION_FAILED);
                                } else {
                                    log.error(requestId, "Scale for stream {}/{} failed with exception {}",
                                            scope, stream, cause);
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
                        }));
    }

    /**
     * Helper method to check if scale operation against an epoch completed or not.
     *
     * @param scope          scope.
     * @param stream         stream name.
     * @param epoch          stream epoch.
     * @param requestId      request id.
     * @return returns the newly created segments.
     */
    public CompletableFuture<ScaleStatusResponse> checkScale(String scope, String stream, int epoch, long requestId) {
        OperationContext context = streamMetadataStore.createStreamContext(scope, stream, requestId);

        CompletableFuture<EpochRecord> activeEpochFuture =
                streamMetadataStore.getActiveEpoch(scope, stream, context, true, executor);
        CompletableFuture<State> stateFuture =
                streamMetadataStore.getState(scope, stream, true, context, executor);
        CompletableFuture<EpochTransitionRecord> etrFuture =
                streamMetadataStore.getEpochTransition(scope, stream, context, executor).thenApply(VersionedMetadata::getObject);
        return CompletableFuture.allOf(stateFuture, activeEpochFuture, etrFuture)
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
                                            (etr.equals(EpochTransitionRecord.EMPTY) ||
                                                    etr.getNewEpoch() == activeEpoch.getEpoch())) {
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
        return eventHelperFuture.thenCompose(eventHelper -> eventHelper.addIndexAndSubmitTask(event, futureSupplier));
    }

    public CompletableFuture<Void> writeEvent(ControllerEvent event) {
        return eventHelperFuture.thenCompose(eventHelper -> eventHelper.writeEvent(event));
    }

    @VisibleForTesting
    public void setRequestEventWriter(EventStreamWriter<ControllerEvent> requestEventWriter) {
        eventHelperFuture.thenAccept(eventHelper -> eventHelper.setRequestEventWriter(requestEventWriter));
    }

    CompletableFuture<Void> removeTaskFromIndex(String hostId, String id) {
        return eventHelperFuture.thenCompose(eventHelper -> eventHelper.removeTaskFromIndex(hostId, id));
    }

    @VisibleForTesting
    CompletableFuture<CreateStreamStatus.Status> createStreamBody(String scope, String stream, StreamConfiguration config,
                                                                  long timestamp, OperationContext context) {
        long requestId = getRequestId(context);
        return this.streamMetadataStore.isScopeSealed(scope, context, executor).thenCompose(scopeSealed -> {
           if (scopeSealed) {
               log.warn(requestId, "Create stream failed due to scope in sealed state");
               return CompletableFuture.completedFuture(CreateStreamStatus.Status.SCOPE_NOT_FOUND);
           }
            return this.streamMetadataStore.createStream(scope, stream, config, timestamp, context, executor)
                    .thenComposeAsync(response -> {
                        log.debug(requestId, "{}/{} created in metadata store", scope, stream);
                        CreateStreamStatus.Status status = translate(response.getStatus());
                        // only if it's a new stream or an already existing non-active stream then we will create
                        // segments and change the state of the stream to active.
                        if (response.getStatus().equals(CreateStreamResponse.CreateStatus.NEW) ||
                                response.getStatus().equals(CreateStreamResponse.CreateStatus.EXISTS_CREATING)) {
                            final int startingSegmentNumber = response.getStartingSegmentNumber();
                            final int minNumSegments = response.getConfiguration().getScalingPolicy().getMinNumSegments();
                            List<Long> newSegments = IntStream.range(startingSegmentNumber, startingSegmentNumber + minNumSegments)
                                    .boxed()
                                    .map(x -> NameUtils.computeSegmentId(x, 0))
                                    .collect(Collectors.toList());
                            return notifyNewSegments(scope, stream, response.getConfiguration(), newSegments,
                                    this.retrieveDelegationToken(), requestId)
                                    .thenCompose(v -> createMarkStream(scope, stream, timestamp, requestId))
                                    .thenCompose(y -> {
                                        return withRetries(() -> {
                                            CompletableFuture<Void> future;
                                            if (config.getRetentionPolicy() != null) {
                                                future = bucketStore.addStreamToBucketStore(
                                                        BucketStore.ServiceType.RetentionService, scope, stream, executor);
                                            } else {
                                                future = CompletableFuture.completedFuture(null);
                                            }
                                            return future.thenCompose(v -> streamMetadataStore.addStreamTagsToIndex(scope, stream, config, context, executor) )
                                                    .thenCompose(v -> streamMetadataStore.getVersionedState(scope, stream, context, executor)
                                                            .thenCompose(state -> {
                                                                if (state.getObject().equals(State.CREATING)) {
                                                                    return streamMetadataStore.updateVersionedState(
                                                                            scope, stream, State.ACTIVE, state, context, executor);
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
                            log.warn(requestId, "Create stream failed due to ", ex);
                            if (cause instanceof StoreException.DataNotFoundException) {
                                return CreateStreamStatus.Status.SCOPE_NOT_FOUND;
                            } else {
                                return CreateStreamStatus.Status.FAILURE;
                            }
                        } else {
                            return result;
                        }
                    });
        });
    }

    /**
     * Method to create mark stream linked to the base stream. Mark Stream is a special single segmented dedicated
     * internal stream where watermarks for the said stream are stored.
     * @param scope scope for base stream
     * @param baseStream name of base stream
     * @param timestamp timestamp
     * @param requestId request id for stream creation.
     * @return Completable future which is completed successfully when the internal mark stream is created
     */
    private CompletableFuture<Void> createMarkStream(String scope, String baseStream, long timestamp, long requestId) {
        String markStream = NameUtils.getMarkStreamForStream(baseStream);
        StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build();
        OperationContext context = streamMetadataStore.createStreamContext(scope, markStream, requestId);
        return this.streamMetadataStore.createStream(scope, markStream, config, timestamp, context, executor)
                                .thenCompose(response -> {
                                    final long segmentId = NameUtils.computeSegmentId(response.getStartingSegmentNumber(), 0);
                                    return notifyNewSegment(scope, markStream, segmentId,
                                            response.getConfiguration().getScalingPolicy(),
                                            this.retrieveDelegationToken(), requestId, config.getRolloverSizeBytes());
                                })
                                .thenCompose(v -> {
                                    return streamMetadataStore.getVersionedState(scope, markStream, context, executor)
                                                       .thenCompose(state ->
                                                               Futures.toVoid(streamMetadataStore.updateVersionedState(
                                                                       scope, markStream, State.ACTIVE, state, context,
                                                                       executor)));
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
                                                     String controllerToken, long requestId) {
        return withRetries(() -> streamMetadataStore.getConfiguration(scope, stream, context, executor), executor)
                .thenCompose(configuration -> notifyNewSegments(scope, stream, configuration, segmentIds, controllerToken,
                        requestId));
    }

    public CompletableFuture<Void> notifyNewSegments(String scope, String stream, StreamConfiguration configuration,
                                                     List<Long> segmentIds, String controllerToken, long requestId) {
        return Futures.toVoid(Futures.allOfWithResults(segmentIds
                .stream()
                .parallel()
                .map(segment -> notifyNewSegment(scope, stream, segment, configuration.getScalingPolicy(), controllerToken,
                        requestId, configuration.getRolloverSizeBytes()))
                .collect(Collectors.toList())));
    }

    public CompletableFuture<Void> notifyNewSegment(String scope, String stream, long segmentId, ScalingPolicy policy,
                                                    String controllerToken, long requestId, long rolloverSize) {
        return Futures.toVoid(withRetries(() -> segmentHelper.createSegment(scope,
                stream, segmentId, policy, controllerToken, requestId, rolloverSize), executor));
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

    public CompletableFuture<Map<Long, Long>> getSealedSegmentsSize(String scope, String stream, List<Long> segments,
                                                                    String delegationToken, long requestId) {
        return Futures.allOfWithResults(
                segments.stream()
                        .parallel()
                        .collect(Collectors.toMap(x -> x, x -> getSegmentOffset(scope, stream, x, delegationToken, requestId))));
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

    private CompletableFuture<Long> getSegmentOffset(String scope, String stream, long segmentId, String delegationToken,
                                                     long requestId) {
        return withRetries(() -> segmentHelper.getSegmentInfo(
                scope,
                stream,
                segmentId,
                delegationToken, requestId), executor)
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

    private UpdateStreamStatus.Status handleUpdateStreamError(Throwable ex, long requestId, String logMessage, String streamFullName) {
        Throwable cause = Exceptions.unwrap(ex);
        log.error(requestId, "Exception updating Stream {}. Cause: {}.", streamFullName, logMessage, cause);
        if (cause instanceof StoreException.DataNotFoundException) {
            return UpdateStreamStatus.Status.STREAM_NOT_FOUND;
        } else if (cause instanceof UnsupportedOperationException) {
            return UpdateStreamStatus.Status.STREAM_SEALED;
        } else if (cause instanceof TimeoutException) {
            throw new CompletionException(cause);
        } else {
            return UpdateStreamStatus.Status.FAILURE;
        }
    }

    private DeleteStreamStatus.Status handleDeleteStreamError(Throwable ex, long requestId, String streamFullName) {
        Throwable cause = Exceptions.unwrap(ex);
        log.error(requestId, "Exception deleting stream {}. Cause: {}", streamFullName, ex);
        if (cause instanceof StoreException.DataNotFoundException) {
            return DeleteStreamStatus.Status.STREAM_NOT_FOUND;
        } else if (cause instanceof TimeoutException) {
            throw new CompletionException(cause);
        } else {
            return DeleteStreamStatus.Status.FAILURE;
        }
    }

    private DeleteScopeStatus.Status handleDeleteScopeError(Throwable ex, long requestId, String scopeName) {
        Throwable cause = Exceptions.unwrap(ex);
        log.error(requestId, "Exception deleting scope {}. Cause: {}", scopeName, ex);
        if (cause instanceof StoreException.DataNotFoundException) {
            return DeleteScopeStatus.Status.SCOPE_NOT_FOUND;
        } else if (cause instanceof TimeoutException) {
            throw new CompletionException(cause);
        } else {
            return DeleteScopeStatus.Status.FAILURE;
        }
    }

    public CompletableFuture<Map<Long, List<Long>>> mergeTxnSegmentsIntoStreamSegments(final String scope, final String stream,
                                                                                       final List<Long> segments, final List<UUID> txnIds, long requestId) {
        return Futures.allOfWithResults(segments.stream()
                        .collect(Collectors.toMap(segId -> segId, segId -> mergeTxnSegments(scope, stream, segId, txnIds, requestId))));
    }

    private CompletableFuture<List<Long>> mergeTxnSegments(final String scope, final String stream,
                                                                             final long segmentNumber, final List<UUID> txnIds, long requestId) {
        return TaskStepsRetryHelper.withRetries(() -> segmentHelper.mergeTxnSegments(scope,
                stream,
                segmentNumber,
                segmentNumber,
                txnIds,
                this.retrieveDelegationToken(), requestId).exceptionally(ex -> {
                    if (ex instanceof WireCommandFailedException) {
                        WireCommandFailedException wcfe = (WireCommandFailedException) ex;
                        if (WireCommandFailedException.Reason.SegmentDoesNotExist.equals(wcfe.getReason())) {
                            final String qualifiedSegmentName = getQualifiedStreamSegmentName(scope, stream, segmentNumber);
                            String message = String.format("Segment Merge failed as target segment [%s] was not found.", qualifiedSegmentName);
                            throw new IllegalStateException(message);
                        }
                    }
                    throw new CompletionException(ex);
        }), executor);
    }

    public CompletableFuture<Void> notifyTxnAbort(final String scope, final String stream,
                                                  final List<Long> segments, final UUID txnId, long requestId) {
        Timer timer = new Timer();
        return Futures.allOf(segments.stream()
                .parallel()
                .map(segment -> notifyTxnAbort(scope, stream, segment, txnId, requestId))
                .collect(Collectors.toList()))
                .thenRun(() -> TransactionMetrics.getInstance().abortTransactionSegments(timer.getElapsed()));
    }

    private CompletableFuture<Controller.TxnStatus> notifyTxnAbort(final String scope, final String stream,
                                                                   final long segmentNumber, final UUID txnId,
                                                                   long requestId) {
        return TaskStepsRetryHelper.withRetries(() -> segmentHelper.abortTransaction(scope,
                stream,
                segmentNumber,
                txnId,
                this.retrieveDelegationToken(), requestId), executor);
    }

    public CompletableFuture<Map<Long, Long>> getCurrentSegmentSizes(String scope, String stream, List<Long> segments,
                                                                     long requestId) {
        return Futures.allOfWithResults(segments.stream().collect(
                Collectors.toMap(x -> x, x -> getSegmentOffset(scope, stream, x, this.retrieveDelegationToken(), requestId))));
    }

    public CompletableFuture<Void> processScale(String scope, String stream,
                                                VersionedMetadata<EpochTransitionRecord> record, OperationContext context,
                                                long requestId, StreamMetadataStore store) {
        List<Long> segmentIds = new ArrayList<>(record.getObject().getNewSegmentsWithRange().keySet());
        List<Long> segmentsToSeal = new ArrayList<>(record.getObject().getSegmentsToSeal());
        String delegationToken = retrieveDelegationToken();
        return notifyNewSegments(scope, stream, segmentIds, context, delegationToken, requestId)
                  .thenCompose(x -> store.scaleCreateNewEpochs(scope, stream, record, context, executor))
                  .thenCompose(x -> notifySealedSegments(scope, stream, segmentsToSeal, delegationToken, requestId))
                  .thenCompose(x -> getSealedSegmentsSize(scope, stream, segmentsToSeal, delegationToken, requestId))
                  .thenCompose(map -> store.scaleSegmentsSealed(scope, stream, map, record, context, executor))
                  .thenCompose(x -> store.completeScale(scope, stream, record, context, executor));
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
                authHelper);
    }

    @Override
    public void close() throws Exception {
        synchronized (lock) {
            toSetEventHelper = false;
            if (eventHelper != null) {
                eventHelper.close();
            }
        }
        eventHelperFuture.cancel(true);
    }

    public String retrieveDelegationToken() {
        return authHelper.retrieveMasterToken();
    }

    @VisibleForTesting
    public void setCompletionTimeoutMillis(long timeoutMillis) {
        eventHelperFuture.thenAccept(eventHelper -> eventHelper.setCompletionTimeoutMillis(timeoutMillis));
    }

    @VisibleForTesting
    void setRetentionFrequencyMillis(long timeoutMillis) {
        retentionFrequencyMillis.set(timeoutMillis);
    }

    @VisibleForTesting
    void setRetentionClock(Supplier<Long> clock) {
        retentionClock.set(clock);
    }

    public long getRequestId(OperationContext context) {
        return context != null ? context.getRequestId() : ControllerService.nextRequestId();
    }
}

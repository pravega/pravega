/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
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
import io.pravega.client.ClientFactory;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ModelHelper;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.eventProcessor.ControllerEventProcessors;
import io.pravega.controller.server.eventProcessor.requesthandlers.TaskExceptions;
import io.pravega.controller.server.rpc.auth.PravegaInterceptor;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.stream.CreateStreamResponse;
import io.pravega.controller.store.stream.EpochTransitionOperationExceptions;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.Segment;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.store.stream.tables.StreamCutRecord;
import io.pravega.controller.store.task.Resource;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScaleStatusResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentRange;
import io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus;
import io.pravega.controller.task.Task;
import io.pravega.controller.task.TaskBase;
import io.pravega.controller.util.Config;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.shared.controller.event.DeleteStreamEvent;
import io.pravega.shared.controller.event.ScaleOpEvent;
import io.pravega.shared.controller.event.SealStreamEvent;
import io.pravega.shared.controller.event.TruncateStreamEvent;
import io.pravega.shared.controller.event.UpdateStreamEvent;
import io.pravega.shared.metrics.DynamicLogger;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import java.io.Serializable;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;

import static io.pravega.controller.task.Stream.TaskStepsRetryHelper.withRetries;
import static io.pravega.shared.MetricsNames.RETENTION_FREQUENCY;
import static io.pravega.shared.MetricsNames.nameFromStream;

/**
 * Collection of metadata update tasks on stream.
 * Task methods are annotated with @Task annotation.
 * <p>
 * Any update to the task method signature should be avoided, since it can cause problems during upgrade.
 * Instead, a new overloaded method may be created with the same task annotation name but a new version.
 */
@Slf4j
public class StreamMetadataTasks extends TaskBase {
    private static final long RETENTION_FREQUENCY_IN_MINUTES = Duration.ofMinutes(Config.MINIMUM_RETENTION_FREQUENCY_IN_MINUTES).toMillis();
    private static final DynamicLogger DYNAMIC_LOGGER = MetricsProvider.getDynamicLogger();

    private final StreamMetadataStore streamMetadataStore;
    private final HostControllerStore hostControllerStore;
    private final ConnectionFactory connectionFactory;
    private final SegmentHelper segmentHelper;
    private final String tokenSigningKey;
    private ClientFactory clientFactory;
    private String requestStreamName;

    private final AtomicReference<EventStreamWriter<ControllerEvent>> requestEventWriterRef = new AtomicReference<>();
    private final boolean authEnabled;

    public StreamMetadataTasks(final StreamMetadataStore streamMetadataStore,
                               final HostControllerStore hostControllerStore, final TaskMetadataStore taskMetadataStore,
                               final SegmentHelper segmentHelper, final ScheduledExecutorService executor, final String hostId,
                               final ConnectionFactory connectionFactory, boolean authEnabled, String tokenSigningKey) {
        this(streamMetadataStore, hostControllerStore, taskMetadataStore, segmentHelper, executor, new Context(hostId),
                connectionFactory, authEnabled, tokenSigningKey);
    }

    private StreamMetadataTasks(final StreamMetadataStore streamMetadataStore,
                                final HostControllerStore hostControllerStore, final TaskMetadataStore taskMetadataStore,
                                final SegmentHelper segmentHelper, final ScheduledExecutorService executor, final Context context,
                                ConnectionFactory connectionFactory, boolean authEnabled, String tokenSigningKey) {
        super(taskMetadataStore, executor, context);
        this.streamMetadataStore = streamMetadataStore;
        this.hostControllerStore = hostControllerStore;
        this.segmentHelper = segmentHelper;
        this.connectionFactory = connectionFactory;
        this.authEnabled = authEnabled;
        this.tokenSigningKey = tokenSigningKey;
        this.setReady();
    }

    public void initializeStreamWriters(final ClientFactory clientFactory,
                                        final String streamName) {
        this.requestStreamName = streamName;
        this.clientFactory = clientFactory;
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

        // 1. get configuration
        return streamMetadataStore.getConfigurationRecord(scope, stream, true, context, executor)
                .thenCompose(configProperty -> {
                    // 2. post event to start update workflow
                    if (!configProperty.isUpdating()) {
                        return writeEvent(new UpdateStreamEvent(scope, stream))
                                // 3. update new configuration in the store with updating flag = true
                                // if attempt to update fails, we bail out with no harm done
                                .thenCompose(x -> streamMetadataStore.startUpdateConfiguration(scope, stream, newConfig,
                                        context, executor))
                                // 4. wait for update to complete
                                .thenCompose(x -> checkDone(() -> isUpdated(scope, stream, newConfig, context))
                                        .thenApply(y -> UpdateStreamStatus.Status.SUCCESS));
                    } else {
                        log.warn("Another update in progress for {}/{}", scope, stream);
                        return CompletableFuture.completedFuture(UpdateStreamStatus.Status.FAILURE);
                    }
                })
                .exceptionally(ex -> {
                    log.warn("Exception thrown in trying to update stream configuration {}", ex.getMessage());
                    return handleUpdateStreamError(ex);
                });
    }

    private CompletionStage<Void> checkDone(Supplier<CompletableFuture<Boolean>> condition) {
        AtomicBoolean isDone = new AtomicBoolean(false);
        return Futures.loop(() -> !isDone.get(),
                () -> Futures.delayedFuture(condition, 100, executor)
                             .thenAccept(isDone::set), executor);
    }

    private CompletableFuture<Boolean> isUpdated(String scope, String stream, StreamConfiguration newConfig, OperationContext context) {
        return streamMetadataStore.getConfigurationRecord(scope, stream, true, context, executor)
                .thenApply(configProperty -> !configProperty.isUpdating() || !configProperty.getStreamConfiguration().equals(newConfig));
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

        return streamMetadataStore.getStreamCutsFromRetentionSet(scope, stream, context, executor)
                .thenCompose(retentionSet -> {
                    StreamCutRecord latestCut = retentionSet.stream()
                            .max(Comparator.comparingLong(StreamCutRecord::getRecordingTime)).orElse(null);
                    return checkGenerateStreamCut(scope, stream, context, latestCut, recordingTime, delegationToken)
                            .thenCompose(newRecord -> truncate(scope, stream, policy, context, retentionSet, newRecord, recordingTime));
                })
                .thenAccept(x -> DYNAMIC_LOGGER.recordMeterEvents(nameFromStream(RETENTION_FREQUENCY, scope, stream), 1));

    }

    private CompletableFuture<StreamCutRecord> checkGenerateStreamCut(String scope, String stream, OperationContext context,
                                                                      StreamCutRecord latestCut, long recordingTime, String delegationToken) {
        if (latestCut == null || recordingTime - latestCut.getRecordingTime() > RETENTION_FREQUENCY_IN_MINUTES) {
            return generateStreamCut(scope, stream, context, delegationToken)
                    .thenCompose(newRecord -> streamMetadataStore.addStreamCutToRetentionSet(scope, stream, newRecord, context, executor)
                        .thenApply(x -> {
                            log.debug("New streamCut generated for stream {}/{}", scope, stream);
                            return newRecord;
                        }));
        } else {
            return  CompletableFuture.completedFuture(null);
        }
    }

    private CompletableFuture<Void> truncate(String scope, String stream, RetentionPolicy policy, OperationContext context,
                                             List<StreamCutRecord> retentionSet, StreamCutRecord newRecord, long recordingTime) {
        return findTruncationRecord(policy, retentionSet, newRecord, recordingTime)
                .map(record -> startTruncation(scope, stream, record.getStreamCut(), context)
                        .thenCompose(started -> {
                            if (started) {
                                return streamMetadataStore.deleteStreamCutBefore(scope, stream, record, context, executor);
                            } else {
                                throw new RuntimeException("Could not start truncation");
                            }
                        })
                        .exceptionally(e -> {
                            if (Exceptions.unwrap(e) instanceof IllegalArgumentException) {
                                // This is ignorable exception. Throwing this will cause unnecessary retries and exceptions logged.
                                log.debug("Cannot truncate at given streamCut because it intersects with existing truncation point");
                                return null;
                            } else {
                                throw new CompletionException(e);
                            }
                        })
                ).orElse(CompletableFuture.completedFuture(null));
    }

    private Optional<StreamCutRecord> findTruncationRecord(RetentionPolicy policy, List<StreamCutRecord> retentionSet,
                                                           StreamCutRecord newRecord, long recordingTime) {
        switch (policy.getRetentionType()) {
            case TIME:
                return retentionSet.stream().filter(x -> x.getRecordingTime() < recordingTime - policy.getRetentionParam())
                        .max(Comparator.comparingLong(StreamCutRecord::getRecordingTime));
            case SIZE:
                // find a stream cut record Si from retentionSet R = {S1.. Sn} such that Sn.size - Si.size > policy and
                // Sn.size - Si+1.size < policy
                Optional<StreamCutRecord> latestOpt = Optional.ofNullable(newRecord);

                return latestOpt.flatMap(latest ->
                        retentionSet.stream().filter(x -> (latest.getRecordingSize() - x.getRecordingSize()) > policy.getRetentionParam())
                                .max(Comparator.comparingLong(StreamCutRecord::getRecordingTime)));
            default:
                throw new NotImplementedException("Size based retention");
        }
    }

    /**
     * Generate a new stream cut.
     *
     * @param scope      scope.
     * @param stream     stream name.
     * @param contextOpt optional context
     * @param delegationToken token to be sent to segmentstore.
     * @return streamCut.
     */
    public CompletableFuture<StreamCutRecord> generateStreamCut(final String scope, final String stream,
                                                                final OperationContext contextOpt, String delegationToken) {
        final OperationContext context = contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;

        return streamMetadataStore.getActiveSegments(scope, stream, context, executor)
                .thenCompose(activeSegments -> Futures.allOfWithResults(activeSegments
                        .stream()
                        .parallel()
                        .collect(Collectors.toMap(Segment::segmentId, x -> getSegmentOffset(scope, stream, x.segmentId(), delegationToken)))))
                .thenCompose(map -> {
                    final long generationTime = System.currentTimeMillis();
                    return streamMetadataStore.getSizeTillStreamCut(scope, stream, map, context, executor)
                            .thenApply(sizeTill -> new StreamCutRecord(generationTime, sizeTill, ImmutableMap.copyOf(map)));
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

        // 1. get stream cut
        return startTruncation(scope, stream, streamCut, context)
                // 4. check for truncation to complete
                .thenCompose(truncationStarted -> {
                    if (truncationStarted) {
                        return checkDone(() -> isTruncated(scope, stream, streamCut, context))
                                .thenApply(y -> UpdateStreamStatus.Status.SUCCESS);
                    } else {
                        log.warn("Unable to start truncation for {}/{}", scope, stream);
                        return CompletableFuture.completedFuture(UpdateStreamStatus.Status.FAILURE);
                    }
                })
                .exceptionally(ex -> {
                    log.warn("Exception thrown in trying to update stream configuration {}", ex);
                    return handleUpdateStreamError(ex);
                });
    }

    private CompletableFuture<Boolean> startTruncation(String scope, String stream, Map<Long, Long> streamCut, OperationContext contextOpt) {
        final OperationContext context = contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;

        return streamMetadataStore.getTruncationRecord(scope, stream, true, context, executor)
                .thenCompose(property -> {
                    if (!property.isUpdating()) {
                        // 2. post event with new stream cut if no truncation is ongoing
                        return writeEvent(new TruncateStreamEvent(scope, stream))
                                // 3. start truncation by updating the metadata
                                .thenCompose(x -> streamMetadataStore.startTruncation(scope, stream, streamCut,
                                        context, executor))
                                .thenApply(x -> {
                                    log.debug("Started truncation request for stream {}/{}", scope, stream);
                                    return true;
                                });
                    } else {
                        log.warn("Another truncation in progress for {}/{}", scope, stream);
                        return CompletableFuture.completedFuture(false);
                    }
                });
    }

    private CompletableFuture<Boolean> isTruncated(String scope, String stream, Map<Long, Long> streamCut, OperationContext context) {
        return streamMetadataStore.getTruncationRecord(scope, stream, true, context, executor)
                .thenApply(truncationProp -> !truncationProp.isUpdating() || !truncationProp.getStreamCut().equals(streamCut));
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
        final OperationContext context = contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;

        // 1. post event for seal.
        SealStreamEvent event = new SealStreamEvent(scope, stream);
        return writeEvent(event)
                // 2. set state to sealing
                .thenCompose(x -> streamMetadataStore.getState(scope, stream, false, context, executor))
                .thenCompose(state -> {
                    if (state.equals(State.SEALED)) {
                        return CompletableFuture.completedFuture(true);
                    } else {
                        return streamMetadataStore.setState(scope, stream, State.SEALING, context, executor);
                    }
                })
                // 3. return with seal initiated.
                .thenCompose(result -> {
                    if (result) {
                        return checkDone(() -> isSealed(scope, stream, context))
                                .thenApply(x -> UpdateStreamStatus.Status.SUCCESS);
                    } else {
                        return CompletableFuture.completedFuture(UpdateStreamStatus.Status.FAILURE);
                    }
                })
                .exceptionally(ex -> {
                    log.warn("Exception thrown in trying to notify sealed segments {}", ex.getMessage());
                    return handleUpdateStreamError(ex);
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

        return streamMetadataStore.getState(scope, stream, false, context, executor)
                .thenCompose(state -> {
                    if (!state.equals(State.SEALED)) {
                        return CompletableFuture.completedFuture(false);
                    } else {
                        return writeEvent(new DeleteStreamEvent(scope, stream))
                                .thenApply(x -> true);
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
                    log.warn("Exception thrown while deleting stream", ex.getMessage());
                    return handleDeleteStreamError(ex);
                });
    }

    private CompletableFuture<Boolean> isDeleted(String scope, String stream) {
        return streamMetadataStore.checkStreamExists(scope, stream)
                .thenApply(x -> !x);
    }

    /**
     * Helper method to perform scale operation against an scale request.
     * This method posts a request in the request stream and then starts the scale operation while
     * tracking its progress. Eventually, after scale completion, it sends a response to the caller.
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
                                                        List<AbstractMap.SimpleEntry<Double, Double>> newRanges, long scaleTimestamp,
                                                        OperationContext context) {
        ScaleOpEvent event = new ScaleOpEvent(scope, stream, segmentsToSeal, newRanges, true, scaleTimestamp);
        return writeEvent(event)
                .thenCompose(segmentsToBeSealed -> streamMetadataStore.startScale(scope, stream, segmentsToSeal, newRanges, scaleTimestamp, false,
                        context, executor)
                        .handle((startScaleResponse, e) -> {
                            ScaleResponse.Builder response = ScaleResponse.newBuilder();

                            if (e != null) {
                                Throwable cause = Exceptions.unwrap(e);
                                if (cause instanceof EpochTransitionOperationExceptions.PreConditionFailureException) {
                                    response.setStatus(ScaleResponse.ScaleStreamStatus.PRECONDITION_FAILED);
                                } else {
                                    log.warn("Scale for stream {}/{} failed with exception {}", scope, stream, cause);
                                    response.setStatus(ScaleResponse.ScaleStreamStatus.FAILURE);
                                }
                            } else {
                                log.info("scale for stream {}/{} started successfully", scope, stream);
                                response.setStatus(ScaleResponse.ScaleStreamStatus.STARTED);
                                response.addAllSegments(
                                        startScaleResponse.getNewSegmentsWithRange().entrySet()
                                                .stream()
                                                .map(segment -> convert(scope, stream, segment))
                                                .collect(Collectors.toList()));
                                response.setEpoch(startScaleResponse.getActiveEpoch());
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
     * @param context        optional context
     * @return returns the newly created segments.
     */
    public CompletableFuture<ScaleStatusResponse> checkScale(String scope, String stream, int epoch,
                                                                        OperationContext context) {
        return streamMetadataStore.getActiveEpoch(scope, stream, context, true, executor)
                        .handle((activeEpoch, ex) -> {
                            ScaleStatusResponse.Builder response = ScaleStatusResponse.newBuilder();

                            if (ex != null) {
                                Throwable e = Exceptions.unwrap(ex);
                                if (e instanceof StoreException.DataNotFoundException) {
                                    response.setStatus(ScaleStatusResponse.ScaleStatus.INVALID_INPUT);
                                } else {
                                    response.setStatus(ScaleStatusResponse.ScaleStatus.INTERNAL_ERROR);
                                }
                            } else {
                                Preconditions.checkNotNull(activeEpoch);

                                if (epoch > activeEpoch.getEpoch()) {
                                    response.setStatus(ScaleStatusResponse.ScaleStatus.INVALID_INPUT);
                                } else if (activeEpoch.getEpoch() == epoch || activeEpoch.getReferenceEpoch() == epoch) {
                                    response.setStatus(ScaleStatusResponse.ScaleStatus.IN_PROGRESS);
                                } else {
                                    response.setStatus(ScaleStatusResponse.ScaleStatus.SUCCESS);
                                }
                            }

                            return response.build();
                        });
    }

    public CompletableFuture<Void> writeEvent(ControllerEvent event) {
        CompletableFuture<Void> result = new CompletableFuture<>();

        getRequestWriter().writeEvent(event).whenComplete((r, e) -> {
            if (e != null) {
                log.warn("exception while posting event {} {}", e.getClass().getName(), e.getMessage());
                if (e instanceof TaskExceptions.ProcessingDisabledException) {
                    result.completeExceptionally(e);
                } else {
                    // transform any other event write exception to retryable exception
                    result.completeExceptionally(new TaskExceptions.PostEventException("Failed to post event", e));
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
    }

    private EventStreamWriter<ControllerEvent> getRequestWriter() {
        if (requestEventWriterRef.get() == null) {
            if (clientFactory == null || requestStreamName == null) {
                throw new TaskExceptions.ProcessingDisabledException("RequestProcessing not enabled");
            }

            requestEventWriterRef.set(clientFactory.createEventWriter(requestStreamName,
                    ControllerEventProcessors.CONTROLLER_EVENT_SERIALIZER,
                    EventWriterConfig.builder().build()));
        }

        return requestEventWriterRef.get();
    }

    @VisibleForTesting
    CompletableFuture<CreateStreamStatus.Status> createStreamBody(String scope, String stream,
                                                                          StreamConfiguration config, long timestamp) {
        return this.streamMetadataStore.createStream(scope, stream, config, timestamp, null, executor)
                .thenComposeAsync(response -> {
                    log.info("{}/{} created in metadata store", scope, stream);
                    CreateStreamStatus.Status status = translate(response.getStatus());
                    // only if its a new stream or an already existing non-active stream then we will create
                    // segments and change the state of the stream to active.
                    if (response.getStatus().equals(CreateStreamResponse.CreateStatus.NEW) ||
                            response.getStatus().equals(CreateStreamResponse.CreateStatus.EXISTS_CREATING)) {
                        List<Long> newSegments = IntStream.range(0, response.getConfiguration().getScalingPolicy()
                                .getMinNumSegments()).boxed().map(x -> StreamSegmentNameUtils.computeSegmentId(x, 0)).collect(Collectors.toList());
                        return notifyNewSegments(scope, stream, response.getConfiguration(), newSegments, this.retrieveDelegationToken())
                                .thenCompose(y -> {
                                    final OperationContext context = streamMetadataStore.createContext(scope, stream);

                                    return withRetries(() -> {
                                        CompletableFuture<Void> future;
                                        if (config.getRetentionPolicy() != null) {
                                            future = streamMetadataStore.addUpdateStreamForAutoStreamCut(scope, stream,
                                                    config.getRetentionPolicy(), context, executor);
                                        } else {
                                            future = CompletableFuture.completedFuture(null);
                                        }
                                        return future
                                                .thenCompose(v ->  streamMetadataStore.setState(scope, stream, State.ACTIVE,
                                                        context, executor));
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
                            log.warn("Create stream failed due to ", ex);
                            return CreateStreamStatus.Status.FAILURE;
                        }
                    } else {
                        return result;
                    }
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

    public CompletableFuture<Void> notifyNewSegments(String scope, String stream, List<Long> segmentIds, OperationContext context, String controllerToken) {
        return withRetries(() -> streamMetadataStore.getConfiguration(scope, stream, context, executor), executor)
                .thenCompose(configuration -> notifyNewSegments(scope, stream, configuration, segmentIds, controllerToken));
    }

    public CompletableFuture<Void> notifyNewSegments(String scope, String stream, StreamConfiguration configuration, List<Long> segmentIds, String controllerToken) {
        return Futures.toVoid(Futures.allOfWithResults(segmentIds
                .stream()
                .parallel()
                .map(segment -> notifyNewSegment(scope, stream, segment, configuration.getScalingPolicy(), controllerToken))
                .collect(Collectors.toList())));
    }

    public CompletableFuture<Void> notifyNewSegment(String scope, String stream, long segmentId, ScalingPolicy policy, String controllerToken) {
        return Futures.toVoid(withRetries(() -> segmentHelper.createSegment(scope,
                stream, segmentId, policy, hostControllerStore, this.connectionFactory, controllerToken), executor));
    }

    public CompletableFuture<Void> notifyDeleteSegments(String scope, String stream, Set<Long> segmentsToDelete, String delegationToken) {
        return Futures.allOf(segmentsToDelete.stream().parallel().map(segment -> notifyDeleteSegment(scope, stream, segment, delegationToken))
                                      .collect(Collectors.toList()));
    }

    public CompletableFuture<Void> notifyDeleteSegment(String scope, String stream, long segmentId, String delegationToken) {
        return Futures.toVoid(withRetries(() -> segmentHelper.deleteSegment(scope,
                stream, segmentId, hostControllerStore, this.connectionFactory, delegationToken), executor));
    }

    public CompletableFuture<Void> notifyTruncateSegment(String scope, String stream, Map.Entry<Long, Long> segmentCut, String delegationToken) {
        return Futures.toVoid(withRetries(() -> segmentHelper.truncateSegment(scope,
                stream, segmentCut.getKey(), segmentCut.getValue(), hostControllerStore, this.connectionFactory, delegationToken), executor));
    }

    public CompletableFuture<Map<Long, Long>> getSealedSegmentsSize(String scope, String stream, List<Long> sealedSegments, String delegationToken) {
        return Futures.allOfWithResults(
                sealedSegments
                        .stream()
                        .parallel()
                        .collect(Collectors.toMap(x -> x, x -> getSegmentOffset(scope, stream, x, delegationToken))));
    }

    public CompletableFuture<Void> notifySealedSegments(String scope, String stream, List<Long> sealedSegments, String delegationToken) {
        return Futures.allOf(
                sealedSegments
                        .stream()
                        .parallel()
                        .map(id -> notifySealedSegment(scope, stream, id, delegationToken))
                        .collect(Collectors.toList()));
    }

    private CompletableFuture<Void> notifySealedSegment(final String scope, final String stream, final long sealedSegment, String delegationToken) {
        return Futures.toVoid(withRetries(() -> segmentHelper.sealSegment(
                scope,
                stream,
                sealedSegment,
                hostControllerStore,
                this.connectionFactory, delegationToken), executor));
    }

    public CompletableFuture<Void> notifyPolicyUpdates(String scope, String stream, List<Segment> activeSegments,
                                                       ScalingPolicy policy, String delegationToken) {
        return Futures.toVoid(Futures.allOfWithResults(activeSegments
                .stream()
                .parallel()
                .map(segment -> notifyPolicyUpdate(scope, stream, policy, segment.segmentId(), delegationToken))
                .collect(Collectors.toList())));
    }

    private CompletableFuture<Long> getSegmentOffset(String scope, String stream, long segmentId, String delegationToken) {

        return withRetries(() -> segmentHelper.getSegmentInfo(
                scope,
                stream,
                segmentId,
                hostControllerStore,
                this.connectionFactory, delegationToken), executor)
                .thenApply(WireCommands.StreamSegmentInfo::getWriteOffset);
    }

    private CompletableFuture<Void> notifyPolicyUpdate(String scope, String stream, ScalingPolicy policy, long segmentId, String delegationToken) {
        return withRetries(() -> segmentHelper.updatePolicy(
                scope,
                stream,
                policy,
                segmentId,
                hostControllerStore,
                this.connectionFactory, delegationToken), executor);
    }

    private SegmentRange convert(String scope, String stream, Map.Entry<Long, AbstractMap.SimpleEntry<Double, Double>> segment) {
        return ModelHelper.createSegmentRange(scope, stream, segment.getKey(), segment.getValue().getKey(),
                segment.getValue().getValue());
    }

    private UpdateStreamStatus.Status handleUpdateStreamError(Throwable ex) {
        Throwable cause = Exceptions.unwrap(ex);
        if (cause instanceof StoreException.DataNotFoundException) {
            return UpdateStreamStatus.Status.STREAM_NOT_FOUND;
        } else {
            log.warn("Update stream failed due to ", cause);
            return UpdateStreamStatus.Status.FAILURE;
        }
    }

    private DeleteStreamStatus.Status handleDeleteStreamError(Throwable ex) {
        Throwable cause = Exceptions.unwrap(ex);
        if (cause instanceof StoreException.DataNotFoundException) {
            return DeleteStreamStatus.Status.STREAM_NOT_FOUND;
        } else {
            log.warn("Delete stream failed.", ex);
            return DeleteStreamStatus.Status.FAILURE;
        }
    }

    public CompletableFuture<Void> notifyTxnCommit(final String scope, final String stream,
                                                   final List<Long> segments, final UUID txnId) {
        return Futures.allOf(segments.stream()
                .parallel()
                .map(segment -> notifyTxnCommit(scope, stream, segment, txnId))
                .collect(Collectors.toList()));
    }

    private CompletableFuture<Controller.TxnStatus> notifyTxnCommit(final String scope, final String stream,
                                                                    final long segmentNumber, final UUID txnId) {
        return TaskStepsRetryHelper.withRetries(() -> segmentHelper.commitTransaction(scope,
                stream,
                segmentNumber,
                segmentNumber,
                txnId,
                this.hostControllerStore,
                this.connectionFactory, this.retrieveDelegationToken()), executor);
    }

    public CompletableFuture<Void> notifyTxnAbort(final String scope, final String stream,
                                                  final List<Long> segments, final UUID txnId) {
        return Futures.allOf(segments.stream()
                .parallel()
                .map(segment -> notifyTxnAbort(scope, stream, segment, txnId))
                .collect(Collectors.toList()));
    }

    private CompletableFuture<Controller.TxnStatus> notifyTxnAbort(final String scope, final String stream,
                                                                   final long segmentNumber, final UUID txnId) {
        return TaskStepsRetryHelper.withRetries(() -> segmentHelper.abortTransaction(scope,
                stream,
                segmentNumber,
                txnId,
                this.hostControllerStore,
                this.connectionFactory, this.retrieveDelegationToken()), executor);
    }

    @Override
    public TaskBase copyWithContext(Context context) {
        return new StreamMetadataTasks(streamMetadataStore,
                hostControllerStore,
                taskMetadataStore,
                segmentHelper,
                executor,
                context,
                connectionFactory,
                authEnabled,
                tokenSigningKey);
    }

    @Override
    public void close() throws Exception {
    }

    public String retrieveDelegationToken() {
        if (authEnabled) {
            return PravegaInterceptor.retrieveMasterToken(tokenSigningKey);
        } else {
            return "";
        }
    }
}

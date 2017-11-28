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
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.stream.CreateStreamResponse;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.ScaleOperationExceptions;
import io.pravega.controller.store.stream.Segment;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamCutRecord;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.store.task.Resource;
import io.pravega.controller.store.task.TaskMetadataStore;
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
import io.pravega.shared.protocol.netty.WireCommands;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;

import java.io.Serializable;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.pravega.controller.task.Stream.TaskStepsRetryHelper.withRetries;

/**
 * Collection of metadata update tasks on stream.
 * Task methods are annotated with @Task annotation.
 * <p>
 * Any update to the task method signature should be avoided, since it can cause problems during upgrade.
 * Instead, a new overloaded method may be created with the same task annotation name but a new version.
 */
@Slf4j
public class StreamMetadataTasks extends TaskBase {

    private final StreamMetadataStore streamMetadataStore;
    private final HostControllerStore hostControllerStore;
    private final ConnectionFactory connectionFactory;
    private final SegmentHelper segmentHelper;
    private ClientFactory clientFactory;
    private String requestStreamName;

    private final AtomicReference<EventStreamWriter<ControllerEvent>> requestEventWriterRef = new AtomicReference<>();

    public StreamMetadataTasks(final StreamMetadataStore streamMetadataStore,
                               final HostControllerStore hostControllerStore, final TaskMetadataStore taskMetadataStore,
                               final SegmentHelper segmentHelper, final ScheduledExecutorService executor, final String hostId,
                               final ConnectionFactory connectionFactory) {
        this(streamMetadataStore, hostControllerStore, taskMetadataStore, segmentHelper, executor, new Context(hostId),
                connectionFactory);
    }

    private StreamMetadataTasks(final StreamMetadataStore streamMetadataStore,
                                final HostControllerStore hostControllerStore, final TaskMetadataStore taskMetadataStore,
                                final SegmentHelper segmentHelper, final ScheduledExecutorService executor, final Context context,
                                ConnectionFactory connectionFactory) {
        super(taskMetadataStore, executor, context);
        this.streamMetadataStore = streamMetadataStore;
        this.hostControllerStore = hostControllerStore;
        this.segmentHelper = segmentHelper;
        this.connectionFactory = connectionFactory;
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
        return streamMetadataStore.getConfigurationProperty(scope, stream, true, context, executor)
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
        return streamMetadataStore.getConfigurationProperty(scope, stream, true, context, executor)
                .thenApply(configProperty -> !configProperty.isUpdating() || !configProperty.getProperty().equals(newConfig));
    }

    /**
     * Method to check retention policy and generate new periodic cuts and/or truncate stream at an existing stream cut.
     * 
     * @param scope scope
     * @param stream stream
     * @param policy retention policy
     * @param recordingTime time of recording
     * @param contextOpt operation context
     * @return future.
     */
    public CompletableFuture<Void> retention(final String scope, final String stream, final RetentionPolicy policy,
                                             final long recordingTime, final OperationContext contextOpt) {
        Preconditions.checkNotNull(policy);
        final OperationContext context = contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;

        return streamMetadataStore.getStreamCutsFromRetentionSet(scope, stream, context, executor)
                .thenCompose(retentionSet -> {
                    StreamCutRecord latestCut = retentionSet.stream()
                            .max(Comparator.comparingLong(StreamCutRecord::getRecordingTime)).orElse(null);
                    return checkGenerateStreamCut(scope, stream, context, policy, latestCut, recordingTime)
                            .thenCompose(x -> truncate(scope, stream, policy, context, retentionSet, recordingTime));
                });

    }

    private CompletableFuture<Void> checkGenerateStreamCut(String scope, String stream, OperationContext context,
                                                           RetentionPolicy policy, StreamCutRecord latestCut, long recordingTime) {
        switch (policy.getRetentionType()) {
            case TIME:
                if (latestCut == null || recordingTime - latestCut.getRecordingTime() > Duration.ofMinutes(Config.MINIMUM_RETENTION_FREQUENCY_IN_MINUTES).toMillis()) {
                    return generateStreamCut(scope, stream, context)
                            .thenCompose(newRecord ->
                                    streamMetadataStore.addStreamCutToRetentionSet(scope, stream, newRecord, context, executor));
                } else {
                    return  CompletableFuture.completedFuture(null);
                }
            case SIZE:
            default:
                throw new NotImplementedException("Size based retention");
        }
    }

    private CompletableFuture<Void> truncate(String scope, String stream, RetentionPolicy policy, OperationContext context,
                                             List<StreamCutRecord> retentionSet, long recordingTime) {
        return findTruncationRecord(policy, retentionSet, recordingTime)
                .map(record -> startTruncation(scope, stream, record.getStreamCut(), context)
                        .thenCompose(started -> {
                            if (started) {
                                return streamMetadataStore.deleteStreamCutBefore(scope, stream, record, context, executor);
                            } else {
                                throw new RuntimeException("Could not start truncation");
                            }
                        })
                ).orElse(CompletableFuture.completedFuture(null));
    }

    private Optional<StreamCutRecord> findTruncationRecord(RetentionPolicy policy, List<StreamCutRecord> retentionSet, long recordingTime) {
        switch (policy.getRetentionType()) {
            case TIME:
                return retentionSet.stream().filter(x -> x.getRecordingTime() < recordingTime - policy.getRetentionParam())
                        .max(Comparator.comparingLong(StreamCutRecord::getRecordingTime));
            case SIZE:
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
     * @return streamCut.
     */
    public CompletableFuture<StreamCutRecord> generateStreamCut(final String scope, final String stream,
                                                                final OperationContext contextOpt) {
        final OperationContext context = contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;

        final long generationTime = System.currentTimeMillis();
        return streamMetadataStore.getActiveSegments(scope, stream, context, executor)
                .thenCompose(activeSegments -> Futures.allOfWithResults(activeSegments
                        .stream()
                        .parallel()
                        .collect(Collectors.toMap(Segment::getNumber, x -> getSegmentOffset(scope, stream, x.getNumber())))))
                .thenApply(map -> new StreamCutRecord(generationTime, Long.MIN_VALUE, ImmutableMap.copyOf(map)));
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
                                                                       final Map<Integer, Long> streamCut,
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

    private CompletableFuture<Boolean> startTruncation(String scope, String stream, Map<Integer, Long> streamCut, OperationContext contextOpt) {
        final OperationContext context = contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;

        return streamMetadataStore.getTruncationProperty(scope, stream, true, context, executor)
                .thenCompose(property -> {
                    if (!property.isUpdating()) {
                        // 2. post event with new stream cut if no truncation is ongoing
                        return writeEvent(new TruncateStreamEvent(scope, stream))
                                // 3. start truncation by updating the metadata
                                .thenCompose(x -> streamMetadataStore.startTruncation(scope, stream, streamCut,
                                        context, executor))
                                .thenApply(x -> true);
                    } else {
                        log.warn("Another truncation in progress for {}/{}", scope, stream);
                        return CompletableFuture.completedFuture(false);
                    }
                });
    }

    private CompletableFuture<Boolean> isTruncated(String scope, String stream, Map<Integer, Long> streamCut, OperationContext context) {
        return streamMetadataStore.getTruncationProperty(scope, stream, true, context, executor)
                .thenApply(truncationProp -> !truncationProp.isUpdating() || !truncationProp.getProperty().getStreamCut().equals(streamCut));
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
    public CompletableFuture<ScaleResponse> manualScale(String scope, String stream, List<Integer> segmentsToSeal,
                                                        List<AbstractMap.SimpleEntry<Double, Double>> newRanges, long scaleTimestamp,
                                                        OperationContext context) {
        ScaleOpEvent event = new ScaleOpEvent(scope, stream, segmentsToSeal, newRanges, true, scaleTimestamp);
        return writeEvent(event).thenCompose(x ->
                streamMetadataStore.startScale(scope, stream, segmentsToSeal, newRanges, scaleTimestamp, false,
                        context, executor)
                        .handle((startScaleResponse, e) -> {
                            ScaleResponse.Builder response = ScaleResponse.newBuilder();

                            if (e != null) {
                                Throwable cause = Exceptions.unwrap(e);
                                if (cause instanceof ScaleOperationExceptions.ScalePreConditionFailureException) {
                                    response.setStatus(ScaleResponse.ScaleStreamStatus.PRECONDITION_FAILED);
                                } else {
                                    log.warn("Scale for stream {}/{} failed with exception {}", scope, stream, cause);
                                    response.setStatus(ScaleResponse.ScaleStreamStatus.FAILURE);
                                }
                            } else {
                                log.info("scale for stream {}/{} started successfully", scope, stream);
                                response.setStatus(ScaleResponse.ScaleStreamStatus.STARTED);
                                response.addAllSegments(
                                        startScaleResponse.getSegmentsCreated()
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

                                if (epoch > activeEpoch.getKey()) {
                                    response.setStatus(ScaleStatusResponse.ScaleStatus.INVALID_INPUT);
                                } else if (activeEpoch.getKey() == epoch) {
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

    /**
     * Method to start scale operation. This creates new segments and then creates partial record in history table.
     * After that it optimistically calls tryCompleteScale.
     * Complete scale completes if there are no ongoing transactions on older epoch.
     * This method is called from both scale request handler and manual scale.
     * This takes a parameter called runOnlyIfStarted. The parameter is set from manual scale requests.
     * For autoscale, this is always false which means when a scale event is received on scale request event processor, it wil
     * be processed.
     * However, if a scale operation is started as part of a manual scale request, we want to make sure that manual scale operation
     * does not get stuck in an incomplete state and hence we also post a request for its processing in scale event stream.
     * However we want to process the scale request inline with the callers call so that we can send the response. And we want to
     * make sure that we dont do any processing on the scale request if caller may have responded with some pre condition failure.
     * So we send this flag to the event processor to process the scale request only if it was already started. Otherwise ignore.
     *
     * @param scaleInput scale input
     * @param runOnlyIfStarted run only if the scale operation was already running. It will ignore requests if the operation isnt started
     *                         by the caller.
     * @param context operation context
     * @return returns list of new segments created as part of this scale operation.
     */
    public CompletableFuture<List<Segment>> startScale(ScaleOpEvent scaleInput, boolean runOnlyIfStarted, OperationContext context) { // called upon event read from requeststream
        return withRetries(() -> streamMetadataStore.startScale(scaleInput.getScope(),
                scaleInput.getStream(),
                scaleInput.getSegmentsToSeal(),
                scaleInput.getNewRanges(),
                scaleInput.getScaleTime(),
                runOnlyIfStarted,
                context,
                executor), executor)
                .thenCompose(response -> streamMetadataStore.setState(scaleInput.getScope(), scaleInput.getStream(), State.SCALING, context, executor)
                        .thenApply(updated -> response))
                .thenCompose(response -> notifyNewSegments(scaleInput.getScope(), scaleInput.getStream(), response.getSegmentsCreated(), context)
                        .thenCompose(x -> {
                            assert !response.getSegmentsCreated().isEmpty();

                            long scaleTs = response.getSegmentsCreated().get(0).getStart();

                            return withRetries(() -> streamMetadataStore.scaleNewSegmentsCreated(scaleInput.getScope(), scaleInput.getStream(),
                                    scaleInput.getSegmentsToSeal(), response.getSegmentsCreated(), response.getActiveEpoch(),
                                    scaleTs, context, executor), executor);
                        })
                        .thenCompose(x -> tryCompleteScale(scaleInput.getScope(), scaleInput.getStream(), response.getActiveEpoch(), context))
                        .thenApply(y -> response.getSegmentsCreated()));
    }

    /**
     * Helper method to complete scale operation. It tries to optimistically complete the scale operation if no transaction is running
     * against previous epoch. If so, it will proceed to seal old segments and then complete partial metadata records.
     * @param scope scope
     * @param stream stream
     * @param epoch epoch
     * @param context operation context
     * @return returns true if it was able to complete scale. false otherwise
     */
    public CompletableFuture<Boolean> tryCompleteScale(String scope, String stream, int epoch, OperationContext context) {
        // Note: if we cant delete old epoch -- txns against old segments are ongoing..
        // if we can delete old epoch, then only do we proceed to subsequent steps
        return withRetries(() -> streamMetadataStore.tryDeleteEpochIfScaling(scope, stream, epoch, context, executor), executor)
                .thenCompose(response -> {
                    if (!response.isDeleted()) {
                        return CompletableFuture.completedFuture(false);
                    }
                    assert !response.getSegmentsCreated().isEmpty() && !response.getSegmentsSealed().isEmpty();

                    long scaleTs = response.getSegmentsCreated().get(0).getStart();
                    return notifySealedSegments(scope, stream, response.getSegmentsSealed())
                            .thenCompose(y ->
                                    withRetries(() -> streamMetadataStore.scaleSegmentsSealed(scope, stream, response.getSegmentsSealed(),
                                            response.getSegmentsCreated(), epoch, scaleTs, context, executor), executor)
                                    .thenApply(z -> {
                                        log.info("scale processing for {}/{} epoch {} completed.", scope, stream, epoch);
                                        return true;
                                    }));
                });
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
                        List<Integer> newSegments = IntStream.range(0, response.getConfiguration().getScalingPolicy()
                                .getMinNumSegments()).boxed().collect(Collectors.toList());
                        return notifyNewSegments(scope, stream, response.getConfiguration(), newSegments)
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

    private CompletableFuture<Void> notifyNewSegments(String scope, String stream, List<Segment> segmentNumbers, OperationContext context) {
        return withRetries(() -> streamMetadataStore.getConfiguration(scope, stream, context, executor), executor)
                .thenCompose(configuration -> notifyNewSegments(scope, stream, configuration,
                        segmentNumbers.stream().map(Segment::getNumber).collect(Collectors.toList())));
    }

    private CompletableFuture<Void> notifyNewSegments(String scope, String stream, StreamConfiguration configuration, List<Integer> segmentNumbers) {
        return Futures.toVoid(Futures.allOfWithResults(segmentNumbers
                .stream()
                .parallel()
                .map(segment -> notifyNewSegment(scope, stream, segment, configuration.getScalingPolicy()))
                .collect(Collectors.toList())));
    }

    private CompletableFuture<Void> notifyNewSegment(String scope, String stream, int segmentNumber, ScalingPolicy policy) {
        return Futures.toVoid(withRetries(() -> segmentHelper.createSegment(scope,
                stream, segmentNumber, policy, hostControllerStore, this.connectionFactory), executor));
    }

    public CompletableFuture<Void> notifyDeleteSegments(String scope, String stream, int count) {
        return Futures.allOf(IntStream.range(0, count)
                                      .parallel()
                                      .mapToObj(segment -> notifyDeleteSegment(scope, stream, segment))
                                      .collect(Collectors.toList()));
    }

    public CompletableFuture<Void> notifyDeleteSegment(String scope, String stream, int segmentNumber) {
        return Futures.toVoid(withRetries(() -> segmentHelper.deleteSegment(scope,
                stream, segmentNumber, hostControllerStore, this.connectionFactory), executor));
    }

    public CompletableFuture<Void> notifyTruncateSegment(String scope, String stream, Map.Entry<Integer, Long> segmentCut) {
        return Futures.toVoid(withRetries(() -> segmentHelper.truncateSegment(scope,
                stream, segmentCut.getKey(), segmentCut.getValue(), hostControllerStore, this.connectionFactory), executor));
    }

    public CompletableFuture<Void> notifySealedSegments(String scope, String stream, List<Integer> sealedSegments) {
        return Futures.allOf(
                sealedSegments
                        .stream()
                        .parallel()
                        .map(number -> notifySealedSegment(scope, stream, number))
                        .collect(Collectors.toList()));
    }

    private CompletableFuture<Void> notifySealedSegment(final String scope, final String stream, final int sealedSegment) {

        return Futures.toVoid(withRetries(() -> segmentHelper.sealSegment(
                scope,
                stream,
                sealedSegment,
                hostControllerStore,
                this.connectionFactory), executor));
    }

    public CompletableFuture<Void> notifyPolicyUpdates(String scope, String stream, List<Segment> activeSegments,
                                                        ScalingPolicy policy) {
        return Futures.toVoid(Futures.allOfWithResults(activeSegments
                .stream()
                .parallel()
                .map(segment -> notifyPolicyUpdate(scope, stream, policy, segment.getNumber()))
                .collect(Collectors.toList())));
    }

    private CompletableFuture<Long> getSegmentOffset(String scope, String stream, int segmentNumber) {

        return withRetries(() -> segmentHelper.getSegmentInfo(
                scope,
                stream,
                segmentNumber,
                hostControllerStore,
                this.connectionFactory), executor)
                .thenApply(WireCommands.StreamSegmentInfo::getSegmentLength);
    }

    private CompletableFuture<Void> notifyPolicyUpdate(String scope, String stream, ScalingPolicy policy, int segmentNumber) {

        return withRetries(() -> segmentHelper.updatePolicy(
                scope,
                stream,
                policy,
                segmentNumber,
                hostControllerStore,
                this.connectionFactory), executor);
    }

    private SegmentRange convert(String scope, String stream, Segment segment) {

        return ModelHelper.createSegmentRange(scope, stream, segment.getNumber(), segment.getKeyEnd(),
                segment.getKeyEnd());
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

    @Override
    public TaskBase copyWithContext(Context context) {
        return new StreamMetadataTasks(streamMetadataStore,
                hostControllerStore,
                taskMetadataStore,
                segmentHelper,
                executor,
                context,
                connectionFactory);
    }

    @Override
    public void close() throws Exception {
    }
}

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
import io.pravega.client.ClientFactory;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ModelHelper;
import io.pravega.common.ExceptionHelpers;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.eventProcessor.ControllerEventProcessors;
import io.pravega.controller.server.eventProcessor.ScaleOpEvent;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.stream.CreateStreamResponse;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.ScaleOperationExceptions;
import io.pravega.controller.store.stream.Segment;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.controller.store.task.Resource;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentRange;
import io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus;
import io.pravega.controller.task.Task;
import io.pravega.controller.task.TaskBase;
import io.pravega.shared.controller.event.ControllerEvent;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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
     * @param config     modified stream configuration.
     * @param contextOpt optional context
     * @return update status.
     */
    @Task(name = "updateConfig", version = "1.0", resource = "{scope}/{stream}")
    public CompletableFuture<UpdateStreamStatus.Status> updateStream(String scope, String stream, StreamConfiguration config, OperationContext contextOpt) {
        return execute(
                new Resource(scope, stream),
                new Serializable[]{scope, stream, config, null},
                () -> updateStreamConfigBody(scope, stream, config, contextOpt));
    }

    /**
     * Seal a stream.
     *
     * @param scope      scope.
     * @param stream     stream name.
     * @param contextOpt optional context
     * @return update status.
     */
    @Task(name = "sealStream", version = "1.0", resource = "{scope}/{stream}")
    public CompletableFuture<UpdateStreamStatus.Status> sealStream(String scope, String stream, OperationContext contextOpt) {
        return execute(
                new Resource(scope, stream),
                new Serializable[]{scope, stream, null},
                () -> sealStreamBody(scope, stream, contextOpt));
    }

    /**
     * Delete a stream. Precondition for deleting a stream is that the stream sholud be sealed.
     *
     * @param scope      scope.
     * @param stream     stream name.
     * @param contextOpt optional context
     * @return delete status.
     */
    @Task(name = "deleteStream", version = "1.0", resource = "{scope}/{stream}")
    public CompletableFuture<DeleteStreamStatus.Status> deleteStream(final String scope, final String stream,
                                                                     final OperationContext contextOpt) {
        return execute(
                new Resource(scope, stream),
                new Serializable[]{scope, stream, null},
                () -> deleteStreamBody(scope, stream, contextOpt));
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
        return postScale(event).thenCompose(x ->
                streamMetadataStore.startScale(scope, stream, segmentsToSeal, newRanges, scaleTimestamp, false,
                        context, executor)
                        .thenComposeAsync(startScaleResponse -> {
                            AtomicBoolean scaling = new AtomicBoolean(true);
                            return FutureHelpers.loop(scaling::get, () ->
                                    streamMetadataStore.getState(scope, stream, null, executor)
                                            .thenAccept(state -> scaling.set(state.equals(State.SCALING))), executor)
                                    .thenApply(r -> startScaleResponse);
                        })
                        .handle((startScaleResponse, e) -> {
                            ScaleResponse.Builder response = ScaleResponse.newBuilder();

                            if (e != null) {
                                Throwable cause = ExceptionHelpers.getRealException(e);
                                if (cause instanceof ScaleOperationExceptions.ScalePreConditionFailureException) {
                                    response.setStatus(ScaleResponse.ScaleStreamStatus.PRECONDITION_FAILED);
                                } else {
                                    response.setStatus(ScaleResponse.ScaleStreamStatus.FAILURE);
                                }
                            } else {
                                response.setStatus(ScaleResponse.ScaleStreamStatus.SUCCESS);
                                response.addAllSegments(
                                        startScaleResponse.getSegmentsCreated()
                                                .stream()
                                                .map(segment -> convert(scope, stream, segment))
                                                .collect(Collectors.toList()));
                            }
                            return response.build();
                        }));
    }

    /**
     * Method to post scale operation requests in the request stream.
     *
     * @param event scale operation event
     * @return returns the newly created segments.
     */
    public CompletableFuture<Void> postScale(ScaleOpEvent event) {
        // if we are unable to post, throw a retryable exception.
        CompletableFuture<Void> result = new CompletableFuture<>();

        CompletableFuture.runAsync(() -> {
            try {
                getRequestWriter().writeEvent(event).get();
                result.complete(null);
            } catch (Exception e) {
                log.error("error sending request to requeststream {}", e);
                if (e instanceof ScaleOperationExceptions.ScaleRequestNotEnabledException) {
                    result.completeExceptionally(e);
                } else {
                    result.completeExceptionally(new ScaleOperationExceptions.ScalePostException());
                }
            }
        }, executor);
        return result;
    }

    @VisibleForTesting
    public void setRequestEventWriter(EventStreamWriter<ControllerEvent> requestEventWriter) {
        requestEventWriterRef.set(requestEventWriter);
    }

    private EventStreamWriter<ControllerEvent> getRequestWriter() {
        if (requestEventWriterRef.get() == null) {
            if (clientFactory == null || requestStreamName == null) {
                throw new ScaleOperationExceptions.ScaleRequestNotEnabledException();
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
                        return CompletableFuture.completedFuture(true);
                    }
                    assert !response.getSegmentsCreated().isEmpty() && !response.getSegmentsSealed().isEmpty();

                    long scaleTs = response.getSegmentsCreated().get(0).getStart();
                    return notifySealedSegments(scope, stream, response.getSegmentsSealed())
                            .thenCompose(y ->
                                    withRetries(() -> streamMetadataStore.scaleSegmentsSealed(scope, stream, response.getSegmentsSealed(),
                                            response.getSegmentsCreated(), epoch, scaleTs, context, executor), executor)
                                    .thenApply(z -> true));
                });
    }

    private CompletableFuture<CreateStreamStatus.Status> createStreamBody(String scope, String stream,
                                                                          StreamConfiguration config, long timestamp) {
        return this.streamMetadataStore.createStream(scope, stream, config, timestamp, null, executor)
                .thenComposeAsync(response -> {
                    log.debug("{}/{} created in metadata store", scope, stream);
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

                                    return withRetries(() -> streamMetadataStore.setState(scope,
                                            stream, State.ACTIVE, context, executor), executor)
                                            .thenApply(z -> status);
                                });
                    } else {
                        return CompletableFuture.completedFuture(status);
                    }
                }, executor)
                .handle((result, ex) -> {
                    if (ex != null) {
                        Throwable cause = ExceptionHelpers.getRealException(ex);
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

    private CompletableFuture<UpdateStreamStatus.Status> updateStreamConfigBody(String scope, String stream,
                                                                                StreamConfiguration config, OperationContext contextOpt) {
        final OperationContext context = contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;

        return streamMetadataStore.updateConfiguration(scope, stream, config, context, executor)
                .thenCompose(updated -> {
                    log.debug("{}/{} created in metadata store", scope, stream);
                    if (updated) {
                        // we are at a point of no return. Metadata has been updated, we need to notify hosts.
                        // wrap subsequent steps in retries.
                        return withRetries(() -> streamMetadataStore.getActiveSegments(scope, stream, context, executor), executor)
                                .thenCompose(activeSegments -> notifyPolicyUpdates(config.getScope(), stream, activeSegments, config.getScalingPolicy()))
                                .handle((res, ex) -> {
                                    if (ex == null) {
                                        return true;
                                    } else {
                                        throw new CompletionException(ex);
                                    }
                                });
                    } else {
                        return CompletableFuture.completedFuture(false);
                    }
                }).thenCompose(x -> streamMetadataStore.setState(scope, stream, State.ACTIVE, context, executor))
                .handle((result, ex) -> {
                    if (ex != null) {
                        return handleUpdateStreamError(ex);
                    } else {
                        return result ? UpdateStreamStatus.Status.SUCCESS
                                : UpdateStreamStatus.Status.FAILURE;
                    }
                });
    }

    CompletableFuture<UpdateStreamStatus.Status> sealStreamBody(String scope, String stream, OperationContext contextOpt) {
        final OperationContext context = contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;

        return withRetries(() -> streamMetadataStore.getState(scope, stream, context, executor)
                .thenCompose(state -> {
                    if (!state.equals(State.SEALED)) {
                        return streamMetadataStore.setState(scope, stream, State.SEALING, context, executor);
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                })
                .thenCompose(x -> streamMetadataStore.getActiveSegments(scope, stream, context, executor)), executor)
                .thenCompose(activeSegments -> {
                    if (activeSegments.isEmpty()) { //if active segments are empty then the stream is sealed.
                        //Do not update the state if the stream is already sealed.
                        return CompletableFuture.completedFuture(UpdateStreamStatus.Status.SUCCESS);
                    } else {
                        List<Integer> segmentsToBeSealed = activeSegments.stream().map(Segment::getNumber).
                                collect(Collectors.toList());
                        return notifySealedSegments(scope, stream, segmentsToBeSealed)
                                .thenCompose(v -> withRetries(() ->
                                        streamMetadataStore.setSealed(scope, stream, context, executor), executor))
                                .handle((result, ex) -> {
                                    if (ex != null) {
                                        log.warn("Exception thrown in trying to notify sealed segments {}", ex.getMessage());
                                        return handleUpdateStreamError(ex);
                                    } else {
                                        return result ? UpdateStreamStatus.Status.SUCCESS
                                                : UpdateStreamStatus.Status.FAILURE;
                                    }
                                });
                    }
                })
                .exceptionally(this::handleUpdateStreamError);
    }

    CompletableFuture<DeleteStreamStatus.Status> deleteStreamBody(final String scope, final String stream,
                                                                  final OperationContext contextOpt) {
        return withRetries(() -> streamMetadataStore.isSealed(scope, stream, contextOpt, executor), executor)
                .thenComposeAsync(sealed -> {
                    if (!sealed) {
                        return CompletableFuture.completedFuture(DeleteStreamStatus.Status.STREAM_NOT_SEALED);
                    }
                    return withRetries(
                            () -> streamMetadataStore.getSegmentCount(scope, stream, contextOpt, executor), executor)
                            .thenComposeAsync(count ->
                                    notifyDeleteSegments(scope, stream, count)
                                            .thenComposeAsync(x -> withRetries(() ->
                                                    streamMetadataStore.deleteStream(scope, stream, contextOpt,
                                                            executor), executor), executor)
                                            .handleAsync((result, ex) -> {
                                                if (ex != null) {
                                                    log.warn("Exception thrown while deleting stream", ex.getMessage());
                                                    return handleDeleteStreamError(ex);
                                                } else {
                                                    return DeleteStreamStatus.Status.SUCCESS;
                                                }
                                            }, executor), executor);
                }, executor).exceptionally(this::handleDeleteStreamError);
    }

    private CompletableFuture<Void> notifyNewSegments(String scope, String stream, List<Segment> segmentNumbers, OperationContext context) {
        return withRetries(() -> streamMetadataStore.getConfiguration(scope, stream, context, executor), executor)
                .thenCompose(configuration -> notifyNewSegments(scope, stream, configuration,
                        segmentNumbers.stream().map(Segment::getNumber).collect(Collectors.toList())));
    }

    private CompletableFuture<Void> notifyNewSegments(String scope, String stream, StreamConfiguration configuration, List<Integer> segmentNumbers) {
        return FutureHelpers.toVoid(FutureHelpers.allOfWithResults(segmentNumbers
                .stream()
                .parallel()
                .map(segment -> notifyNewSegment(scope, stream, segment, configuration.getScalingPolicy()))
                .collect(Collectors.toList())));
    }

    private CompletableFuture<Void> notifyNewSegment(String scope, String stream, int segmentNumber, ScalingPolicy policy) {
        return FutureHelpers.toVoid(withRetries(() -> segmentHelper.createSegment(scope,
                stream, segmentNumber, policy, hostControllerStore, this.connectionFactory), executor));
    }

    private CompletableFuture<Void> notifyDeleteSegments(String scope, String stream, int count) {
        return FutureHelpers.allOf(IntStream.range(0, count)
                .parallel()
                .mapToObj(segment -> notifyDeleteSegment(scope, stream, segment))
                .collect(Collectors.toList()));
    }

    private CompletableFuture<Void> notifyDeleteSegment(String scope, String stream, int segmentNumber) {
        return FutureHelpers.toVoid(withRetries(() -> segmentHelper.deleteSegment(scope,
                stream, segmentNumber, hostControllerStore, this.connectionFactory), executor));
    }

    private CompletableFuture<Void> notifySealedSegments(String scope, String stream, List<Integer> sealedSegments) {
        return FutureHelpers.allOf(
                sealedSegments
                        .stream()
                        .parallel()
                        .map(number -> notifySealedSegment(scope, stream, number))
                        .collect(Collectors.toList()));
    }

    private CompletableFuture<Void> notifySealedSegment(final String scope, final String stream, final int sealedSegment) {

        return FutureHelpers.toVoid(withRetries(() -> segmentHelper.sealSegment(
                scope,
                stream,
                sealedSegment,
                hostControllerStore,
                this.connectionFactory), executor));
    }

    private CompletableFuture<Void> notifyPolicyUpdates(String scope, String stream, List<Segment> activeSegments,
                                                        ScalingPolicy policy) {
        return FutureHelpers.toVoid(FutureHelpers.allOfWithResults(activeSegments
                .stream()
                .parallel()
                .map(segment -> notifyPolicyUpdate(scope, stream, policy, segment.getNumber()))
                .collect(Collectors.toList())));
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
        Throwable cause = ExceptionHelpers.getRealException(ex);
        if (cause instanceof StoreException.DataNotFoundException) {
            return UpdateStreamStatus.Status.STREAM_NOT_FOUND;
        } else {
            log.warn("Update stream failed due to ", cause);
            return UpdateStreamStatus.Status.FAILURE;
        }
    }

    private DeleteStreamStatus.Status handleDeleteStreamError(Throwable ex) {
        Throwable cause = ExceptionHelpers.getRealException(ex);
        if (cause instanceof StoreException.DataNotFoundException) {
            return DeleteStreamStatus.Status.STREAM_NOT_FOUND;
        } else {
            log.warn("Update stream failed.", ex);
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

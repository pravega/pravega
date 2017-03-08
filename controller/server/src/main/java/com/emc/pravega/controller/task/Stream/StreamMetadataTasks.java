/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.task.Stream;

import com.emc.pravega.common.ExceptionHelpers;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.controller.server.SegmentHelper;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.DataNotFoundException;
import com.emc.pravega.controller.store.stream.OperationContext;
import com.emc.pravega.controller.store.stream.Segment;
import com.emc.pravega.controller.store.stream.StoreException;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.tables.State;
import com.emc.pravega.controller.store.task.Resource;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.SegmentRange;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus;
import com.emc.pravega.controller.task.Task;
import com.emc.pravega.controller.task.TaskBase;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.ModelHelper;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.emc.pravega.controller.store.stream.StoreException.Type.NODE_EXISTS;
import static com.emc.pravega.controller.store.stream.StoreException.Type.NODE_NOT_FOUND;
import static com.emc.pravega.controller.task.Stream.TaskStepsRetryHelper.withRetries;

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
    private final ConnectionFactoryImpl connectionFactory;
    private final SegmentHelper segmentHelper;

    public StreamMetadataTasks(final StreamMetadataStore streamMetadataStore,
                               final HostControllerStore hostControllerStore,
                               final TaskMetadataStore taskMetadataStore,
                               final SegmentHelper segmentHelper,
                               final ScheduledExecutorService executor,
                               final String hostId) {
        this(streamMetadataStore, hostControllerStore, taskMetadataStore, segmentHelper, executor, new Context(hostId));
    }

    private StreamMetadataTasks(final StreamMetadataStore streamMetadataStore,
                                final HostControllerStore hostControllerStore,
                                final TaskMetadataStore taskMetadataStore,
                                final SegmentHelper segmentHelper,
                                final ScheduledExecutorService executor,
                                final Context context) {
        super(taskMetadataStore, executor, context);
        this.streamMetadataStore = streamMetadataStore;
        this.hostControllerStore = hostControllerStore;
        this.segmentHelper = segmentHelper;
        connectionFactory = new ConnectionFactoryImpl(false);
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
                new Serializable[]{scope, stream, config, createTimestamp, null},
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
    public CompletableFuture<UpdateStreamStatus.Status> alterStream(String scope, String stream, StreamConfiguration config, OperationContext contextOpt) {
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
     * Scales stream segments.
     *
     * @param scope          scope.
     * @param stream         stream name.
     * @param sealedSegments segments to be sealed.
     * @param newRanges      key ranges for new segments.
     * @param scaleTimestamp scaling time stamp.
     * @param contextOpt     optional context
     * @return returns the newly created segments.
     */
    @Task(name = "scaleStream", version = "1.0", resource = "{scope}/{stream}")
    public CompletableFuture<ScaleResponse> scale(String scope, String stream, ArrayList<Integer> sealedSegments,
            ArrayList<AbstractMap.SimpleEntry<Double, Double>> newRanges, long scaleTimestamp,
            OperationContext contextOpt) {
        return execute(
                new Resource(scope, stream),
                new Serializable[]{scope, stream, sealedSegments, newRanges, scaleTimestamp, null},
                () -> scaleBody(scope, stream, sealedSegments, newRanges, scaleTimestamp, contextOpt));
    }

    private CompletableFuture<CreateStreamStatus.Status> createStreamBody(String scope, String stream,
            StreamConfiguration config, long timestamp) {
        if (!validateName(stream)) {
            log.debug("Create stream failed due to invalid stream name {}", stream);
            return CompletableFuture.completedFuture(CreateStreamStatus.Status.INVALID_STREAM_NAME);
        } else {
            return this.streamMetadataStore.createStream(scope, stream, config, timestamp, null, executor)
                    .thenComposeAsync(created -> {
                        log.debug("{}/{} created in metadata store", scope, stream);
                        if (created) {
                            List<Integer> newSegments = IntStream.range(0, config.getScalingPolicy().getMinNumSegments()).boxed().collect(Collectors.toList());
                            return notifyNewSegments(config.getScope(), stream, config, newSegments)
                                    .thenApply(y -> CreateStreamStatus.Status.SUCCESS);
                        } else {
                            return CompletableFuture.completedFuture(CreateStreamStatus.Status.FAILURE);
                        }
                    }, executor)
                    .thenCompose(status -> {
                        if (status == CreateStreamStatus.Status.FAILURE) {
                            return CompletableFuture.completedFuture(status);
                        } else {
                            final OperationContext context = streamMetadataStore.createContext(scope, stream);

                            return withRetries(() -> streamMetadataStore.setState(scope,
                                    stream, State.ACTIVE, context, executor), executor)
                                    .thenApply(v -> status);
                        }
                    })
                    .handle((result, ex) -> {
                        if (ex != null) {
                            Throwable cause = ExceptionHelpers.getRealException(ex);
                            if (cause instanceof StoreException && ((StoreException) ex.getCause()).getType() == NODE_EXISTS) {
                                return CreateStreamStatus.Status.STREAM_EXISTS;
                            } else if (ex.getCause() instanceof StoreException && ((StoreException) ex.getCause()).getType() == NODE_NOT_FOUND) {
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
    }

    private static boolean validateName(final String path) {
        return (path.indexOf('\\') >= 0 || path.indexOf('/') >= 0) ? false : true;
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
                })
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

        return withRetries(() -> streamMetadataStore.getActiveSegments(scope, stream, context, executor), executor)
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
                }).exceptionally(this::handleUpdateStreamError);
    }

    CompletableFuture<ScaleResponse> scaleBody(final String scope, final String stream, final List<Integer> segmentsToSeal,
                                               final List<AbstractMap.SimpleEntry<Double, Double>> newRanges, final long scaleTimestamp,
                                               final OperationContext contextOpt) {
        // Abort scaling operation in the following error scenarios
        // 1. if the active segments in the stream have ts greater than scaleTimestamp -- ScaleStreamStatus.PRECONDITION_FAILED
        // 2. if active segments having creation timestamp as scaleTimestamp have different key ranges than the ones specified
        // in newRanges
        // 3. Transaction is active on the stream
        // 4. sealedSegments should be a subset of activeSegments.
        //
        // If there is intermittent network issue before during precondition check (e.g. for metadata store reads) we will throw
        // exception and fail the task.
        // However, once preconditions pass and scale task starts, all steps are wrapped inside Retry block
        // with exponential back offs. We will have significant number of retries (default: 100).
        // This is because we dont have roll backs for scale operations. So once started, we should try to complete it by
        // retrying against all intermittent failures.
        // Also, we cant leave with intermediate failures as the system state will be inconsistent -
        // for example: existing segments are sealed, but we are not able to create new segments in metadata store.
        // So we need to retry and complete all steps.
        // However, after sufficient retries, if we are still not able to complete all steps in scale task,
        // we should stop retrying indefinitely and notify administrator.
        final OperationContext context = contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;

        CompletableFuture<Boolean> checkValidity = withRetries(
                () -> streamMetadataStore.getActiveSegments(scope, stream, context, executor), executor)
                .thenApply(activeSegments -> {
                    boolean result = false;
                    Set<Integer> activeNum = activeSegments.stream().mapToInt(Segment::getNumber).boxed().collect(Collectors.toSet());
                    if (activeNum.containsAll(segmentsToSeal)) {
                        result = true;
                    } else if (activeSegments.size() > 0 && activeSegments
                            .stream()
                            .mapToLong(Segment::getStart)
                            .max()
                            .getAsLong() == scaleTimestamp) {
                        result = true;
                    }
                    return result;
                });

        return checkValidity.thenCompose(valid -> {
            if (valid) {
                // keep checking until no transactions are running.
                CompletableFuture<Boolean> check = new CompletableFuture<>();
                FutureHelpers.loop(() -> !check.isDone(), () -> withRetries(() -> streamMetadataStore.isTransactionOngoing(scope, stream, context, executor)
                        .thenAccept(txnOngoing -> {
                            if (!txnOngoing) {
                                check.complete(true);
                            }
                        }), executor), executor);
                return check;
            } else {
                return CompletableFuture.completedFuture(false);
            }
        }).thenCompose(valid -> {
                    if (valid) {
                        return notifySealedSegments(scope, stream, segmentsToSeal)
                                .thenCompose(results -> withRetries(
                                        () -> streamMetadataStore.scale(scope, stream,
                                                segmentsToSeal, newRanges, scaleTimestamp, context, executor), executor))
                                .thenCompose((List<Segment> newSegments) -> notifyNewSegments(scope, stream, newSegments, context)
                                        .thenApply((Void v) -> newSegments))
                                .thenApply((List<Segment> newSegments) -> {
                                    ScaleResponse.Builder response = ScaleResponse.newBuilder();
                                    response.setStatus(ScaleResponse.ScaleStreamStatus.SUCCESS);
                                    response.addAllSegments(
                                            newSegments
                                                    .stream()
                                                    .map(segment -> convert(scope, stream, segment))
                                                    .collect(Collectors.toList()));
                                    return response.build();
                                });
                    } else {
                        ScaleResponse.Builder response = ScaleResponse.newBuilder();
                        response.setStatus(ScaleResponse.ScaleStreamStatus.PRECONDITION_FAILED);
                        response.addAllSegments(Collections.emptyList());
                        return CompletableFuture.completedFuture(response.build());
                    }
                }
        );
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

    private SegmentRange convert(String scope, String stream, com.emc.pravega.controller.store.stream.Segment segment) {

        return ModelHelper.createSegmentRange(scope, stream, segment.getNumber(), segment.getKeyEnd(),
                                              segment.getKeyEnd());
    }

    private UpdateStreamStatus.Status handleUpdateStreamError(Throwable ex) {
        Throwable cause = ExceptionHelpers.getRealException(ex);
        if (cause instanceof DataNotFoundException) {
            return UpdateStreamStatus.Status.STREAM_NOT_FOUND;
        } else if (ex instanceof StoreException && ((StoreException) ex).getType() == NODE_NOT_FOUND) {
            return UpdateStreamStatus.Status.SCOPE_NOT_FOUND;
        } else {
            log.warn("Update stream failed due to ", ex);
            return UpdateStreamStatus.Status.FAILURE;
        }
    }

    @Override
    public TaskBase copyWithContext(Context context) {
        return new StreamMetadataTasks(streamMetadataStore, hostControllerStore, taskMetadataStore, segmentHelper, executor, context);
    }
}

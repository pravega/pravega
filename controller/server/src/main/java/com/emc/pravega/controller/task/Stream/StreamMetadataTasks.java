/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.task.Stream;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.util.Retry;
import com.emc.pravega.controller.NonRetryableException;
import com.emc.pravega.controller.RetryableException;
import com.emc.pravega.controller.server.rpc.v1.SegmentHelper;
import com.emc.pravega.controller.server.rpc.v1.WireCommandFailedException;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.DataNotFoundException;
import com.emc.pravega.controller.store.stream.OperationContext;
import com.emc.pravega.controller.store.stream.Segment;
import com.emc.pravega.controller.store.stream.StreamAlreadyExistsException;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.task.Resource;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.stream.api.v1.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.v1.NodeUri;
import com.emc.pravega.controller.stream.api.v1.ScaleResponse;
import com.emc.pravega.controller.stream.api.v1.ScaleStreamStatus;
import com.emc.pravega.controller.stream.api.v1.SegmentId;
import com.emc.pravega.controller.stream.api.v1.SegmentRange;
import com.emc.pravega.controller.stream.api.v1.UpdateStreamStatus;
import com.emc.pravega.controller.task.Task;
import com.emc.pravega.controller.task.TaskBase;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.ModelHelper;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;
import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/**
 * Collection of metadata update tasks on stream.
 * Task methods are annotated with @Task annotation.
 * <p>
 * Any update to the task method signature should be avoided, since it can cause problems during upgrade.
 * Instead, a new overloaded method may be created with the same task annotation name but a new version.
 */
@Slf4j
public class StreamMetadataTasks extends TaskBase implements Cloneable {
    private static final long RETRY_INITIAL_DELAY = 100;
    private static final int RETRY_MULTIPLIER = 2;
    private static final int RETRY_MAX_ATTEMPTS = 100;
    private static final long RETRY_MAX_DELAY = Duration.ofSeconds(10).toMillis();

    private final StreamMetadataStore streamMetadataStore;
    private final HostControllerStore hostControllerStore;
    private final ConnectionFactoryImpl connectionFactory;

    public StreamMetadataTasks(final StreamMetadataStore streamMetadataStore,
                               final HostControllerStore hostControllerStore,
                               final TaskMetadataStore taskMetadataStore,
                               final ScheduledExecutorService executor,
                               final String hostId) {
        super(taskMetadataStore, executor, hostId);
        this.streamMetadataStore = streamMetadataStore;
        this.hostControllerStore = hostControllerStore;
        connectionFactory = new ConnectionFactoryImpl(false);
    }

    @Override
    public StreamMetadataTasks clone() throws CloneNotSupportedException {
        return (StreamMetadataTasks) super.clone();
    }

    /**
     * Create stream.
     *
     * @param scope           scope.
     * @param stream          stream name.
     * @param config          stream configuration.
     * @param createTimestamp creation timestamp.
     * @param contextOpt      optional context
     * @return creation status.
     */
    @Task(name = "createStream", version = "1.0", resource = "{scope}/{stream}")
    public CompletableFuture<CreateStreamStatus> createStream(String scope, String stream, StreamConfiguration config, long createTimestamp, OperationContext contextOpt) {
        return execute(
                new Resource(scope, stream),
                new Serializable[]{scope, stream, config, createTimestamp, null},
                () -> createStreamBody(scope, stream, config, createTimestamp, contextOpt));
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
    public CompletableFuture<UpdateStreamStatus> alterStream(String scope, String stream, StreamConfiguration config, OperationContext contextOpt) {
        return execute(
                new Resource(scope, stream),
                new Serializable[]{scope, stream, config, null},
                () -> updateStreamConfigBody(scope, stream, config, contextOpt));
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
    public CompletableFuture<ScaleResponse> scale(String scope, String stream, ArrayList<Integer> sealedSegments, ArrayList<AbstractMap.SimpleEntry<Double, Double>> newRanges, long scaleTimestamp, OperationContext contextOpt) {
        return execute(
                new Resource(scope, stream),
                new Serializable[]{scope, stream, sealedSegments, newRanges, scaleTimestamp, null},
                () -> scaleBody(scope, stream, sealedSegments, newRanges, scaleTimestamp, contextOpt));
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
    public CompletableFuture<UpdateStreamStatus> sealStream(String scope, String stream, OperationContext contextOpt) {
        return execute(
                new Resource(scope, stream),
                new Serializable[]{scope, stream, null},
                () -> sealStreamBody(scope, stream, contextOpt));
    }

    private CompletableFuture<CreateStreamStatus> createStreamBody(String scope, String stream, StreamConfiguration config, long timestamp, OperationContext contextOpt) {

        return this.streamMetadataStore.createStream(scope, stream, config, timestamp, null)
                .thenCompose(x -> {
                    log.debug("{}/{} created in metadata store", scope, stream);
                    if (x) {
                        final OperationContext context = contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;

                        return this.streamMetadataStore.getActiveSegments(scope, stream, context)
                                .thenCompose(activeSegments -> notifyNewSegments(config.getScope(), stream, activeSegments, context)
                                        .thenApply(y -> CreateStreamStatus.SUCCESS));
                    } else {
                        return CompletableFuture.completedFuture(CreateStreamStatus.FAILURE);
                    }
                })
                .handle((result, ex) -> {
                    if (ex != null) {
                        if (ex.getCause() instanceof StreamAlreadyExistsException) {
                            return CreateStreamStatus.STREAM_EXISTS;
                        } else {
                            log.warn("Create stream failed due to {}", ex);
                            return CreateStreamStatus.FAILURE;
                        }
                    } else {
                        return result;
                    }
                });
    }

    public CompletableFuture<UpdateStreamStatus> updateStreamConfigBody(String scope, String stream, StreamConfiguration config, OperationContext contextOpt) {
        final OperationContext context = contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;

        return streamMetadataStore.updateConfiguration(scope, stream, config, context)
                .handle((result, ex) -> {
                    if (ex != null) {
                        return handleUpdateStreamError(ex);
                    } else {
                        return result ? UpdateStreamStatus.SUCCESS : UpdateStreamStatus.FAILURE;
                    }
                });
    }

    public CompletableFuture<UpdateStreamStatus> sealStreamBody(String scope, String stream, OperationContext contextOpt) {
        final OperationContext context = contextOpt == null ? streamMetadataStore.createContext(scope, stream) : contextOpt;

        return streamMetadataStore.getActiveSegments(scope, stream, context)
                .thenCompose(activeSegments -> {
                    if (activeSegments.isEmpty()) { //if active segments are empty then the stream is sealed.
                        //Do not update the state if the stream is already sealed.
                        return CompletableFuture.completedFuture(UpdateStreamStatus.SUCCESS);
                    } else {
                        List<Integer> segmentsToBeSealed = activeSegments.stream().map(Segment::getNumber).
                                collect(Collectors.toList());
                        return notifySealedSegments(scope, stream, segmentsToBeSealed)
                                .thenCompose(v -> streamMetadataStore.setSealed(scope, stream, context))
                                .handle((result, ex) -> {
                                    if (ex != null) {
                                        log.warn("Exception thrown in trying to notify sealed segments {}", ex.getMessage());
                                        return handleUpdateStreamError(ex);
                                    } else {
                                        return result ? UpdateStreamStatus.SUCCESS : UpdateStreamStatus.FAILURE;
                                    }
                                });
                    }
                }).exceptionally(this::handleUpdateStreamError);

    }

    @VisibleForTesting
    CompletableFuture<ScaleResponse> scaleBody(String scope, String stream, List<Integer> sealedSegments, List<AbstractMap.SimpleEntry<Double, Double>> newRanges, long scaleTimestamp, OperationContext contextOpt) {
        // Abort scaling operation in the following error scenarios
        // 1. if the active segments in the stream have ts greater than scaleTimestamp -- ScaleStreamStatus.PRECONDITION_FAILED
        // 2. if active segments having creation timestamp as scaleTimestamp have different key ranges than the ones specified
        // in newRanges (todo) -- ScaleStreamStatus.CONFLICT
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

        CompletableFuture<Pair<Boolean, Boolean>> checkValidity = streamMetadataStore.getActiveSegments(scope, stream, context)
                .thenCompose(activeSegments -> streamMetadataStore.isTransactionOngoing(scope, stream, context)
                        .thenApply(active -> {
                            boolean result = false;
                            Set<Integer> activeNum = activeSegments.stream().mapToInt(Segment::getNumber).boxed().collect(Collectors.toSet());
                            if (activeNum.containsAll(sealedSegments)) {
                                result = true;
                            } else if (activeSegments.size() > 0 && activeSegments
                                    .stream()
                                    .mapToLong(Segment::getStart)
                                    .max()
                                    .getAsLong() == scaleTimestamp) {
                                result = true;
                            }
                            return new ImmutablePair<>(!active, result);
                        }));

        return checkValidity.thenCompose(result -> {
                    if (result.getLeft() && result.getRight()) {
                        return notifySealedSegments(scope, stream, sealedSegments)
                                .thenCompose(results ->
                                        scaleMetadataUpdate(scope, stream, sealedSegments, newRanges, scaleTimestamp, context))
                                .thenCompose((List<Segment> newSegments) -> notifyNewSegments(scope, stream, newSegments, context)
                                        .thenApply(z -> newSegments))
                                .thenApply((List<Segment> newSegments) -> {
                                    ScaleResponse response = new ScaleResponse();
                                    response.setStatus(ScaleStreamStatus.SUCCESS);
                                    response.setSegments(
                                            newSegments
                                                    .stream()
                                                    .map(segment -> convert(scope, stream, segment))
                                                    .collect(Collectors.toList()));
                                    return response;
                                });
                    } else {
                        ScaleResponse response = new ScaleResponse();
                        if (!result.getRight()) {
                            response.setStatus(ScaleStreamStatus.PRECONDITION_FAILED);
                        } else {
                            response.setStatus(ScaleStreamStatus.TXN_CONFLICT);
                        }
                        response.setSegments(Collections.emptyList());
                        return CompletableFuture.completedFuture(response);
                    }
                }
        );
    }

    private CompletableFuture<List<Segment>> scaleMetadataUpdate(final String scope, final String name,
                                                                 final List<Integer> sealedSegments,
                                                                 final List<AbstractMap.SimpleEntry<Double, Double>> newRanges,
                                                                 final long scaleTimestamp, OperationContext context) {
        return Retry.withExpBackoff(RETRY_INITIAL_DELAY, RETRY_MULTIPLIER, RETRY_MAX_ATTEMPTS, RETRY_MAX_DELAY)
                .retryingOn(RetryableException.class)
                .throwingOn(NonRetryableException.class)
                .runAsync(() -> streamMetadataStore.scale(scope, name, sealedSegments, newRanges, scaleTimestamp, context), executor);
    }

    private SegmentRange convert(String scope, String stream, com.emc.pravega.controller.store.stream.Segment segment) {
        return new SegmentRange(
                new SegmentId(scope, stream, segment.getNumber()), segment.getKeyStart(), segment.getKeyEnd());
    }

    private CompletableFuture<List<Boolean>> notifyNewSegments(String scope, String stream, List<Segment> segmentNumbers, OperationContext context) {
        return FutureHelpers.allOfWithResults(segmentNumbers
                .stream()
                .parallel()
                .map(segment ->
                        Retry.withExpBackoff(RETRY_INITIAL_DELAY, RETRY_MULTIPLIER, RETRY_MAX_ATTEMPTS, RETRY_MAX_DELAY)
                                .retryingOn(RetryableException.class)
                                .throwingOn(RuntimeException.class)
                                .runAsync(() ->
                                        streamMetadataStore.getConfiguration(scope, stream, context)
                                                .thenApply(configuration ->
                                                        new ImmutablePair<>(SegmentHelper.getSingleton().getSegmentUri(scope, stream, segment.getNumber(), hostControllerStore), configuration))
                                                .thenCompose(pair ->
                                                        notifyNewSegment(
                                                                scope,
                                                                stream,
                                                                segment.getNumber(),
                                                                pair.right.getScalingPolicy(),
                                                                pair.left))
                                                .<Boolean>handle((res, ex) -> {
                                                    Throwable t = ex;
                                                    if (ex != null) {
                                                        log.warn("Exception thrown while notifying host of new segment: {}", ex.getMessage());
                                                        if (ex instanceof CompletionException || ex instanceof ExecutionException) {
                                                            t = ex.getCause();
                                                        }

                                                        RetryableException.throwRetryableOrElse(t, z -> {
                                                            if (z instanceof WireCommandFailedException) {
                                                                throw new RetryableException(z);
                                                            } else {
                                                                throw new RuntimeException(z);
                                                            }
                                                        });
                                                    }
                                                    return res;
                                                }), executor))
                .collect(Collectors.toList()));
    }

    @VisibleForTesting
    CompletableFuture<Boolean> notifyNewSegment(String scope, String stream, int segmentNumber, ScalingPolicy policy, NodeUri uri) {
        return SegmentHelper.getSingleton().createSegment(scope,
                stream, segmentNumber, policy, ModelHelper.encode(uri), this.connectionFactory);
    }

    private CompletableFuture<Void> notifySealedSegments(String scope, String stream, List<Integer> sealedSegments) {
        return FutureHelpers.allOf(
                sealedSegments
                        .stream()
                        .parallel()
                        .map(number -> notifySealedSegment(scope, stream, number))
                        .collect(Collectors.toList()));
    }

    @VisibleForTesting
    CompletableFuture<Boolean> notifySealedSegment(final String scope, final String stream, final int
            sealedSegment) {
        return Retry.withExpBackoff(RETRY_INITIAL_DELAY, RETRY_MULTIPLIER, RETRY_MAX_ATTEMPTS, RETRY_MAX_DELAY)
                .retryingOn(RetryableException.class)
                .throwingOn(RuntimeException.class)
                .runAsync(() -> CompletableFuture.completedFuture(SegmentHelper.getSingleton().getSegmentUri(scope, stream, sealedSegment, hostControllerStore))
                                .thenCompose(uri ->
                                        SegmentHelper.getSingleton().sealSegment(
                                                scope,
                                                stream,
                                                sealedSegment,
                                                uri,
                                                this.connectionFactory))
                                .handle((res, ex) -> {
                                    if (ex != null) {
                                        log.warn("Exception thrown while notifying host of sealed segment: {}", ex.getMessage());
                                        if (ex instanceof WireCommandFailedException) {
                                            throw new RetryableException(ex);
                                        } else if (ex.getCause() instanceof WireCommandFailedException) {
                                            throw new RetryableException(ex.getCause());
                                        } else {
                                            throw new RuntimeException(ex);
                                        }
                                    }
                                    return res;
                                }),
                        executor);
    }

    private UpdateStreamStatus handleUpdateStreamError(Throwable ex) {
        if (ex instanceof DataNotFoundException ||
                (ex instanceof CompletionException && ex.getCause() instanceof DataNotFoundException)) {
            return UpdateStreamStatus.STREAM_NOT_FOUND;
        } else {
            log.warn("Update stream failed due to ", ex);
            return UpdateStreamStatus.FAILURE;
        }
    }
}

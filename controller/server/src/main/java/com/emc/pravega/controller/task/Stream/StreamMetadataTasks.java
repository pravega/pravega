/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.task.Stream;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.util.Retry;
import com.emc.pravega.controller.server.rpc.v1.SegmentHelper;
import com.emc.pravega.controller.server.rpc.v1.WireCommandFailedException;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.Segment;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.StreamNotFoundException;
import com.emc.pravega.controller.store.stream.StoreException;
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
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.ModelHelper;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;
import com.google.common.annotations.VisibleForTesting;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

import static com.emc.pravega.controller.store.stream.StoreException.Type.NODE_EXISTS;
import static com.emc.pravega.controller.store.stream.StoreException.Type.NODE_NOT_FOUND;

/**
 * Collection of metadata update tasks on stream.
 * Task methods are annotated with @Task annotation.
 * <p>
 * Any update to the task method signature should be avoided, since it can cause problems during upgrade.
 * Instead, a new overloaded method may be created with the same task annotation name but a new version.
 */
@Slf4j
public class StreamMetadataTasks extends TaskBase {
    private static final long RETRY_INITIAL_DELAY = 100;
    private static final int RETRY_MULTIPLIER = 10;
    private static final int RETRY_MAX_ATTEMPTS = 100;
    private static final long RETRY_MAX_DELAY = 100000;

    private final StreamMetadataStore streamMetadataStore;
    private final HostControllerStore hostControllerStore;
    private final ConnectionFactoryImpl connectionFactory;

    public StreamMetadataTasks(final StreamMetadataStore streamMetadataStore,
                               final HostControllerStore hostControllerStore,
                               final TaskMetadataStore taskMetadataStore,
                               final ScheduledExecutorService executor,
                               final String hostId) {
        this(streamMetadataStore, hostControllerStore, taskMetadataStore, executor, new Context(hostId));
    }

    private StreamMetadataTasks(final StreamMetadataStore streamMetadataStore,
                                final HostControllerStore hostControllerStore,
                                final TaskMetadataStore taskMetadataStore,
                                final ScheduledExecutorService executor,
                                final Context context) {
        super(taskMetadataStore, executor, context);
        this.streamMetadataStore = streamMetadataStore;
        this.hostControllerStore = hostControllerStore;
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
    public CompletableFuture<CreateStreamStatus> createStream(String scope, String stream, StreamConfiguration config, long createTimestamp) {
        return execute(
                new Resource(scope, stream),
                new Serializable[]{scope, stream, config},
                () -> createStreamBody(scope, stream, config, createTimestamp));
    }

    /**
     * Update stream's configuration.
     *
     * @param scope  scope.
     * @param stream stream name.
     * @param config modified stream configuration.
     * @return update status.
     */
    @Task(name = "updateConfig", version = "1.0", resource = "{scope}/{stream}")
    public CompletableFuture<UpdateStreamStatus> alterStream(String scope, String stream, StreamConfiguration config) {
        return execute(
                new Resource(scope, stream),
                new Serializable[]{scope, stream, config},
                () -> updateStreamConfigBody(scope, stream, config));
    }

    /**
     * Seal a stream.
     *
     * @param scope  scope.
     * @param stream stream name.
     * @return update status.
     */
    @Task(name = "sealStream", version = "1.0", resource = "{scope}/{stream}")
    public CompletableFuture<UpdateStreamStatus> sealStream(String scope, String stream) {
        return execute(
                new Resource(scope, stream),
                new Serializable[]{scope, stream},
                () -> sealStreamBody(scope, stream));
    }

    /**
     * Scales stream segments.
     *
     * @param scope          scope.
     * @param stream         stream name.
     * @param sealedSegments segments to be sealed.
     * @param newRanges      key ranges for new segments.
     * @param scaleTimestamp scaling time stamp.
     * @return returns the newly created segments.
     */
    @Task(name = "scaleStream", version = "1.0", resource = "{scope}/{stream}")
    public CompletableFuture<ScaleResponse> scale(String scope, String stream, ArrayList<Integer> sealedSegments, ArrayList<AbstractMap.SimpleEntry<Double, Double>> newRanges, long scaleTimestamp) {
        return execute(
                new Resource(scope, stream),
                new Serializable[]{scope, stream, sealedSegments, newRanges, scaleTimestamp},
                () -> scaleBody(scope, stream, sealedSegments, newRanges, scaleTimestamp));
    }

    private CompletableFuture<CreateStreamStatus> createStreamBody(String scope, String stream, StreamConfiguration config, long timestamp) {
        if (!validateName(stream)) {
            log.debug("Create stream failed due to invalid stream name {}", stream);
            return CompletableFuture.completedFuture(CreateStreamStatus.INVALID_STREAM_NAME);
        } else {
            return this.streamMetadataStore.createStream(scope, stream, config, timestamp)
                    .thenCompose(x -> {
                        if (x) {
                            return this.streamMetadataStore.getActiveSegments(scope, stream)
                                    .thenApply(activeSegments ->
                                            notifyNewSegments(config.getScope(), stream, activeSegments))
                                    .thenApply(y -> CreateStreamStatus.SUCCESS);
                        } else {
                            return CompletableFuture.completedFuture(CreateStreamStatus.FAILURE);
                        }
                    })
                    .handle((result, ex) -> {
                        if (ex != null) {
                            if (ex.getCause() instanceof StoreException && ((StoreException) ex.getCause()).getType() == NODE_EXISTS) {
                                return CreateStreamStatus.STREAM_EXISTS;
                            } else if (ex.getCause() instanceof StoreException && ((StoreException) ex.getCause()).getType() == NODE_NOT_FOUND) {
                                return CreateStreamStatus.SCOPE_NOT_FOUND;
                            } else {
                                log.warn("Create stream failed due to ", ex);
                                return CreateStreamStatus.FAILURE;
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

    public CompletableFuture<UpdateStreamStatus> updateStreamConfigBody(String scope, String stream, StreamConfiguration config) {
        return streamMetadataStore.updateConfiguration(scope, stream, config)
                .handle((result, ex) -> {
                    if (ex != null) {
                        return handleUpdateStreamError(ex);
                    } else {
                        return result ? UpdateStreamStatus.SUCCESS : UpdateStreamStatus.FAILURE;
                    }
                });
    }

    public CompletableFuture<UpdateStreamStatus> sealStreamBody(String scope, String stream) {
        return streamMetadataStore.getActiveSegments(scope, stream)
                .thenCompose(activeSegments -> {
                    if (activeSegments.isEmpty()) { //if active segments are empty then the stream is sealed.
                        //Do not update the state if the stream is already sealed.
                        return CompletableFuture.completedFuture(UpdateStreamStatus.SUCCESS);
                    } else {
                        List<Integer> segmentsToBeSealed = activeSegments.stream().map(Segment::getNumber).
                                collect(Collectors.toList());
                        return notifySealedSegments(scope, stream, segmentsToBeSealed)
                                .thenCompose(v -> streamMetadataStore.setSealed(scope, stream))
                                .handle((result, ex) -> {
                                    if (ex != null) {
                                        return handleUpdateStreamError(ex);
                                    } else {
                                        return result ? UpdateStreamStatus.SUCCESS : UpdateStreamStatus.FAILURE;
                                    }
                                });
                    }
                }).exceptionally(this::handleUpdateStreamError);
    }

    @VisibleForTesting
    CompletableFuture<ScaleResponse> scaleBody(String scope, String stream, List<Integer> sealedSegments, List<AbstractMap.SimpleEntry<Double, Double>> newRanges, long scaleTimestamp) {
        // Abort scaling operation in the following error scenarios
        // 1. if the active segments in the stream have ts greater than scaleTimestamp -- ScaleStreamStatus.PRECONDITION_FAILED
        // 2. if active segments having creation timestamp as scaleTimestamp have different key ranges than the ones specified in newRanges (todo) -- ScaleStreamStatus.CONFLICT
        // 3. Transaction is active on the stream
        // 4. sealedSegments should be a subset of activeSegments.
        CompletableFuture<Boolean> checkValidity =
                streamMetadataStore.getActiveSegments(scope, stream)
                        .thenCompose(activeSegments ->
                                streamMetadataStore
                                        .isTransactionOngoing(scope, stream)
                                        .thenApply(active ->
                                                // transaction is ongoing
                                                active
                                                        ||
                                                        // some segment to be sealed is not an active segment
                                                        sealedSegments
                                                                .stream()
                                                                .anyMatch(x ->
                                                                        activeSegments
                                                                                .stream()
                                                                                .noneMatch(segment ->
                                                                                        segment.getNumber() == x))
                                                        ||
                                                        // scale timestamp is not larger than start time of
                                                        // some active segment
                                                        activeSegments
                                                                .stream()
                                                                .anyMatch(segment ->
                                                                        segment.getStart() > scaleTimestamp)));

        return checkValidity.thenCompose(result -> {

                    if (!result) {
                        return notifySealedSegments(scope, stream, sealedSegments)

                                .thenCompose(results ->
                                        streamMetadataStore.scale(scope, stream, sealedSegments, newRanges, scaleTimestamp))

                                .thenApply((List<Segment> newSegments) -> {
                                    notifyNewSegments(scope, stream, newSegments);
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
                        response.setStatus(ScaleStreamStatus.PRECONDITION_FAILED);
                        response.setSegments(Collections.emptyList());
                        return CompletableFuture.completedFuture(response);
                    }
                }
        );
    }

    private SegmentRange convert(String scope, String stream, com.emc.pravega.controller.store.stream.Segment segment) {
        return new SegmentRange(
                new SegmentId(scope, stream, segment.getNumber()), segment.getKeyStart(), segment.getKeyEnd());
    }

    private Void notifyNewSegments(String scope, String stream, List<Segment> segmentNumbers) {
        segmentNumbers
                .stream()
                .parallel()
                .forEach(segment -> notifyNewSegment(scope, stream, segment.getNumber()));
        return null;
    }

    private Void notifyNewSegment(String scope, String stream, int segmentNumber) {
        NodeUri uri = SegmentHelper.getSegmentUri(scope, stream, segmentNumber, this.hostControllerStore);

        // async call, don't wait for its completion or success. Host will contact controller if it does not know
        // about some segment even if this call fails?
        CompletableFuture.runAsync(() -> SegmentHelper.createSegment(scope, stream, segmentNumber, ModelHelper.encode(uri), this.connectionFactory), executor);
        return null;
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
                .retryingOn(WireCommandFailedException.class)
                .throwingOn(RuntimeException.class)
                .runAsync(() ->
                                SegmentHelper.sealSegment(
                                        scope,
                                        stream,
                                        sealedSegment,
                                        this.hostControllerStore,
                                        this.connectionFactory),
                        executor);
    }

    private UpdateStreamStatus handleUpdateStreamError(Throwable ex) {
        if (ex instanceof StreamNotFoundException ||
                (ex instanceof CompletionException && ex.getCause() instanceof StreamNotFoundException)) {
            return UpdateStreamStatus.STREAM_NOT_FOUND;
        } else {
            log.warn("Update stream failed due to ", ex);
            return UpdateStreamStatus.FAILURE;
        }
    }

    @Override
    public TaskBase copyWithContext(Context context) {
        return new StreamMetadataTasks(streamMetadataStore, hostControllerStore, taskMetadataStore, executor, context);
    }
}

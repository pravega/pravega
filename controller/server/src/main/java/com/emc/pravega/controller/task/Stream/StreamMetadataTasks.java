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

import com.emc.pravega.common.concurrent.FutureCollectionHelper;
import com.emc.pravega.common.util.Retry;
import com.emc.pravega.controller.server.rpc.v1.WireCommandFailedException;
import com.emc.pravega.controller.server.rpc.v1.SegmentHelper;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.Segment;
import com.emc.pravega.controller.store.stream.StreamAlreadyExistsException;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.StreamNotFoundException;
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

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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
        return this.streamMetadataStore.createStream(stream, config, timestamp)
                .thenCompose(x -> {
                    if (x) {
                        return this.streamMetadataStore.getActiveSegments(stream)
                                .thenApply(activeSegments ->
                                        notifyNewSegments(config.getScope(), stream, activeSegments))
                                .thenApply(y -> CreateStreamStatus.SUCCESS);
                    } else {
                        return CompletableFuture.completedFuture(CreateStreamStatus.FAILURE);
                    }
                })
                .handle((result, ex) -> {
                    if (ex != null) {
                        if (ex.getCause() instanceof StreamAlreadyExistsException) {
                            return CreateStreamStatus.STREAM_EXISTS;
                        } else {
                            log.warn("Create stream failed due to ", ex);
                            return CreateStreamStatus.FAILURE;
                        }
                    } else {
                        return result;
                    }
                });
    }

    public CompletableFuture<UpdateStreamStatus> updateStreamConfigBody(String scope, String stream, StreamConfiguration config) {
        return streamMetadataStore.updateConfiguration(stream, config)
                .handle((result, ex) -> {
                    if (ex != null) {
                        if (ex instanceof StreamNotFoundException) {
                            return UpdateStreamStatus.STREAM_NOT_FOUND;
                        } else {
                            log.warn("Update stream failed due to ", ex);
                            return UpdateStreamStatus.FAILURE;
                        }
                    } else {
                        return result ? UpdateStreamStatus.SUCCESS : UpdateStreamStatus.FAILURE;
                    }
                });
    }

    private CompletableFuture<ScaleResponse> scaleBody(String scope, String stream, List<Integer> sealedSegments, List<AbstractMap.SimpleEntry<Double, Double>> newRanges, long scaleTimestamp) {
        // Abort scaling operation in the following error scenarios
        // 1. if the active segments in the stream have ts greater than scaleTimestamp -- ScaleStreamStatus.PRECONDITION_FAILED
        // 2. if active segments having creation timestamp as scaleTimestamp have different key ranges than the ones specified in newRanges (todo) -- ScaleStreamStatus.CONFLICT
        // 3. Transaction is active on the stream (todo)-- return ScaleStreamStatus.CONFLICT status in this case
        CompletableFuture<Boolean> checkValidity =
                streamMetadataStore.getActiveSegments(stream)
                        .thenCompose(activeSegments ->
                                        streamMetadataStore
                                                .isTransactionOngoing(scope, stream)
                                                .thenApply(active -> active ||
                                                                activeSegments
                                                                        .stream()
                                                                        .anyMatch(segment ->
                                                                                segment.getStart() > scaleTimestamp)));

        return checkValidity.thenCompose(result -> {

                    if (!result) {
                        return notifySealedSegments(scope, stream, sealedSegments)

                                .thenCompose(results ->
                                        streamMetadataStore.scale(stream, sealedSegments, newRanges, scaleTimestamp))

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
        return FutureCollectionHelper.sequence(
                sealedSegments
                        .stream()
                        .parallel()
                        .map(number -> notifySealedSegment(scope, stream, number))
                        .collect(Collectors.toList()))
                .thenApply(x -> null);
    }

    private CompletableFuture<Boolean> notifySealedSegment(final String scope, final String stream, final int sealedSegment) {
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
}

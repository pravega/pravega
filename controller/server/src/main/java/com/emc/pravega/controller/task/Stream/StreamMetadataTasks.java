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

import com.emc.pravega.controller.server.rpc.v1.SegmentHelper;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.ScalingConflictException;
import com.emc.pravega.controller.store.stream.Segment;
import com.emc.pravega.controller.store.stream.StreamAlreadyExistsException;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.StreamNotFoundException;
import com.emc.pravega.controller.stream.api.v1.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.v1.NodeUri;
import com.emc.pravega.controller.stream.api.v1.UpdateStreamStatus;
import com.emc.pravega.controller.task.Task;
import com.emc.pravega.controller.task.TaskBase;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.model.ModelHelper;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Collection of metadata update tasks on stream.
 * Task methods are annotated with @Task annotation.
 *
 * Any update to the task method signature should be avoided, since it can cause problems during upgrade.
 * Instead, a new overloaded method may be created with the same task annotation name but a new version.
 */
public class StreamMetadataTasks extends TaskBase {

    public StreamMetadataTasks(StreamMetadataStore streamMetadataStore, HostControllerStore hostControllerStore, TaskMetadataStore taskMetadataStore) {
        super(streamMetadataStore, hostControllerStore, taskMetadataStore);
    }

    /**
     * Create stream.
     * @param scope scope.
     * @param stream stream name.
     * @param config stream configuration.
     * @param createTimestamp creation timestamp.
     * @return creation status.
     */
    @Task(name = "createStream", version = "1.0", resource = "{scope}/{stream}")
    public CompletableFuture<CreateStreamStatus> createStream(String scope, String stream, StreamConfiguration config, long createTimestamp) {
        return execute(
                getResource(scope, stream),
                new Serializable[]{scope, stream, config},
                () -> createStreamBody(scope, stream, config),
                null);
    }

    /**
     * Update stream's configuration.
     * @param scope scope.
     * @param stream stream name.
     * @param config modified stream configuration.
     * @return update status.
     */
    @Task(name = "updateConfig", version = "1.0", resource = "{scope}/{stream}")
    public CompletableFuture<UpdateStreamStatus> alterStream(String scope, String stream, StreamConfiguration config) {
        return execute(
                getResource(scope, stream),
                new Serializable[]{scope, stream, config},
                () -> updateStreamConfigBody(scope, stream, config),
                null);
    }

    /**
     * Scales stream segments.
     * @param scope scope.
     * @param stream stream name.
     * @param sealedSegments segments to be sealed.
     * @param newRanges key ranges for new segments.
     * @param scaleTimestamp scaling time stamp.
     * @return returns the newly created segments.
     */
    @Task(name = "scaleStream", version = "1.0", resource = "{scope}/{stream}")
    public CompletableFuture<List<Segment>> scale(String scope, String stream, ArrayList<Integer> sealedSegments, ArrayList<AbstractMap.SimpleEntry<Double, Double>> newRanges, long scaleTimestamp) {
        Serializable[] params = {scope, stream, sealedSegments, newRanges, scaleTimestamp};
        return execute(
                getResource(scope, stream),
                new Serializable[]{scope, stream, sealedSegments, newRanges, scaleTimestamp},
                () -> scaleBody(scope, stream, sealedSegments, newRanges, scaleTimestamp),
                null);
    }

    private CompletableFuture<CreateStreamStatus> createStreamBody(String scope, String stream, StreamConfiguration config) {
        return this.streamMetadataStore.createStream(stream, config)
                .handle((result, ex) -> {
                    if (ex != null) {
                        if (ex instanceof StreamAlreadyExistsException) {
                            return CreateStreamStatus.STREAM_EXISTS;
                        } else {
                            return CreateStreamStatus.FAILURE;
                        }
                    } else {
                        // result is non-null
                        if (result) {
                            // successful stream creation implies the stream was completely created from scratch
                            // or its creation was completed from a previous incomplete state resulting from host failure
                            this.streamMetadataStore.getActiveSegments(stream)
                                    .thenApply(activeSegments ->
                                            notifyNewSegments(config.getScope(), stream, activeSegments));
                            return CreateStreamStatus.SUCCESS;
                        } else {
                            // failure indicates that the stream creation failed due to some internal error, or
                            return CreateStreamStatus.FAILURE;
                        }
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
                            return UpdateStreamStatus.FAILURE;
                        }
                    } else {
                        return result ? UpdateStreamStatus.SUCCESS : UpdateStreamStatus.FAILURE;
                    }
                });
    }

    private CompletableFuture<List<Segment>> scaleBody(String scope, String stream, List<Integer> sealedSegments, List<AbstractMap.SimpleEntry<Double, Double>> newRanges, long scaleTimestamp) {
        // Abort scaling operation in the following error scenarios
        // 1. if the active segments in the stream have ts greater than scaleTimestamp, or
        // 2. if active segments having creation timestamp as scaleTimestamp have different key ranges than the ones specified in newRanges

        CompletableFuture<Boolean> checkValidity =
                streamMetadataStore.getActiveSegments(stream)
                        .thenApply(activeSegments ->
                                activeSegments
                                        .stream()
                                        .anyMatch(segment -> segment.getStart() > scaleTimestamp));

        return checkValidity.thenCompose(result -> {

                    if (result) {
                        return notifySealedSegments(scope, stream, sealedSegments)
                                .thenCompose(results ->
                                        streamMetadataStore.scale(stream, sealedSegments, newRanges, scaleTimestamp))
                                .thenApply(newSegments -> {
                                    notifyNewSegments(scope, stream, newSegments);
                                    return newSegments;
                                });
                    } else {
                        throw new ScalingConflictException(stream, scaleTimestamp);
                    }
                }
        );
    }

    private String getResource(String scope, String stream) {
        return scope + "/" + stream;
    }

    private Void notifyNewSegments(String scope, String stream, List<Segment> segmentNumbers) {
        segmentNumbers
                .stream()
                .parallel()
                .forEach(segment -> asyncNotifyNewSegment(scope, stream, segment.getNumber()));
        return null;
    }

    private Void asyncNotifyNewSegment(String scope, String stream, int segmentNumber) {
        NodeUri uri = SegmentHelper.getSegmentUri(scope, stream, segmentNumber, this.hostControllerStore);

        // async call, don't wait for its completion or success. Host will contact controller if it does not know
        // about some segment even if this call fails?
        CompletableFuture.runAsync(() -> SegmentHelper.createSegment(scope, stream, segmentNumber, ModelHelper.encode(uri), this.connectionFactory));
        return null;
    }

    private CompletableFuture<Void> notifySealedSegments(String scope, String stream, List<Integer> sealedSegments) {
        sealedSegments
                .stream()
                .parallel()
                .forEach(number -> SegmentHelper.sealSegment(scope, stream, number, this.hostControllerStore, this.connectionFactory));
        return CompletableFuture.completedFuture(null);
    }
}

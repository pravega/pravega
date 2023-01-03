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
package io.pravega.controller.server.eventProcessor.requesthandlers;

import com.google.common.base.Preconditions;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Timer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.TagLogger;
import io.pravega.controller.metrics.StreamMetrics;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.stream.State;
import io.pravega.controller.store.stream.records.EpochRecord;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;
import io.pravega.controller.store.stream.records.StreamConfigurationRecord;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.shared.NameUtils;
import io.pravega.shared.controller.event.UpdateStreamEvent;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.LoggerFactory;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.pravega.client.stream.ScalingPolicy.ScaleType.FIXED_NUM_SEGMENTS;

/**
 * Request handler for performing scale operations received from requeststream.
 */
public class UpdateStreamTask implements StreamTask<UpdateStreamEvent> {
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(UpdateStreamTask.class));

    private final StreamMetadataTasks streamMetadataTasks;
    private final StreamMetadataStore streamMetadataStore;
    private final BucketStore bucketStore;
    private final ScheduledExecutorService executor;

    public UpdateStreamTask(final StreamMetadataTasks streamMetadataTasks,
                            final StreamMetadataStore streamMetadataStore,
                            final BucketStore bucketStore, 
                            final ScheduledExecutorService executor) {
        Preconditions.checkNotNull(streamMetadataStore);
        Preconditions.checkNotNull(streamMetadataTasks);
        Preconditions.checkNotNull(bucketStore);
        Preconditions.checkNotNull(executor);
        this.streamMetadataTasks = streamMetadataTasks;
        this.streamMetadataStore = streamMetadataStore;
        this.bucketStore = bucketStore;
        this.executor = executor;
    }

    @Override
    public CompletableFuture<Void> execute(final UpdateStreamEvent request) {

        Timer timer = new Timer();
        String scope = request.getScope();
        String stream = request.getStream();
        long requestId = request.getRequestId();
        final OperationContext context = streamMetadataStore.createStreamContext(scope, stream, requestId);

        return streamMetadataStore.getVersionedState(scope, stream, context, executor)
                .thenCompose(versionedState -> {
                    if (versionedState.getObject().equals(State.SEALED)) {
                        // updating the stream should not be allowed since it has been SEALED
                        // hence, we need to update the configuration in the store with updating flag = false
                        // and then throw an exception
                        return streamMetadataStore.getConfigurationRecord(scope, stream, context, executor)
                                .thenCompose(versionedMetadata -> streamMetadataStore.completeUpdateConfiguration(scope, stream, versionedMetadata, context, executor)
                                        .thenAccept(v -> {
                                            throw new UnsupportedOperationException("Cannot update a sealed stream: " + NameUtils.getScopedStreamName(scope, stream));
                                        }));
                    }
                    return streamMetadataStore.getConfigurationRecord(scope, stream, context, executor)
                            .thenCompose(versionedMetadata -> {
                                if (!versionedMetadata.getObject().isUpdating()) {
                                    if (versionedState.getObject().equals(State.UPDATING)) {
                                        return Futures.toVoid(streamMetadataStore.updateVersionedState(scope, stream, State.ACTIVE,
                                                versionedState, context, executor));
                                    } else {
                                        return CompletableFuture.completedFuture(null);
                                    }
                                } else {
                                    return processUpdate(scope, stream, versionedMetadata, versionedState, context, requestId)
                                            .thenAccept(v -> StreamMetrics.getInstance().controllerEventProcessorUpdateStreamEvent(timer.getElapsed()));
                                }
                            });
                });
    }

    private CompletableFuture<Void> processUpdate(String scope, String stream, VersionedMetadata<StreamConfigurationRecord> record,
                                                  VersionedMetadata<State> state, OperationContext context, long requestId) {
        StreamConfigurationRecord configProperty = record.getObject();

        return Futures.toVoid(
                streamMetadataStore.getEpochTransition(scope, stream, context, executor)
                .thenCompose(etr -> streamMetadataStore.updateVersionedState(scope, stream, State.UPDATING,
                        state, context, executor)
                .thenCompose(updated -> updateStreamForAutoStreamCut(scope, stream, configProperty, updated)
                        .thenCompose(x -> notifyPolicyUpdate(context, scope, stream, configProperty.getStreamConfiguration(),
                                requestId))
                        .thenCompose(x -> handleSegmentCountUpdates(scope, stream, configProperty, etr, context, executor,
                                                                    requestId))
                        .thenCompose(x -> streamMetadataStore.removeTagsFromIndex(scope, stream, configProperty.getRemoveTags(), context, executor))
                        .thenCompose(x -> streamMetadataStore.addStreamTagsToIndex(scope, stream, configProperty.getStreamConfiguration(), context, executor))
                        .thenCompose(x -> streamMetadataStore.completeUpdateConfiguration(scope, stream, record, context,
                                executor))
                        .thenCompose(x -> streamMetadataStore.updateVersionedState(scope, stream, State.ACTIVE, updated,
                                context, executor)))));
    }

    private CompletableFuture<Void> handleSegmentCountUpdates(final String scope, final String stream,
                                                              final StreamConfigurationRecord config,
                                                              final VersionedMetadata<EpochTransitionRecord> etr,
                                                              final OperationContext context,
                                                              final ScheduledExecutorService executor,
                                                              final long requestId) {
        return streamMetadataStore.getActiveEpoch(scope, stream, context, true, executor)
                              .thenCompose(activeEpoch -> {
                                  ScalingPolicy scalingPolicy = config.getStreamConfiguration().getScalingPolicy();
                                  int minNumSegments = scalingPolicy.getMinNumSegments();

                                  if ((scalingPolicy.getScaleType() == FIXED_NUM_SEGMENTS &&
                                          activeEpoch.getSegments().size() != minNumSegments) ||
                                          activeEpoch.getSegments().size() < minNumSegments) {
                                      return processScale(scope, stream, minNumSegments, etr, activeEpoch, context, requestId);
                                  } else {
                                      return CompletableFuture.completedFuture(null);
                                  }
                              });
    }

    private CompletableFuture<Void> processScale(String scope, String stream, int numSegments,
                                                 VersionedMetadata<EpochTransitionRecord> etr, EpochRecord activeEpoch,
                                                 OperationContext context, long requestId) {
        // First reset the existing epoch transition (any submitted but unattempted scale
        // request is discarded). Then submit a new scale to seal all segments in active epoch and create new segments
        // with count = numSegments and range equally divided among them.
        final double keyRangeChunk = 1.0 / numSegments;

        List<Map.Entry<Double, Double>> newRange = IntStream.range(0, numSegments).boxed()
                                                            .map(x -> new AbstractMap.SimpleEntry<>(x * keyRangeChunk,
                                                                    (x + 1) * keyRangeChunk))
                                                            .collect(Collectors.toList());
        log.debug("{} Scaling stream to update minimum number of segments to {}", requestId, numSegments);
        return streamMetadataStore.resetEpochTransition(scope, stream, etr, context, executor)
              .thenCompose(reset -> streamMetadataStore.submitScale(scope, stream,
                      new ArrayList<>(activeEpoch.getSegmentIds()), newRange,
                      System.currentTimeMillis(), reset, context, executor))
              .thenCompose(updated -> streamMetadataTasks.processScale(scope, stream, updated, context,
                      requestId, streamMetadataStore)
              .thenAccept(r -> log.info("{} Stream scaled to epoch {} to update minimum number of segments to {}", requestId,
                      updated.getObject().getActiveEpoch(), numSegments)));
    }

    private CompletableFuture<Void> updateStreamForAutoStreamCut(String scope, String stream,
                        StreamConfigurationRecord configProperty, VersionedMetadata<State> updated) {
        if (configProperty.getStreamConfiguration().getRetentionPolicy() != null) {
            return bucketStore.addStreamToBucketStore(BucketStore.ServiceType.RetentionService, scope, stream, executor);
        } else {
            return bucketStore.removeStreamFromBucketStore(BucketStore.ServiceType.RetentionService, scope, stream, executor);
        }
    }

    private CompletableFuture<Boolean> notifyPolicyUpdate(OperationContext context, String scope, String stream,
                                                          StreamConfiguration newConfig, long requestId) {
        return streamMetadataStore.getActiveSegments(scope, stream, context, executor)
                .thenCompose(activeSegments -> streamMetadataTasks.notifyPolicyUpdates(scope, stream, activeSegments,
                        newConfig.getScalingPolicy(), this.streamMetadataTasks.retrieveDelegationToken(), requestId))
                .handle((res, ex) -> {
                    if (ex == null) {
                        return true;
                    } else {
                        throw new CompletionException(ex);
                    }
                });
    }

    @Override
    public CompletableFuture<Void> writeBack(UpdateStreamEvent event) {
        return streamMetadataTasks.writeEvent(event);
    }

    @Override
    public CompletableFuture<Boolean> hasTaskStarted(UpdateStreamEvent event) {
        return streamMetadataStore.getState(event.getScope(), event.getStream(), true, null, executor)
                                  .thenApply(state -> state.equals(State.UPDATING));

    }
}

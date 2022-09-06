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
import io.pravega.common.Timer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.TagLogger;
import io.pravega.controller.metrics.StreamMetrics;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.stream.State;
import io.pravega.controller.store.stream.records.StreamTruncationRecord;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.shared.NameUtils;
import io.pravega.shared.controller.event.TruncateStreamEvent;
import io.pravega.shared.metrics.DynamicLogger;
import io.pravega.shared.metrics.MetricsProvider;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import org.slf4j.LoggerFactory;

import static io.pravega.shared.MetricsNames.TRUNCATED_SIZE;
import static io.pravega.shared.MetricsTags.streamTags;

/**
 * Request handler for performing truncation operations received from requeststream.
 */
public class TruncateStreamTask implements StreamTask<TruncateStreamEvent> {

    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(TruncateStreamTask.class));
    private static final DynamicLogger DYNAMIC_LOGGER = MetricsProvider.getDynamicLogger();

    private final StreamMetadataTasks streamMetadataTasks;
    private final StreamMetadataStore streamMetadataStore;
    private final ScheduledExecutorService executor;

    public TruncateStreamTask(final StreamMetadataTasks streamMetadataTasks,
                              final StreamMetadataStore streamMetadataStore,
                              final ScheduledExecutorService executor) {
        Preconditions.checkNotNull(streamMetadataStore);
        Preconditions.checkNotNull(streamMetadataTasks);
        Preconditions.checkNotNull(executor);
        this.streamMetadataTasks = streamMetadataTasks;
        this.streamMetadataStore = streamMetadataStore;
        this.executor = executor;
    }

    @Override
    public CompletableFuture<Void> execute(final TruncateStreamEvent request) {

        Timer timer = new Timer();
        String scope = request.getScope();
        String stream = request.getStream();
        long requestId = request.getRequestId();
        final OperationContext context = streamMetadataStore.createStreamContext(scope, stream, requestId);

        return streamMetadataStore.getVersionedState(scope, stream, context, executor)
                .thenCompose(versionedState -> {
                    if (versionedState.getObject().equals(State.SEALED)) {
                        // truncation should not be allowed since the stream is in SEALED state
                        // hence, we need to complete the truncation by updating the metadata
                        // and then throw an exception
                        return streamMetadataStore.getTruncationRecord(scope, stream, context, executor)
                                .thenCompose(versionedMetadata -> streamMetadataStore.completeTruncation(scope, stream, versionedMetadata, context, executor)
                                        .thenAccept(v -> {
                                            throw new UnsupportedOperationException("Cannot truncate a sealed stream: " + NameUtils.getScopedStreamName(scope, stream));
                                        }));
                    }
                    return streamMetadataStore.getTruncationRecord(scope, stream, context, executor)
                            .thenCompose(versionedMetadata -> {
                                if (!versionedMetadata.getObject().isUpdating()) {
                                    if (versionedState.getObject().equals(State.TRUNCATING)) {
                                        return Futures.toVoid(streamMetadataStore.updateVersionedState(scope, stream, State.ACTIVE,
                                                versionedState, context, executor));
                                    } else {
                                        return CompletableFuture.completedFuture(null);
                                    }
                                } else {
                                    return processTruncate(scope, stream, versionedMetadata, versionedState, context, requestId)
                                            .thenAccept(v -> StreamMetrics.getInstance().controllerEventProcessorTruncateStreamEvent(timer.getElapsed()));
                                }
                            });
                });
    }

    private CompletableFuture<Void> processTruncate(String scope, String stream, VersionedMetadata<StreamTruncationRecord> versionedTruncationRecord,
                                                    VersionedMetadata<State> versionedState, OperationContext context, long requestId) {
        String delegationToken = this.streamMetadataTasks.retrieveDelegationToken();
        StreamTruncationRecord truncationRecord = versionedTruncationRecord.getObject();
        log.info(requestId, "Truncating stream {}/{} at stream cut: {}", scope, stream, truncationRecord.getStreamCut());
        return Futures.toVoid(streamMetadataStore.updateVersionedState(scope, stream, State.TRUNCATING, versionedState, context, executor)
                .thenCompose(update -> notifyTruncateSegments(scope, stream, truncationRecord.getStreamCut(), delegationToken, requestId)
                        .thenCompose(x -> notifyDeleteSegments(scope, stream, truncationRecord.getToDelete(), delegationToken, requestId))
                        .thenAccept(x -> DYNAMIC_LOGGER.reportGaugeValue(TRUNCATED_SIZE,
                                versionedTruncationRecord.getObject().getSizeTill(), streamTags(scope, stream)))
                        .thenCompose(deleted -> streamMetadataStore.completeTruncation(scope, stream, versionedTruncationRecord, context, executor))
                        .thenCompose(x -> streamMetadataStore.updateVersionedState(scope, stream, State.ACTIVE, update, context, executor))));
    }

    private CompletableFuture<Void> notifyDeleteSegments(String scope, String stream, Set<Long> segmentsToDelete,
                                                         String delegationToken, long requestId) {
        log.debug(requestId, "{}/{} deleting segments {}", scope, stream, segmentsToDelete);
        return Futures.allOf(segmentsToDelete.stream()
                .parallel()
                .map(segment -> streamMetadataTasks.notifyDeleteSegment(scope, stream, segment, delegationToken, requestId))
                .collect(Collectors.toList()));
    }

    private CompletableFuture<Void> notifyTruncateSegments(String scope, String stream, Map<Long, Long> streamCut,
                                                           String delegationToken, long requestId) {
        log.debug(requestId, "{}/{} truncating segments", scope, stream);
        return Futures.allOf(streamCut.entrySet().stream()
                .parallel()
                .map(segmentCut -> streamMetadataTasks.notifyTruncateSegment(scope, stream, segmentCut, delegationToken, requestId))
                .collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<Void> writeBack(TruncateStreamEvent event) {
        return streamMetadataTasks.writeEvent(event);
    }

    @Override
    public CompletableFuture<Boolean> hasTaskStarted(TruncateStreamEvent event) {
        return streamMetadataStore.getState(event.getScope(), event.getStream(), true, null, executor)
                                  .thenApply(state -> state.equals(State.TRUNCATING));
    }
}

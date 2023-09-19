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
import io.pravega.common.Exceptions;
import io.pravega.common.Timer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.TagLogger;
import io.pravega.controller.metrics.StreamMetrics;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.State;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.shared.NameUtils;
import io.pravega.shared.controller.event.DeleteStreamEvent;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.LoggerFactory;

/**
 * Request handler for performing scale operations received from requeststream.
 */
public class DeleteStreamTask implements StreamTask<DeleteStreamEvent> {

    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(DeleteStreamTask.class));

    private final StreamMetadataTasks streamMetadataTasks;
    private final StreamMetadataStore streamMetadataStore;
    private final BucketStore bucketStore;
    private final ScheduledExecutorService executor;

    public DeleteStreamTask(final StreamMetadataTasks streamMetadataTasks,
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
    public CompletableFuture<Void> execute(final DeleteStreamEvent request) {
        Timer timer = new Timer();
        String scope = request.getScope();
        String stream = request.getStream();
        long requestId = request.getRequestId();
        final OperationContext context = streamMetadataStore.createStreamContext(scope, stream, requestId);
        log.debug(requestId, "Deleting {}/{} stream", scope, stream);

        return streamMetadataStore.getCreationTime(scope, stream, context, executor)
            .thenAccept(creationTime -> Preconditions.checkArgument(request.getCreationTime() == 0 ||
                                          request.getCreationTime() == creationTime))
            .thenCompose(v -> streamMetadataStore.getState(scope, stream, true, context, executor))
                .thenComposeAsync(state -> {
                    // We should delete stream if its creating or sealed. 
                    // For streams in creating state, we may not have segments 
                    if (!state.equals(State.CREATING) && !state.equals(State.SEALED)) {
                        log.warn(requestId, "{}/{} stream not sealed", scope, stream);
                        return Futures.failedFuture(new RuntimeException("Stream not sealed"));
                    }
                    return deleteAssociatedStreams(scope, stream, requestId)
                            .thenCompose(v -> removeTagsFromIndex(context, scope, stream, requestId))
                            .thenCompose(v -> notifyAndDelete(context, scope, stream, requestId))
                            .thenAccept(v -> StreamMetrics.getInstance().controllerEventProcessorDeleteStreamEvent(timer.getElapsed()));
                }, executor)
                .exceptionally(e -> {
                    if (Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException) {
                        return null;
                    }
                    log.warn(requestId, "{}/{} stream delete workflow threw exception.", scope, stream, e);
                    throw new CompletionException(e);
                });
    }

    private CompletableFuture<Void> deleteAssociatedStreams(String scope, String stream, long requestId) {
        String markStream = NameUtils.getMarkStreamForStream(stream);
        OperationContext context = streamMetadataStore.createStreamContext(scope, markStream, requestId);
        return Futures.exceptionallyExpecting(notifyAndDelete(context, scope, markStream, requestId),
                e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, null);
    }

    private CompletableFuture<Void> removeTagsFromIndex(OperationContext context, String scope, String stream, long requestId) {
        return Futures.exceptionallyExpecting(streamMetadataStore.getConfiguration(scope, stream, context, executor)
                                                                 .thenCompose(cfg -> {
                                                                    return streamMetadataStore.removeTagsFromIndex(scope, stream, cfg.getTags(), context, executor);
                                                                }), e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, null);
    }

    private CompletableFuture<Void> notifyAndDelete(OperationContext context, String scope, String stream, long requestId) {
        log.info(requestId, "{}/{} deleting segments", scope, stream);
        return Futures.exceptionallyExpecting(streamMetadataStore.getAllSegmentIds(scope, stream, context, executor)
                .thenComposeAsync(allSegments -> 
                    streamMetadataTasks.notifyDeleteSegments(scope, stream, allSegments, 
                            streamMetadataTasks.retrieveDelegationToken(), requestId)),
                            e -> Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException, null)
                            .thenComposeAsync(x -> CompletableFuture.allOf(
                                    bucketStore.removeStreamFromBucketStore(BucketStore.ServiceType.RetentionService, 
                                    scope, stream, executor),
                                    bucketStore.removeStreamFromBucketStore(BucketStore.ServiceType.WatermarkingService,
                                            scope, stream, executor)), executor)
                            .thenComposeAsync(x -> streamMetadataStore.deleteStream(scope, stream, context,
                                    executor), executor);
    }

    @Override
    public CompletableFuture<Void> writeBack(DeleteStreamEvent event) {
        return streamMetadataTasks.writeEvent(event);
    }

    @Override
    public CompletableFuture<Boolean> hasTaskStarted(DeleteStreamEvent event) {
        return streamMetadataStore.getState(event.getScope(), event.getStream(), true, null, executor)
                                  .thenApply(state -> state.equals(State.SEALED));
    }
}

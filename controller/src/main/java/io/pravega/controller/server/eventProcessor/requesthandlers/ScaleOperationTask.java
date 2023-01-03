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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.Timer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.TagLogger;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.controller.metrics.StreamMetrics;
import io.pravega.controller.store.stream.EpochTransitionOperationExceptions;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.VersionedMetadata;
import io.pravega.controller.store.stream.State;
import io.pravega.controller.store.stream.records.EpochTransitionRecord;
import io.pravega.controller.store.stream.records.RecordHelper;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.shared.controller.event.ScaleOpEvent;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.LoggerFactory;

/**
 * Request handler for performing scale operations received from requeststream.
 */
public class ScaleOperationTask implements StreamTask<ScaleOpEvent> {
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(ScaleOperationTask.class));

    private final StreamMetadataTasks streamMetadataTasks;
    private final StreamMetadataStore streamMetadataStore;
    private final ScheduledExecutorService executor;

    public ScaleOperationTask(final StreamMetadataTasks streamMetadataTasks,
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
    public CompletableFuture<Void> execute(final ScaleOpEvent request) {
        Timer timer = new Timer();
        CompletableFuture<Void> result = new CompletableFuture<>();

        long requestId = request.getRequestId();
        final OperationContext context = streamMetadataStore.createStreamContext(request.getScope(), request.getStream(), 
                requestId);
        log.info(requestId, "starting scale request for {}/{} segments {} to new ranges {}",
                request.getScope(), request.getStream(), request.getSegmentsToSeal(), request.getNewRanges());

        runScale(request, request.isRunOnlyIfStarted(), context)
                .whenCompleteAsync((res, e) -> {
                    if (e != null) {
                        Throwable cause = Exceptions.unwrap(e);
                        if (cause instanceof RetriesExhaustedException) {
                            cause = cause.getCause();
                        }
                        if (cause instanceof EpochTransitionOperationExceptions.PreConditionFailureException) {
                            log.info(requestId, "processing scale request for {}/{} segments {} failed {}",
                                    request.getScope(), request.getStream(), request.getSegmentsToSeal(), 
                                    cause.getClass().getName());
                            result.complete(null);
                        } else {
                            log.warn(requestId, "processing scale request for {}/{} segments {} failed {}",
                                    request.getScope(), request.getStream(), request.getSegmentsToSeal(), cause);
                            result.completeExceptionally(cause);
                        }
                    } else {
                        log.info(requestId, "scale request for {}/{} segments {} to new ranges {} completed successfully.",
                                request.getScope(), request.getStream(), request.getSegmentsToSeal(), request.getNewRanges());
                        StreamMetrics.getInstance().controllerEventProcessorScaleStreamEvent(timer.getElapsed());

                        result.complete(null);
                    }
                }, executor);

        return result;
    }

    @Override
    public CompletableFuture<Void> writeBack(ScaleOpEvent event) {
        return streamMetadataTasks.writeEvent(event);
    }

    @VisibleForTesting
    public CompletableFuture<Void> runScale(ScaleOpEvent scaleInput, boolean isManualScale, OperationContext context) { // called upon event read from requeststream
        String scope = scaleInput.getScope();
        String stream = scaleInput.getStream();
        long requestId = scaleInput.getRequestId();
        // Note: There are two interesting cases for retry of partially completed workflow that are interesting:
        // if we crash before resetting epoch transition (complete scale step), then in rerun ETR will be rendered inconsistent
        // and deleted in startScale method.
        // if we crash before resetting state to active, in rerun (retry) we will find epoch transition to be empty and
        // hence reset the state here before attempting to start scale in idempotent fashion.
        return streamMetadataStore.getVersionedState(scope, stream, context, executor)
            .thenCompose(state -> streamMetadataStore.getEpochTransition(scope, stream, context, executor)
                    .thenCompose(record -> {
                        AtomicReference<VersionedMetadata<State>> reference = new AtomicReference<>(state);
                        CompletableFuture<VersionedMetadata<EpochTransitionRecord>> future = 
                                CompletableFuture.completedFuture(record);
                        if (record.getObject().equals(EpochTransitionRecord.EMPTY)) {
                            if (state.getObject().equals(State.SCALING)) {
                                future = streamMetadataStore.updateVersionedState(scope, stream, State.ACTIVE,
                                        state, context, executor).thenApply(updatedState -> {
                                            reference.set(updatedState);
                                            return record;
                                });
                            } 

                            if (isManualScale) {
                                log.info(requestId, 
                                        "Found empty epoch transition record, scale processing is already completed.");
                                return Futures.toVoid(future);
                            } else {
                                future = future.thenCompose(r -> streamMetadataStore.submitScale(scope, stream, 
                                        scaleInput.getSegmentsToSeal(),
                                        new ArrayList<>(scaleInput.getNewRanges()), scaleInput.getScaleTime(), 
                                        record, context, executor));
                            }
                        } else if (!RecordHelper.verifyRecordMatchesInput(scaleInput.getSegmentsToSeal(), scaleInput.getNewRanges(),
                                isManualScale, record.getObject())) {
                            // ensure that we process the event only if the input matches the epoch transition record.
                            // if its manual scale and the event doesnt match, it means the scale has already completed. 
                            // for auto scale, it means another scale is on going and just throw scale conflict exception 
                            // which will result in this event being postponed. 
                            if (isManualScale) {
                                log.info(requestId, "Scale for stream {}/{} for segments {} already completed.",
                                        scaleInput.getScope(), scaleInput.getStream(), scaleInput.getSegmentsToSeal());
                                return CompletableFuture.completedFuture(null);
                            } else {
                                log.info(requestId, 
                                        "Scale for stream {}/{} for segments {} cannot be started as another scale is ongoing.",
                                        scaleInput.getScope(), scaleInput.getStream(), scaleInput.getSegmentsToSeal());
                                throw new EpochTransitionOperationExceptions.ConflictException();
                            }
                        }
                        
                        return future
                                .thenCompose(versionedMetadata -> processScale(scope, stream, isManualScale, versionedMetadata, 
                                        reference.get(), context, requestId));
                    }));
    }
    
    private CompletableFuture<Void> processScale(String scope, String stream, boolean isManualScale,
                                                 VersionedMetadata<EpochTransitionRecord> metadata,
                                                 VersionedMetadata<State> state, OperationContext context,
                                                 long requestId) {
        return streamMetadataStore.updateVersionedState(scope, stream, State.SCALING, state, context, executor)
              .thenCompose(updatedState -> streamMetadataStore.startScale(scope, stream, isManualScale, metadata,
                      updatedState, context, executor)
                        .thenCompose(record -> streamMetadataTasks.processScale(scope, stream, metadata, 
                                context, requestId, streamMetadataStore)
                            .thenCompose(r ->  streamMetadataStore.updateVersionedState(scope, stream, 
                                    State.ACTIVE, updatedState, context, executor))
                            .thenAccept(y -> {
                                log.info(requestId, "scale processing for {}/{} epoch {} completed.", scope, stream, 
                                        record.getObject().getActiveEpoch());
                            })));
    }
    
    @Override
    public CompletableFuture<Boolean> hasTaskStarted(ScaleOpEvent event) {
        return streamMetadataStore.getState(event.getScope(), event.getStream(), true, null, executor)
                                  .thenApply(state -> state.equals(State.SCALING));
    }
}

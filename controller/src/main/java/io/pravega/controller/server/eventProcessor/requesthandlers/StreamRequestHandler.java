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

import io.pravega.common.tracing.TagLogger;
import io.pravega.controller.store.stream.EpochTransitionOperationExceptions;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.shared.controller.event.AutoScaleEvent;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.shared.controller.event.DeleteScopeEvent;
import io.pravega.shared.controller.event.DeleteStreamEvent;
import io.pravega.shared.controller.event.ScaleOpEvent;
import io.pravega.shared.controller.event.SealStreamEvent;
import io.pravega.shared.controller.event.TruncateStreamEvent;
import io.pravega.shared.controller.event.UpdateStreamEvent;
import io.pravega.shared.controller.event.CreateReaderGroupEvent;
import io.pravega.shared.controller.event.UpdateReaderGroupEvent;
import io.pravega.shared.controller.event.DeleteReaderGroupEvent;

import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;

public class StreamRequestHandler extends AbstractRequestProcessor<ControllerEvent> {
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(StreamRequestHandler.class));

    private final AutoScaleTask autoScaleTask;
    private final ScaleOperationTask scaleOperationTask;
    private final UpdateStreamTask updateStreamTask;
    private final SealStreamTask sealStreamTask;
    private final DeleteStreamTask deleteStreamTask;
    private final TruncateStreamTask truncateStreamTask;
    private final CreateReaderGroupTask createRGTask;
    private final DeleteReaderGroupTask deleteRGTask;
    private final UpdateReaderGroupTask updateRGTask;
    private final DeleteScopeTask deleteScopeTask;

    public StreamRequestHandler(AutoScaleTask autoScaleTask,
                                ScaleOperationTask scaleOperationTask,
                                UpdateStreamTask updateStreamTask,
                                SealStreamTask sealStreamTask,
                                DeleteStreamTask deleteStreamTask,
                                TruncateStreamTask truncateStreamTask,
                                CreateReaderGroupTask createRGTask,
                                DeleteReaderGroupTask deleteRGTask,
                                UpdateReaderGroupTask updateRGTask,
                                StreamMetadataStore streamMetadataStore,
                                DeleteScopeTask deleteScopeTask,
                                ScheduledExecutorService executor) {
        super(streamMetadataStore, executor);
        this.autoScaleTask = autoScaleTask;
        this.scaleOperationTask = scaleOperationTask;
        this.updateStreamTask = updateStreamTask;
        this.sealStreamTask = sealStreamTask;
        this.deleteStreamTask = deleteStreamTask;
        this.truncateStreamTask = truncateStreamTask;
        this.createRGTask = createRGTask;
        this.deleteRGTask = deleteRGTask;
        this.updateRGTask = updateRGTask;
        this.deleteScopeTask = deleteScopeTask;
    }
    
    @Override
    public CompletableFuture<Void> processAutoScaleRequest(AutoScaleEvent autoScaleEvent) {
        return autoScaleTask.execute(autoScaleEvent);
    }

    @Override
    public CompletableFuture<Void> processScaleOpRequest(ScaleOpEvent scaleOpEvent) {
        log.info(scaleOpEvent.getRequestId(), "Processing scale request for stream {}/{}", scaleOpEvent.getScope(), 
                scaleOpEvent.getStream());
        return withCompletion(scaleOperationTask, scaleOpEvent, scaleOpEvent.getScope(), scaleOpEvent.getStream(),
                OPERATION_NOT_ALLOWED_PREDICATE.or(e -> e instanceof EpochTransitionOperationExceptions.ConflictException))
                .thenAccept(v -> {
                    log.info(scaleOpEvent.getRequestId(), "Processing scale request for stream {}/{} complete",
                            scaleOpEvent.getScope(), scaleOpEvent.getStream());
                });
    }

    @Override
    public CompletableFuture<Void> processUpdateStream(UpdateStreamEvent updateStreamEvent) {
        log.info(updateStreamEvent.getRequestId(), "Processing update request for stream {}/{}",  
                updateStreamEvent.getScope(), updateStreamEvent.getStream());
        return withCompletion(updateStreamTask, updateStreamEvent, updateStreamEvent.getScope(), updateStreamEvent.getStream(),
                OPERATION_NOT_ALLOWED_PREDICATE)
                .thenAccept(v -> {
                    log.info(updateStreamEvent.getRequestId(), "Processing update request for stream {}/{} complete", 
                            updateStreamEvent.getScope(), updateStreamEvent.getStream());
                });
    }

    @Override
    public CompletableFuture<Void> processTruncateStream(TruncateStreamEvent truncateStreamEvent) {
        log.info(truncateStreamEvent.getRequestId(), "Processing truncate request for stream {}/{}", 
                truncateStreamEvent.getScope(), truncateStreamEvent.getStream());
        return withCompletion(truncateStreamTask, truncateStreamEvent, truncateStreamEvent.getScope(), truncateStreamEvent.getStream(),
                OPERATION_NOT_ALLOWED_PREDICATE)
                .thenAccept(v -> {
                    log.info(truncateStreamEvent.getRequestId(), "Processing truncate request for stream {}/{} complete", 
                            truncateStreamEvent.getScope(), truncateStreamEvent.getStream());
                });
    }

    @Override
    public CompletableFuture<Void> processSealStream(SealStreamEvent sealStreamEvent) {
        log.info(sealStreamEvent.getRequestId(), "Processing seal request for stream {}/{}", 
                sealStreamEvent.getScope(), sealStreamEvent.getStream());
        return withCompletion(sealStreamTask, sealStreamEvent, sealStreamEvent.getScope(), sealStreamEvent.getStream(),
                OPERATION_NOT_ALLOWED_PREDICATE)
                .thenAccept(v -> {
                    log.info(sealStreamEvent.getRequestId(), "Processing seal request for stream {}/{} complete", 
                            sealStreamEvent.getScope(), sealStreamEvent.getStream());
                });
    }

    @Override
    public CompletableFuture<Void> processDeleteStream(DeleteStreamEvent deleteStreamEvent) {
        log.info(deleteStreamEvent.getRequestId(), "Processing delete request for stream {}/{}", 
                deleteStreamEvent.getScope(), deleteStreamEvent.getStream());
        return withCompletion(deleteStreamTask, deleteStreamEvent, deleteStreamEvent.getScope(), deleteStreamEvent.getStream(),
                OPERATION_NOT_ALLOWED_PREDICATE)
                .thenAccept(v -> {
                    log.info(deleteStreamEvent.getRequestId(), "Processing delete request for stream {}/{} complete", 
                            deleteStreamEvent.getScope(), deleteStreamEvent.getStream());
                });
    }

    @Override
    public CompletableFuture<Void> processCreateReaderGroup(CreateReaderGroupEvent createRGEvent) {
        log.info(createRGEvent.getRequestId(), "Processing create request for ReaderGroup {}/{}", 
                createRGEvent.getScope(), createRGEvent.getRgName());
        return createRGTask.execute(createRGEvent).thenAccept(v -> {
            log.info(createRGEvent.getRequestId(), "Processing of create event for Reader Group {}/{} completed successfully.",
                    createRGEvent.getScope(), createRGEvent.getRgName());
        }).exceptionally(ex -> {
            log.error(createRGEvent.getRequestId(), String.format("Error processing create event for Reader Group %s/%s. Unexpected exception.",
                    createRGEvent.getScope(), createRGEvent.getRgName()), ex);
            throw new CompletionException(ex);
        });
    }

    @Override
    public CompletableFuture<Void> processDeleteReaderGroup(DeleteReaderGroupEvent deleteRGEvent) {
        log.info(deleteRGEvent.getRequestId(), "Processing delete request for ReaderGroup {}/{}",
                deleteRGEvent.getScope(), deleteRGEvent.getRgName());
        return deleteRGTask.execute(deleteRGEvent)
                .thenAccept(v -> log.info(deleteRGEvent.getRequestId(), "Processing of delete event for Reader Group {}/{} completed successfully.",
                    deleteRGEvent.getScope(), deleteRGEvent.getRgName()))
                .exceptionally(ex -> {
                    log.error(deleteRGEvent.getRequestId(), String.format("Error processing delete event for Reader Group %s/%s. Unexpected exception.",
                            deleteRGEvent.getScope(), deleteRGEvent.getRgName()), ex);
                    throw new CompletionException(ex);
                });
    }

    @Override
    public CompletableFuture<Void> processUpdateReaderGroup(UpdateReaderGroupEvent updateRGEvent) {
        log.info(updateRGEvent.getRequestId(), "Processing update request for ReaderGroup {}/{}",
                updateRGEvent.getScope(), updateRGEvent.getRgName());
        return updateRGTask.execute(updateRGEvent)
                .thenAccept(v -> {
                    log.info(updateRGEvent.getRequestId(), "Processing of update event for Reader Group {}/{} completed successfully.",
                            updateRGEvent.getScope(), updateRGEvent.getRgName());
                })
                .exceptionally(ex -> {
                    log.error(updateRGEvent.getRequestId(), String.format("Error processing update event for Reader Group %s/%s. Unexpected exception.",
                            updateRGEvent.getScope(), updateRGEvent.getRgName()), ex);
                    throw new CompletionException(ex);
                });
    }

    @Override
    public CompletableFuture<Void> processDeleteScopeRecursive(DeleteScopeEvent deleteScopeEvent) {
        log.info(deleteScopeEvent.getRequestId(), "Processing Delete Scope Recursive {}",
                deleteScopeEvent.getScope());
        return deleteScopeTask.execute(deleteScopeEvent)
                .thenAccept(v -> log.info(deleteScopeEvent.getRequestId(), "Processing of Delete Scope Recursive" +
                                " event for Scope {} completed successfully.",
                        deleteScopeEvent.getScope()))
                .exceptionally(ex -> {
                    log.error(deleteScopeEvent.getRequestId(), String.format("Error processing delete scope recursive" +
                                    "event for scope %s. Unexpected exception.",
                            deleteScopeEvent.getScope()), ex);
                    throw new CompletionException(ex);
                });
    }

}

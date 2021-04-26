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

import io.pravega.controller.store.stream.EpochTransitionOperationExceptions;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.shared.controller.event.AutoScaleEvent;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.shared.controller.event.DeleteStreamEvent;
import io.pravega.shared.controller.event.ScaleOpEvent;
import io.pravega.shared.controller.event.SealStreamEvent;
import io.pravega.shared.controller.event.TruncateStreamEvent;
import io.pravega.shared.controller.event.UpdateStreamEvent;
import io.pravega.shared.controller.event.CreateReaderGroupEvent;
import io.pravega.shared.controller.event.UpdateReaderGroupEvent;
import io.pravega.shared.controller.event.DeleteReaderGroupEvent;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

@Slf4j
public class StreamRequestHandler extends AbstractRequestProcessor<ControllerEvent> {
    private final AutoScaleTask autoScaleTask;
    private final ScaleOperationTask scaleOperationTask;
    private final UpdateStreamTask updateStreamTask;
    private final SealStreamTask sealStreamTask;
    private final DeleteStreamTask deleteStreamTask;
    private final TruncateStreamTask truncateStreamTask;
    private final CreateReaderGroupTask createRGTask;
    private final DeleteReaderGroupTask deleteRGTask;
    private final UpdateReaderGroupTask updateRGTask;

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
    }
    
    @Override
    public CompletableFuture<Void> processAutoScaleRequest(AutoScaleEvent autoScaleEvent) {
        return autoScaleTask.execute(autoScaleEvent);
    }

    @Override
    public CompletableFuture<Void> processScaleOpRequest(ScaleOpEvent scaleOpEvent) {
        log.info("Processing scale request {} for stream {}/{}", scaleOpEvent.getRequestId(), scaleOpEvent.getScope(), scaleOpEvent.getStream());
        return withCompletion(scaleOperationTask, scaleOpEvent, scaleOpEvent.getScope(), scaleOpEvent.getStream(),
                OPERATION_NOT_ALLOWED_PREDICATE.or(e -> e instanceof EpochTransitionOperationExceptions.ConflictException))
                .thenAccept(v -> {
                    log.info("Processing scale request {} for stream {}/{} complete", scaleOpEvent.getRequestId(), scaleOpEvent.getScope(), scaleOpEvent.getStream());
                });
    }

    @Override
    public CompletableFuture<Void> processUpdateStream(UpdateStreamEvent updateStreamEvent) {
        log.info("Processing update request {} for stream {}/{}", updateStreamEvent.getRequestId(), updateStreamEvent.getScope(), updateStreamEvent.getStream());
        return withCompletion(updateStreamTask, updateStreamEvent, updateStreamEvent.getScope(), updateStreamEvent.getStream(),
                OPERATION_NOT_ALLOWED_PREDICATE)
                .thenAccept(v -> {
                    log.info("Processing update request {} for stream {}/{} complete", updateStreamEvent.getRequestId(), updateStreamEvent.getScope(), updateStreamEvent.getStream());
                });
    }

    @Override
    public CompletableFuture<Void> processTruncateStream(TruncateStreamEvent truncateStreamEvent) {
        log.info("Processing truncate request {} for stream {}/{}", truncateStreamEvent.getRequestId(), truncateStreamEvent.getScope(), truncateStreamEvent.getStream());
        return withCompletion(truncateStreamTask, truncateStreamEvent, truncateStreamEvent.getScope(), truncateStreamEvent.getStream(),
                OPERATION_NOT_ALLOWED_PREDICATE)
                .thenAccept(v -> {
                    log.info("Processing truncate request {} for stream {}/{} complete", truncateStreamEvent.getRequestId(), truncateStreamEvent.getScope(), truncateStreamEvent.getStream());
                });
    }

    @Override
    public CompletableFuture<Void> processSealStream(SealStreamEvent sealStreamEvent) {
        log.info("Processing seal request {} for stream {}/{}", sealStreamEvent.getRequestId(), sealStreamEvent.getScope(), sealStreamEvent.getStream());
        return withCompletion(sealStreamTask, sealStreamEvent, sealStreamEvent.getScope(), sealStreamEvent.getStream(),
                OPERATION_NOT_ALLOWED_PREDICATE)
                .thenAccept(v -> {
                    log.info("Processing seal request {} for stream {}/{} complete", sealStreamEvent.getRequestId(), sealStreamEvent.getScope(), sealStreamEvent.getStream());
                });
    }

    @Override
    public CompletableFuture<Void> processDeleteStream(DeleteStreamEvent deleteStreamEvent) {
        log.info("Processing delete request {} for stream {}/{}", deleteStreamEvent.getRequestId(), deleteStreamEvent.getScope(), deleteStreamEvent.getStream());
        return withCompletion(deleteStreamTask, deleteStreamEvent, deleteStreamEvent.getScope(), deleteStreamEvent.getStream(),
                OPERATION_NOT_ALLOWED_PREDICATE)
                .thenAccept(v -> {
                    log.info("Processing delete request {} for stream {}/{} complete", deleteStreamEvent.getRequestId(), deleteStreamEvent.getScope(), deleteStreamEvent.getStream());
                });
    }

    @Override
    public CompletableFuture<Void> processCreateReaderGroup(CreateReaderGroupEvent createRGEvent) {
        log.info("Processing create request {} for ReaderGroup {}/{}", createRGEvent.getRequestId(),
                createRGEvent.getScope(), createRGEvent.getRgName());
        return createRGTask.execute(createRGEvent);
    }

    @Override
    public CompletableFuture<Void> processDeleteReaderGroup(DeleteReaderGroupEvent deleteRGEvent) {
        log.info("Processing delete request {} for ReaderGroup {}/{}",
                deleteRGEvent.getRequestId(), deleteRGEvent.getScope(), deleteRGEvent.getRgName());
        return deleteRGTask.execute(deleteRGEvent);
    }

    @Override
    public CompletableFuture<Void> processUpdateReaderGroup(UpdateReaderGroupEvent updateRGEvent) {
        log.info("Processing update request {} for ReaderGroup {}/{}",
                updateRGEvent.getRequestId(), updateRGEvent.getScope(), updateRGEvent.getRgName());
        return updateRGTask.execute(updateRGEvent);
    }
}

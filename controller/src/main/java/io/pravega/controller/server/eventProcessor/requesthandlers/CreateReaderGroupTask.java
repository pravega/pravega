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
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.retryable.RetryableException;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.util.RetryHelper;
import io.pravega.shared.controller.event.CreateReaderGroupEvent;
import io.pravega.shared.controller.event.RGStreamCutRecord;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/**
 * Request handler for executing a create operation for a ReaderGroup.
 */
@Slf4j
public class CreateReaderGroupTask implements ReaderGroupTask<CreateReaderGroupEvent> {

    private final StreamMetadataStore streamMetadataStore;
    private final StreamMetadataTasks streamMetadataTasks;
    private final ScheduledExecutorService executor;

    public CreateReaderGroupTask(final StreamMetadataTasks streamMetaTasks,
                                 final StreamMetadataStore streamMetaStore,
                                 final ScheduledExecutorService executor) {
        Preconditions.checkNotNull(streamMetaStore);
        Preconditions.checkNotNull(streamMetaTasks);
        Preconditions.checkNotNull(executor);
        this.streamMetadataStore = streamMetaStore;
        this.streamMetadataTasks = streamMetaTasks;
        this.executor = executor;
    }

    @Override
    public CompletableFuture<Void> execute(final CreateReaderGroupEvent request) {
        String scope = request.getScope();
        String readerGroup = request.getRgName();
        UUID readerGroupId = request.getReaderGroupId();
        ReaderGroupConfig config = getConfigFromEvent(request);
        return RetryHelper.withRetriesAsync(() -> streamMetadataStore.getReaderGroupId(scope, readerGroup)
                .thenCompose(rgId -> {
                    if (!rgId.equals(readerGroupId)) {
                        log.warn("Skipping processing of CreateReaderGroupEvent with stale UUID.");
                        return CompletableFuture.completedFuture(null);
                    }
                    return streamMetadataTasks.isRGCreationComplete(scope, readerGroup)
                            .thenCompose(complete -> {
                                if (!complete) {
                                    return Futures.toVoid(streamMetadataTasks.createReaderGroupTasks(scope, readerGroup,
                                            config, request.getCreateTimeStamp()));
                                }
                                return CompletableFuture.completedFuture(null);
                            });
        }), e -> Exceptions.unwrap(e) instanceof RetryableException, Integer.MAX_VALUE, executor);
    }

    private ReaderGroupConfig getConfigFromEvent(CreateReaderGroupEvent request) {
        Map<Stream, StreamCut> startStreamCut = getStreamCutMapFromRecord(request.getStartingStreamCuts());
        Map<Stream, StreamCut> endStreamCut = getStreamCutMapFromRecord(request.getEndingStreamCuts());
        ReaderGroupConfig conf = ReaderGroupConfig.builder()
                .groupRefreshTimeMillis(request.getGroupRefreshTimeMillis())
                .automaticCheckpointIntervalMillis(request.getAutomaticCheckpointIntervalMillis())
                .maxOutstandingCheckpointRequest(request.getMaxOutstandingCheckpointRequest())

                .retentionType(ReaderGroupConfig.StreamDataRetention.values()[request.getRetentionTypeOrdinal()])
                .startingStreamCuts(startStreamCut)
                .endingStreamCuts(endStreamCut).build();
        return ReaderGroupConfig.cloneConfig(conf, request.getReaderGroupId(), request.getGeneration());
    }

    private Map<Stream, StreamCut> getStreamCutMapFromRecord(final Map<String, RGStreamCutRecord> streamCutMap) {
        return streamCutMap.entrySet()
                .stream().collect(Collectors.toMap(e -> Stream.of(e.getKey()),
                        e -> io.pravega.client.control.impl.ModelHelper.generateStreamCut(Stream.of(e.getKey()).getScope(),
                                Stream.of(e.getKey()).getStreamName(),
                                e.getValue().getStreamCut())));
    }

}

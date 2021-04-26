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
import com.google.common.collect.ImmutableSet;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Stream;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.retryable.RetryableException;
import io.pravega.controller.store.stream.RGOperationContext;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.util.RetryHelper;
import io.pravega.shared.NameUtils;
import io.pravega.shared.controller.event.UpdateReaderGroupEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Request handler for executing a create operation for a ReaderGroup.
 */
@Slf4j
public class UpdateReaderGroupTask implements ReaderGroupTask<UpdateReaderGroupEvent> {

    private final StreamMetadataStore streamMetadataStore;
    private final StreamMetadataTasks streamMetadataTasks;
    private final ScheduledExecutorService executor;

    public UpdateReaderGroupTask(final StreamMetadataTasks streamMetaTasks,
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
    public CompletableFuture<Void> execute(final UpdateReaderGroupEvent request) {
        String scope = request.getScope();
        String readerGroup = request.getRgName();
        long requestId = request.getRequestId();
        long generation = request.getGeneration();
        UUID readerGroupId = request.getReaderGroupId();
        boolean isTransition = request.isTransitionToFromSubscriber();
        ImmutableSet<String> streamsToBeUnsubscribed = request.getRemoveStreams();
        final RGOperationContext context = streamMetadataStore.createRGContext(scope, readerGroup);

        return RetryHelper.withRetriesAsync(() -> streamMetadataStore.getReaderGroupId(scope, readerGroup)
                .thenCompose(id -> {
                if (!id.equals(readerGroupId)) {
                        log.warn("Skipping processing of Reader Group update request {} as UUID did not match.", requestId);
                        return CompletableFuture.completedFuture(null);
                }
                return streamMetadataStore.getReaderGroupConfigRecord(scope, readerGroup, context, executor)
                       .thenCompose(rgConfigRecord -> {
                           if (rgConfigRecord.getObject().getGeneration() != generation) {
                               log.warn("Skipping processing of Reader Group update request {} as generation did not match.", requestId);
                               return CompletableFuture.completedFuture(null);
                           }
                           if (rgConfigRecord.getObject().isUpdating()) {
                               if (isTransition) {
                                   // update Stream metadata tables, only if RG is a Subscriber
                                   Iterator<String> streamIter = rgConfigRecord.getObject().getStartingStreamCuts().keySet().iterator();
                                   String scopedRGName = NameUtils.getScopedReaderGroupName(scope, readerGroup);
                                   Iterator<String> removeStreamsIter = streamsToBeUnsubscribed.stream().iterator();
                                   return Futures.loop(() -> removeStreamsIter.hasNext(), () -> {
                                       Stream stream = Stream.of(removeStreamsIter.next());
                                       return streamMetadataStore.deleteSubscriber(stream.getScope(),
                                               stream.getStreamName(), scopedRGName, rgConfigRecord.getObject().getGeneration(), null, executor);
                                   }, executor)
                                   .thenCompose(v -> {
                                       // updated config suggests this is a subscriber RG so addSubscriber
                                       if (!ReaderGroupConfig.StreamDataRetention.NONE
                                           .equals(ReaderGroupConfig.StreamDataRetention.values()
                                                  [rgConfigRecord.getObject().getRetentionTypeOrdinal()])) {
                                           return Futures.loop(() -> streamIter.hasNext(), () -> {
                                                  Stream stream = Stream.of(streamIter.next());
                                                  return streamMetadataStore.addSubscriber(stream.getScope(),
                                                         stream.getStreamName(), scopedRGName, rgConfigRecord.getObject().getGeneration(),
                                                         null, executor);
                                           }, executor);
                                       }
                                       return CompletableFuture.completedFuture(null);
                                   })
                                   .thenCompose(v -> streamMetadataStore.completeRGConfigUpdate(scope, readerGroup, rgConfigRecord, context, executor));
                               }
                               return streamMetadataStore.completeRGConfigUpdate(scope, readerGroup, rgConfigRecord, context, executor);
                           }
                           return CompletableFuture.completedFuture(null);
                       });
        }), e -> Exceptions.unwrap(e) instanceof RetryableException, Integer.MAX_VALUE, executor);
    }
}
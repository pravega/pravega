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
import io.pravega.common.Timer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.TagLogger;
import io.pravega.controller.metrics.StreamMetrics;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.shared.NameUtils;
import io.pravega.shared.controller.event.DeleteReaderGroupEvent;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Request handler for executing a delete operation for a ReaderGroup.
 */
public class DeleteReaderGroupTask implements ReaderGroupTask<DeleteReaderGroupEvent> {
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(DeleteReaderGroupTask.class));

    private final StreamMetadataStore streamMetadataStore;
    private final StreamMetadataTasks streamMetadataTasks;
    private final ScheduledExecutorService executor;

    public DeleteReaderGroupTask(final StreamMetadataTasks streamMetaTasks,
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
    public CompletableFuture<Void> execute(final DeleteReaderGroupEvent request) {
      Timer timer = new Timer();
      String scope = request.getScope();
      String readerGroup = request.getRgName();
      long requestId = request.getRequestId();
      UUID readerGroupId = request.getReaderGroupId();
      final OperationContext context = streamMetadataStore.createRGContext(scope, readerGroup, requestId);

      return streamMetadataStore.getReaderGroupId(scope, readerGroup, context, executor)
              .thenCompose(id -> {
                  if (!id.equals(readerGroupId)) {
                      log.warn(requestId, "Skipping processing of Reader Group delete request {} as UUIDs did not match.", 
                              requestId);
                      return CompletableFuture.completedFuture(null);
                  }
                  return streamMetadataStore.getReaderGroupConfigRecord(scope, readerGroup, context, executor)
                                            .thenCompose(configRecord -> {
                                                if (!ReaderGroupConfig.StreamDataRetention.values()[configRecord
                                                        .getObject().getRetentionTypeOrdinal()]
                                                        .equals(ReaderGroupConfig.StreamDataRetention.NONE)) {
                                                    String scopedRGName = NameUtils.getScopedReaderGroupName(scope, 
                                                            readerGroup);
                                                    // update Stream metadata tables, if RG is a Subscriber
                                                    Iterator<String> streamIter = configRecord
                                                            .getObject().getStartingStreamCuts().keySet().iterator();
                                                    return Futures.loop(streamIter::hasNext, () -> {
                                                        Stream stream = Stream.of(streamIter.next());
                                                        OperationContext streamContext = streamMetadataStore.createStreamContext(
                                                                stream.getScope(), stream.getStreamName(), requestId);

                                                        return streamMetadataStore.deleteSubscriber(stream.getScope(),
                                                                stream.getStreamName(), scopedRGName, 
                                                                configRecord.getObject().getGeneration(),
                                                                streamContext, executor);
                                                    }, executor);
                                                }
                                                return CompletableFuture.completedFuture(null);
                                            }).thenCompose(v -> {
                              String rgStreamContext = NameUtils.getStreamForReaderGroup(readerGroup);
                              OperationContext streamContext = streamMetadataStore.createStreamContext(scope, 
                                      rgStreamContext, requestId);
                              return streamMetadataTasks.sealStream(scope, rgStreamContext, streamContext)
                                                        .thenCompose(z -> streamMetadataTasks.deleteStream(scope,
                                                                rgStreamContext, streamContext));
                          }).thenCompose(v1 -> streamMetadataStore.deleteReaderGroup(scope, readerGroup, context, executor))
                          .thenAccept(v -> StreamMetrics.getInstance().controllerEventProcessorDeleteReaderGroupEvent(timer.getElapsed()));
              });

    }
}

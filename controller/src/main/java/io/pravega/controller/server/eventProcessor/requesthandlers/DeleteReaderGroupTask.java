/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.eventProcessor.requesthandlers;

import com.google.common.base.Preconditions;

import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Stream;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.stream.RGOperationContext;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.shared.NameUtils;
import io.pravega.shared.controller.event.DeleteReaderGroupEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Request handler for executing a delete operation for a ReaderGroup.
 */
@Slf4j
public class DeleteReaderGroupTask implements ReaderGroupTask<DeleteReaderGroupEvent> {

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
      String scope = request.getScope();
      String readerGroup = request.getRgName();
      long requestId = request.getRequestId();
      UUID readerGroupId = request.getReaderGroupId();
      long generation = request.getGeneration();
      final RGOperationContext context = streamMetadataStore.createRGContext(scope, readerGroup);
      return streamMetadataStore.getReaderGroupId(scope, readerGroup)
              .thenCompose(id -> {
               if (!id.equals(readerGroupId)) {
                      log.warn("Skipping processing of Reader Group delete request {} as UUIDs did not match.", requestId);
                      return CompletableFuture.completedFuture(null);
               }
               return streamMetadataStore.getReaderGroupConfigRecord(scope, readerGroup, context, executor)
                       .thenCompose(configRecord -> {
                       if (configRecord.getObject().getGeneration() != generation) {
                           log.warn("Skipping processing of Reader Group delete request {} as generation did not match.", requestId);
                           return CompletableFuture.completedFuture(null);
                       }
                       if (!ReaderGroupConfig.StreamDataRetention.values()[configRecord.getObject().getRetentionTypeOrdinal()]
                              .equals(ReaderGroupConfig.StreamDataRetention.NONE)) {
                              String scopedRGName = NameUtils.getScopedReaderGroupName(scope, readerGroup);
                              // update Stream metadata tables, if RG is a Subscriber
                              Iterator<String> streamIter = configRecord.getObject().getStartingStreamCuts().keySet().iterator();
                              return Futures.loop(() -> streamIter.hasNext(), () -> {
                                     Stream stream = Stream.of(streamIter.next());
                                     return streamMetadataStore.deleteSubscriber(stream.getScope(),
                                            stream.getStreamName(), scopedRGName, configRecord.getObject().getGeneration(), null, executor);
                                 }, executor);
                              }
                              return CompletableFuture.completedFuture(null);
                       })
                          .thenCompose(v -> streamMetadataTasks.sealStream(scope, NameUtils.getStreamForReaderGroup(readerGroup), null))
                             .thenCompose(v -> streamMetadataTasks.deleteStream(scope,
                                  NameUtils.getStreamForReaderGroup(readerGroup), null)
                                  .thenCompose(v1 -> streamMetadataStore.deleteReaderGroup(scope, readerGroup, context, executor)));
              });

    }
}

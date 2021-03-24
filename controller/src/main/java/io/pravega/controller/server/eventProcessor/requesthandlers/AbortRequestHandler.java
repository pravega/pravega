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
import io.pravega.common.Timer;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.eventProcessor.impl.SerializedRequestHandler;
import io.pravega.controller.metrics.TransactionMetrics;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.records.StreamSegmentRecord;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.shared.controller.event.AbortEvent;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

/**
 * This actor processes commit txn events.
 * It does the following 2 operations in order.
 * 1. Send abort txn message to active segments of the stream.
 * 2. Change txn state from aborting to aborted.
 */
@Slf4j
public class AbortRequestHandler extends SerializedRequestHandler<AbortEvent> {
    private final StreamMetadataStore streamMetadataStore;
    private final StreamMetadataTasks streamMetadataTasks;
    private final ScheduledExecutorService executor;

    private final BlockingQueue<AbortEvent> processedEvents;

    @VisibleForTesting
    public AbortRequestHandler(final StreamMetadataStore streamMetadataStore,
                               final StreamMetadataTasks streamMetadataTasks,
                               final ScheduledExecutorService executor,
                               final BlockingQueue<AbortEvent> queue) {
        super(executor);
        this.streamMetadataStore = streamMetadataStore;
        this.streamMetadataTasks = streamMetadataTasks;
        this.executor = executor;
        this.processedEvents = queue;
    }

    public AbortRequestHandler(final StreamMetadataStore streamMetadataStore,
                               final StreamMetadataTasks streamMetadataTasks,
                               final ScheduledExecutorService executor) {
        super(executor);
        this.streamMetadataStore = streamMetadataStore;
        this.streamMetadataTasks = streamMetadataTasks;
        this.executor = executor;
        this.processedEvents = null;
    }

    @Override
    public CompletableFuture<Void> processEvent(AbortEvent event) {
        String scope = event.getScope();
        String stream = event.getStream();
        int epoch = event.getEpoch();
        UUID txId = event.getTxid();
        Timer timer = new Timer();
        OperationContext context = streamMetadataStore.createContext(scope, stream);
        log.debug("Aborting transaction {} on stream {}/{}", event.getTxid(), event.getScope(), event.getStream());

        return Futures.toVoid(streamMetadataStore.getSegmentsInEpoch(event.getScope(), event.getStream(), epoch, context, executor)
                                                 .thenApply(segments -> segments.stream().map(StreamSegmentRecord::segmentId)
                                                                        .collect(Collectors.toList()))
                .thenCompose(segments -> streamMetadataTasks.notifyTxnAbort(scope, stream, segments, txId))
                .thenCompose(x -> streamMetadataStore.abortTransaction(scope, stream, txId, context, executor))
                .whenComplete((result, error) -> {
                    if (error != null) {
                        log.error("Failed aborting transaction {} on stream {}/{}", event.getTxid(),
                                event.getScope(), event.getStream());
                        TransactionMetrics.getInstance().abortTransactionFailed(scope, stream, event.getTxid().toString());
                    } else {
                        log.debug("Successfully aborted transaction {} on stream {}/{}", event.getTxid(),
                                event.getScope(), event.getStream());
                        if (processedEvents != null) {
                            processedEvents.offer(event);
                        }
                        TransactionMetrics.getInstance().abortTransaction(scope, stream, timer.getElapsed());
                    }
                }));
    }
}

/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.controller.server.eventProcessor;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.common.util.Retry;
import io.pravega.controller.eventProcessor.impl.EventProcessor;
import io.pravega.controller.retryable.RetryableException;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.stream.Position;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

/**
 * This actor processes commit txn events.
 * It does the following 2 operations in order.
 * 1. Send abort txn message to active segments of the stream.
 * 2. Change txn state from aborting to aborted.
 */
@Slf4j
public class AbortEventProcessor extends EventProcessor<AbortEvent> {
    private final StreamMetadataStore streamMetadataStore;
    private final HostControllerStore hostControllerStore;
    private final ScheduledExecutorService executor;
    private final SegmentHelper segmentHelper;
    private final ConnectionFactory connectionFactory;
    private final BlockingQueue<AbortEvent> processedEvents;

    @VisibleForTesting
    public AbortEventProcessor(final StreamMetadataStore streamMetadataStore,
                               final HostControllerStore hostControllerStore,
                               final ScheduledExecutorService executor,
                               final SegmentHelper segmentHelper,
                               final ConnectionFactory connectionFactory,
                               final BlockingQueue<AbortEvent> queue) {
        this.streamMetadataStore = streamMetadataStore;
        this.hostControllerStore = hostControllerStore;
        this.segmentHelper = segmentHelper;
        this.executor = executor;
        this.connectionFactory = connectionFactory;
        this.processedEvents = queue;
    }

    public AbortEventProcessor(final StreamMetadataStore streamMetadataStore,
                               final HostControllerStore hostControllerStore,
                               final ScheduledExecutorService executor,
                               final SegmentHelper segmentHelper,
                               final ConnectionFactory connectionFactory) {
        this.streamMetadataStore = streamMetadataStore;
        this.hostControllerStore = hostControllerStore;
        this.segmentHelper = segmentHelper;
        this.executor = executor;
        this.connectionFactory = connectionFactory;
        this.processedEvents = null;
    }

    @Override
    protected void process(AbortEvent event, Position position) {
        String scope = event.getScope();
        String stream = event.getStream();
        UUID txId = event.getTxid();
        OperationContext context = streamMetadataStore.createContext(scope, stream);
        log.debug("Aborting transaction {} on stream {}/{}", event.getTxid(), event.getScope(), event.getStream());

        streamMetadataStore.getActiveSegments(event.getScope(), event.getStream(), context, executor)
                .thenCompose(segments ->
                        FutureHelpers.allOfWithResults(
                                segments.stream()
                                        .parallel()
                                        .map(segment -> notifyAbortToHost(scope, stream, segment.getNumber(), txId))
                                        .collect(Collectors.toList())))
                .thenCompose(x -> streamMetadataStore.abortTransaction(scope, stream, txId, context, executor))
                .whenComplete((result, error) -> {
                    if (error != null) {
                        log.error("Failed aborting transaction {} on stream {}/{}", event.getTxid(),
                                event.getScope(), event.getStream());
                    } else {
                        log.debug("Successfully aborted transaction {} on stream {}/{}", event.getTxid(),
                                event.getScope(), event.getStream());
                        if (processedEvents != null) {
                            processedEvents.offer(event);
                        }
                    }
                }).join();
    }

    private CompletableFuture<Controller.TxnStatus> notifyAbortToHost(final String scope, final String stream, final int segmentNumber, final UUID txId) {
        final long retryInitialDelay = 100;
        final int retryMultiplier = 10;
        final int retryMaxAttempts = 100;
        final long retryMaxDelay = 100000;

        return Retry.withExpBackoff(retryInitialDelay, retryMultiplier, retryMaxAttempts, retryMaxDelay)
                .retryWhen(RetryableException::isRetryable)
                .throwingOn(RuntimeException.class)
                .runAsync(() -> segmentHelper.abortTransaction(scope,
                        stream,
                        segmentNumber,
                        txId,
                        this.hostControllerStore,
                        this.connectionFactory), executor);
    }
}

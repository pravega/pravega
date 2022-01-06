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
package io.pravega.controller.server.bucket;

import io.pravega.client.stream.Stream;
import io.pravega.common.hash.RandomFactory;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.common.tracing.TagLogger;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.controller.util.RetryHelper;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

public class PeriodicTxnGC {
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(PeriodicTxnGC.class));

    private final StreamTransactionMetadataTasks streamTxnMetadataTasks;
    private final StreamMetadataStore streamMetadataStore;
    private final ScheduledExecutorService executor;
    private final RequestTracker requestTracker;
    private final Supplier<Long> requestIdGenerator = RandomFactory.create()::nextLong;

    public PeriodicTxnGC(StreamMetadataStore streamMetadataStore, StreamTransactionMetadataTasks streamTxnMetadataTasks,
                         ScheduledExecutorService executor,
                         RequestTracker requestTracker) {
        this.streamTxnMetadataTasks = streamTxnMetadataTasks;
        this.streamMetadataStore = streamMetadataStore;
        this.executor = executor;
        this.requestTracker = requestTracker;
    }
    
    public CompletableFuture<Void> txnGC(Stream stream) {
        // Track the new request for this automatic transaction GC.
        long requestId = requestIdGenerator.get();
        String requestDescriptor = RequestTracker.buildRequestDescriptor("truncateStream", stream.getScope(),
                stream.getStreamName());
        requestTracker.trackRequest(requestDescriptor, requestId);
        OperationContext context = streamMetadataStore.createStreamContext(stream.getScope(), stream.getStreamName(),
                requestId);

        log.info(requestId, "Periodic background processing for transaction GC called for stream {}/{}", stream.getScope(), stream.getStreamName());
        return RetryHelper.withRetriesAsync(() -> streamMetadataStore.getOpenTxns(stream.getScope(),
                stream.getStreamName(), context, executor).thenCompose(openTxnsData -> {
                             if (openTxnsData.isEmpty()) {
                                 // There are no open transactions on this Stream, so return
                                 return CompletableFuture.completedFuture(null);
                             }
                             return streamTxnMetadataTasks.transactionGC(stream.getScope(), stream.getStreamName(), openTxnsData, context);
                         })
                         .exceptionally(e -> {
                             log.warn(requestId, "Exception thrown while performing transaction garbage collection for stream {} ", stream, e);
                             throw new CompletionException(e);
                         }), RetryHelper.UNCONDITIONAL_PREDICATE, 5, executor)
                          .exceptionally(e -> {
                              log.warn(requestId, "Unable to perform transaction garbage collection for stream {}. " +
                                      "Ignoring, garbage collection will be re-attempted again in the next cycle.", stream, e);
                              return null;
                          }).thenRun(() -> requestTracker.untrackRequest(requestDescriptor));
    }
}

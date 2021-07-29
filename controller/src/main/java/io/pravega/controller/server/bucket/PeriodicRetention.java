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
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.util.RetryHelper;
import io.pravega.common.tracing.TagLogger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import org.slf4j.LoggerFactory;

public class PeriodicRetention {
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(PeriodicRetention.class));

    private final StreamMetadataTasks streamMetadataTasks;
    private final StreamMetadataStore streamMetadataStore;
    private final ScheduledExecutorService executor;
    private final RequestTracker requestTracker;
    private final Supplier<Long> requestIdGenerator = RandomFactory.create()::nextLong;

    public PeriodicRetention(StreamMetadataStore streamMetadataStore, StreamMetadataTasks streamMetadataTasks, 
                             ScheduledExecutorService executor,
                             RequestTracker requestTracker) {
        this.streamMetadataTasks = streamMetadataTasks;
        this.streamMetadataStore = streamMetadataStore;
        this.executor = executor;
        this.requestTracker = requestTracker;
    }
    
    public CompletableFuture<Void> retention(Stream stream) {

        // Track the new request for this automatic truncation.
        long requestId = requestIdGenerator.get();
        String requestDescriptor = RequestTracker.buildRequestDescriptor("truncateStream", stream.getScope(),
                stream.getStreamName());
        requestTracker.trackRequest(requestDescriptor, requestId);
        OperationContext context = streamMetadataStore.createStreamContext(stream.getScope(), stream.getStreamName(),
                requestId);

        log.debug(requestId, "Periodic background processing for retention called for stream {}/{}",
                stream.getScope(), stream.getStreamName());

        return RetryHelper.withRetriesAsync(() -> streamMetadataStore.getConfiguration(stream.getScope(), 
                stream.getStreamName(), context, executor)
                         .thenCompose(config -> streamMetadataTasks.retention(stream.getScope(), stream.getStreamName(),
                                 config.getRetentionPolicy(), System.currentTimeMillis(), context,
                                 this.streamMetadataTasks.retrieveDelegationToken()))
                         .exceptionally(e -> {
                             log.warn(requestId, "Exception thrown while performing auto retention for stream {} ",
                                     stream, e);
                             throw new CompletionException(e);
                         }), RetryHelper.UNCONDITIONAL_PREDICATE, 5, executor)
                          .exceptionally(e -> {
                              log.warn(requestId, "Unable to perform retention for stream {}. " +
                                      "Ignoring, retention will be attempted in next cycle.", stream, e);
                              return null;
                          }).thenRun(() -> requestTracker.untrackRequest(requestDescriptor));
    }
}

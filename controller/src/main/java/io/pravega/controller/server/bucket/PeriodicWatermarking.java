/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.bucket;

import io.pravega.client.stream.Stream;
import io.pravega.common.hash.RandomFactory;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.common.tracing.TagLogger;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.util.RetryHelper;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

public class PeriodicWatermarking {
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(PeriodicWatermarking.class));
    private final StreamMetadataStore streamMetadataStore;
    private final ScheduledExecutorService executor;

    public PeriodicWatermarking(StreamMetadataStore streamMetadataStore, ScheduledExecutorService executor) {
        this.streamMetadataStore = streamMetadataStore;
        this.executor = executor;
    }
    
    public CompletableFuture<Void> watermark(Stream stream) {
        OperationContext context = streamMetadataStore.createContext(stream.getScope(), stream.getStreamName());

        log.debug("Periodic background processing for watermarking called for stream {}/{}",
                stream.getScope(), stream.getStreamName());

        // TODO: shivesh
        // perform watermark computation
        // 0. retrieve previous watermark (if present)
        // 1. getAllMarks for stream --> only consider writers whose marked times are greater than last watermark.   
        // 2. Compute lower bound on time
        // 3. compute upper bound on position
        // 4. emit watermark into mark-segment
        // 5. opportunistically try to remove writers that have not participated in this watermark computation and 
        // is lagging by a certain degree. 
    }
}

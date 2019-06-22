/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import io.pravega.controller.store.stream.records.HistoryTimeSeries;
import io.pravega.controller.store.stream.records.HistoryTimeSeriesRecord;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public interface ExtendedStreamMetadataStore extends StreamMetadataStore {
    CompletableFuture<HistoryTimeSeriesRecord> getHistoryTimeSeriesRecord(final String scope, final String streamName,
                                                                          final int epoch, final OperationContext context,
                                                                          final Executor executor);

    CompletableFuture<Boolean> checkSegmentSealed(final String scope, final String streamName, final long segmentId,
                                                  final OperationContext context, final Executor executor);

    CompletableFuture<HistoryTimeSeries> getHistoryTimeSeriesChunkRecent(final String scope, final String streamName,
                                                                         final OperationContext context, final Executor executor);
}

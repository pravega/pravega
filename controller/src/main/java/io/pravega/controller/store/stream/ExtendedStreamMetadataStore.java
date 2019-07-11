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

/**
 * Extended Stream Metadata.
 */
public interface ExtendedStreamMetadataStore extends StreamMetadataStore {

    /**
     * Method to get the HistoryTimeSeriesRecord, given the epoch to which it corresponds to.
     *
     * @param scope       stream scope.
     * @param streamName  stream name.
     * @param epoch       epoch.
     * @param context     operation context.
     * @param executor    callers executor.
     * @return Completable future that, upon completion, holds the history record corresponding to request epoch.
     */
    CompletableFuture<HistoryTimeSeriesRecord> getHistoryTimeSeriesRecord(final String scope,
                                                                          final String streamName,
                                                                          final int epoch,
                                                                          final OperationContext context,
                                                                          final Executor executor);

    /**
     * Method to check if the given segment, whose ID is given, is sealed or not.
     *
     * @param scope       stream scope.
     * @param streamName  stream name.
     * @param segmentId   segment ID.
     * @param context     operation context.
     * @param executor    callers executor.
     * @return Completable future that, upon completion, holds the boolean representing whether the segment is sealed or not.
     */
    CompletableFuture<Boolean> checkSegmentSealed(final String scope,
                                                  final String streamName,
                                                  final long segmentId,
                                                  final OperationContext context,
                                                  final Executor executor);

    /**
     * Method to get the most recent chunk of the HistoryTimeSeries.
     *
     * @param scope       stream scope.
     * @param streamName  stream name.
     * @param context     operation context.
     * @param executor    callers executor.
     * @return Completable future that, upon completion, holds the most recent HistoryTimeSeries chunk.
     */
    CompletableFuture<HistoryTimeSeries> getHistoryTimeSeriesChunkRecent(final String scope,
                                                                         final String streamName,
                                                                         final OperationContext context,
                                                                         final Executor executor);
}

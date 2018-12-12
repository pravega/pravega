/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.containers;

import io.pravega.common.LoggerHelpers;
import io.pravega.common.concurrent.AbstractThreadPoolService;
import io.pravega.common.concurrent.CancellationToken;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.AsyncMap;
import io.pravega.segmentstore.server.EvictableMetadata;
import io.pravega.segmentstore.server.SegmentMetadata;
import com.google.common.base.Preconditions;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * Utility Service that performs ContainerMetadata cleanup on a bucket basis.
 */
@Slf4j
@ThreadSafe
class MetadataCleaner extends AbstractThreadPoolService {
    //region Private

    private final ContainerConfig config;
    private final EvictableMetadata metadata;
    private final AsyncMap<String, SegmentState> stateStore;
    private final Consumer<Collection<SegmentMetadata>> cleanupCallback;
    private final AtomicLong lastIterationSequenceNumber;
    private final CancellationToken stopToken;
    private final Object singleRunLock = new Object();
    @GuardedBy("singleRunLock")
    private CompletableFuture<Void> currentIteration = null;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the MetadataCleaner class.
     *
     * @param config          Container Configuration to use.
     * @param metadata        An EvictableMetadata to operate on.
     * @param stateStore      SegmentStateStore to serialize SegmentState in.
     * @param cleanupCallback A callback to invoke every time cleanup happened.
     * @param traceObjectId   An identifier to use for logging purposes. This will be included at the beginning of all
     *                        log calls initiated by this Service.
     * @param executor        The Executor to use for async callbacks and operations.
     */
    MetadataCleaner(ContainerConfig config, EvictableMetadata metadata, AsyncMap<String, SegmentState> stateStore,
                    Consumer<Collection<SegmentMetadata>> cleanupCallback, ScheduledExecutorService executor, String traceObjectId) {
        super(traceObjectId, executor);
        Preconditions.checkNotNull(metadata, "metadata");
        Preconditions.checkNotNull(stateStore, "stateStore");
        Preconditions.checkNotNull(cleanupCallback, "cleanupCallback");

        this.config = config;
        this.metadata = metadata;
        this.stateStore = stateStore;
        this.cleanupCallback = cleanupCallback;
        this.lastIterationSequenceNumber = new AtomicLong(metadata.getOperationSequenceNumber());
        this.stopToken = new CancellationToken();
    }

    //endregion

    //region AbstractThreadPooledService Implementation

    @Override
    protected Duration getShutdownTimeout() {
        return Duration.ofSeconds(30);
    }

    @Override
    protected CompletableFuture<Void> doRun() {
        return Futures.loop(
                () -> !this.stopToken.isCancellationRequested(),
                () -> delay().thenCompose(v -> runOnce()),
                this.executor);
    }

    @Override
    protected void doStop() {
        this.stopToken.requestCancellation();
        super.doStop();
    }

    //endregion

    /**
     * Executes one iteration of the MetadataCleaner. This ensures that there cannot be more than one concurrent executions of
     * such an iteration (whether it's from this direct call or from the regular MetadataCleaner invocation). If concurrent
     * invocations are made, then subsequent calls will be tied to the execution of the first, and will all complete at
     * the same time (even though there's only one executing).
     *
     * @return A CompletableFuture that, when completed, indicates that the operation completed (successfully or not).
     */
    CompletableFuture<Void> runOnce() {
        CompletableFuture<Void> result;
        synchronized (this.singleRunLock) {
            if (this.currentIteration != null) {
                // Some other iteration is in progress. Piggyback on that one and return when it is done.
                return this.currentIteration;
            } else {
                // No other iteration is running.
                this.currentIteration = new CompletableFuture<>();
                this.currentIteration.whenComplete((r, ex) -> {
                    // Unregister the current iteration when done.
                    synchronized (this.singleRunLock) {
                        this.currentIteration = null;
                    }
                });
                result = this.currentIteration;
            }
        }

        Futures.completeAfter(this::runOnceInternal, result);
        return result;
    }

    private CompletableFuture<Void> runOnceInternal() {
        long lastSeqNo = this.lastIterationSequenceNumber.getAndSet(this.metadata.getOperationSequenceNumber());
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "metadataCleanup", lastSeqNo);

        // Get candidates.
        Collection<SegmentMetadata> cleanupCandidates = this.metadata.getEvictionCandidates(lastSeqNo, this.config.getMaxConcurrentSegmentEvictionCount());

        // Serialize only those segments that are still alive (not deleted or merged - those will get removed anyway).
        val cleanupTasks = cleanupCandidates
                .stream()
                .filter(sm -> !sm.isDeleted() || !sm.isMerged())
                .map(sm -> this.stateStore.put(sm.getName(), new SegmentState(sm.getId(), sm), this.config.getSegmentMetadataExpiration()))
                .collect(Collectors.toList());

        return Futures
                .allOf(cleanupTasks)
                .thenRunAsync(() -> {
                    Collection<SegmentMetadata> evictedSegments = this.metadata.cleanup(cleanupCandidates, lastSeqNo);
                    this.cleanupCallback.accept(evictedSegments);
                    int evictedAttributes = this.metadata.cleanupExtendedAttributes(0, lastSeqNo);
                    LoggerHelpers.traceLeave(log, this.traceObjectId, "metadataCleanup", traceId, evictedSegments.size(), evictedAttributes);
                }, this.executor);
    }

    private CompletableFuture<Void> delay() {
        val result = Futures.delayedFuture(this.config.getSegmentMetadataExpiration(), this.executor);
        this.stopToken.register(result);
        return result;
    }
}
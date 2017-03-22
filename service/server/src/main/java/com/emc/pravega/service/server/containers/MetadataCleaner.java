/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.server.containers;

import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.common.concurrent.AbstractThreadPoolService;
import com.emc.pravega.common.concurrent.CancellationToken;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.util.AsyncMap;
import com.emc.pravega.service.server.SegmentMetadata;
import com.emc.pravega.service.server.UpdateableContainerMetadata;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * Utility Service that performs ContainerMetadata cleanup on a periodic basis.
 */
@Slf4j
class MetadataCleaner extends AbstractThreadPoolService {
    //region Private

    private final ContainerConfig config;
    private final UpdateableContainerMetadata metadata;
    private final AsyncMap<String, SegmentState> stateStore;
    private final Consumer<Collection<SegmentMetadata>> cleanupCallback;
    private final AtomicLong lastIterationSequenceNumber;
    private final CancellationToken stopToken;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the MetadataCleaner class.
     *
     * @param traceObjectId An identifier to use for logging purposes. This will be included at the beginning of all
     *                      log calls initiated by this Service.
     * @param executor      The Executor to use for async callbacks and operations.
     */
    MetadataCleaner(ContainerConfig config, UpdateableContainerMetadata metadata, AsyncMap<String, SegmentState> stateStore,
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
        return FutureHelpers.loop(
                () -> !this.stopToken.isCancellationRequested(),
                () -> delay().thenCompose(this::runOnce),
                this.executor);
    }

    @Override
    protected void doStop() {
        this.stopToken.requestCancellation();
        super.doStop();
    }

    //endregion

    @VisibleForTesting
    protected CompletableFuture<Void> runOnce(Void ignored) {
        long lastSeqNo = this.lastIterationSequenceNumber.getAndSet(this.metadata.getOperationSequenceNumber());
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "metadataCleanup", lastSeqNo);

        // Get candidates.
        Collection<SegmentMetadata> cleanupCandidates = this.metadata.getEvictionCandidates(lastSeqNo);

        // Serialize only those segments that are still alive (not deleted or merged - those will get removed anyway).
        val cleanupTasks = cleanupCandidates
                .stream()
                .filter(sm -> !sm.isDeleted() || !sm.isMerged())
                .map(sm -> this.stateStore.put(sm.getName(), new SegmentState(sm), this.config.getSegmentMetadataExpiration()))
                .collect(Collectors.toList());

        return FutureHelpers
                .allOf(cleanupTasks)
                .thenRun(() -> {
                    Collection<SegmentMetadata> evictedSegments = this.metadata.cleanup(cleanupCandidates, lastSeqNo);
                    this.cleanupCallback.accept(evictedSegments);
                    LoggerHelpers.traceLeave(log, this.traceObjectId, "metadataCleanup", traceId, evictedSegments.size());
                });
    }

    private CompletableFuture<Void> delay() {
        val result = FutureHelpers.delayedFuture(this.config.getSegmentMetadataExpiration(), this.executor);
        this.stopToken.register(result);
        return result;
    }
}
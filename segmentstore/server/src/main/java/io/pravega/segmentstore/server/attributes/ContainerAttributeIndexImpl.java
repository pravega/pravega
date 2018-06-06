/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.attributes;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.server.AttributeIndex;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.ContainerMetadata;
import io.pravega.segmentstore.server.OperationLog;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.storage.Cache;
import io.pravega.segmentstore.storage.CacheFactory;
import io.pravega.segmentstore.storage.Storage;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.concurrent.GuardedBy;
import lombok.extern.slf4j.Slf4j;

/**
 * Default implementation for ContainerAttributeIndex.
 */
@Slf4j
class ContainerAttributeIndexImpl implements ContainerAttributeIndex {
    //region Members

    private final ContainerMetadata containerMetadata;
    private final Storage storage;
    private final OperationLog operationLog;
    private final AttributeIndexConfig config;
    private final Cache cache;
    private final CacheManager cacheManager;
    @GuardedBy("attributeIndices")
    private final HashMap<Long, CompletableFuture<AttributeIndex>> attributeIndices;
    private final ScheduledExecutorService executor;
    private final String traceObjectId;
    private final AtomicBoolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ContainerAttributeIndexImpl class.
     *
     * @param containerMetadata The Segment Container's Metadata.
     * @param storage           A Storage adapter which can be used to access the Attribute Segment.
     * @param operationLog      An OperationLog that can be used to atomically update attributes for the main Segment.
     * @param cacheFactory      A CacheFactory that can be used to create Caches for storing data into.
     * @param cacheManager      The CacheManager to use for cache lifecycle management.
     * @param config            Attribute Index Configuration.
     * @param executor          An Executor to run async tasks.
     */
    ContainerAttributeIndexImpl(ContainerMetadata containerMetadata, Storage storage, OperationLog operationLog, CacheFactory cacheFactory,
                                CacheManager cacheManager, AttributeIndexConfig config, ScheduledExecutorService executor) {
        this.containerMetadata = Preconditions.checkNotNull(containerMetadata, "containerMetadata");
        this.storage = Preconditions.checkNotNull(storage, "storage");
        this.operationLog = Preconditions.checkNotNull(operationLog, "operationLog");
        this.cache = cacheFactory.getCache(String.format("Container_%d_Attributes", containerMetadata.getContainerId()));
        this.cacheManager = Preconditions.checkNotNull(cacheManager, "cacheManager");
        this.config = Preconditions.checkNotNull(config, "config");
        this.executor = Preconditions.checkNotNull(executor, "executor");
        this.attributeIndices = new HashMap<>();
        this.traceObjectId = String.format("ContainerAttributeIndex[%d]", containerMetadata.getContainerId());
        this.closed = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            closeIndices(null, false); // This will close all registered indices, without cleaning the cache.
            this.cache.close();
            log.info("{}: Closed.", this.traceObjectId);
        }
    }

    //endregion

    //region ContainerAttributeIndex Implementation

    @Override
    public CompletableFuture<AttributeIndex> forSegment(long streamSegmentId, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        SegmentMetadata sm = this.containerMetadata.getStreamSegmentMetadata(streamSegmentId);
        if (sm.isDeleted()) {
            return Futures.failedFuture(new StreamSegmentNotExistsException(sm.getName()));
        }

        // Figure out if we already have this AttributeIndex cached. If not, we need to initialize it.
        CompletableFuture<AttributeIndex> result;
        AtomicReference<SegmentAttributeIndex> toInitialize = new AtomicReference<>();
        synchronized (this.attributeIndices) {
            result = this.attributeIndices.computeIfAbsent(streamSegmentId, id -> {
                toInitialize.set(new SegmentAttributeIndex(sm, this.storage, this.operationLog, this.cache, this.config, this.executor));
                return new CompletableFuture<>();
            });
        }

        if (toInitialize.get() == null) {
            // We already have it cached - return its future (which should be already completed or will complete once
            // its initialization is done).
            return result;
        } else {
            try {
                // Need to initialize the AttributeIndex and complete the future that we just registered.
                // If this fails, we must fail the Future that we previously registered and unregister any pointers to
                // this index.
                toInitialize.get().initialize(timeout)
                            .thenRun(() -> this.cacheManager.register(toInitialize.get()))
                            .whenComplete((r, ex) -> {
                                if (ex == null) {
                                    result.complete(toInitialize.get());
                                } else {
                                    indexInitializationFailed(streamSegmentId, result, ex);
                                }
                            });
            } catch (Throwable ex) {
                if (!Exceptions.mustRethrow(ex)) {
                    indexInitializationFailed(streamSegmentId, result, ex);
                }
                throw ex;
            }
        }

        return result;
    }

    @Override
    public CompletableFuture<Void> delete(String segmentName, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return SegmentAttributeIndex.delete(segmentName, this.storage, timeout);
    }

    @Override
    public void cleanup(Collection<Long> segmentIds) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        closeIndices(segmentIds, true);
        log.info("{}: Cleaned up Attribute Indices for {} Segment(s).", this.traceObjectId, segmentIds == null ? "all" : segmentIds.size());
    }

    //endregion

    //region Helpers

    private void indexInitializationFailed(long streamSegmentId, CompletableFuture<AttributeIndex> result, Throwable ex) {
        synchronized (this.attributeIndices) {
            this.attributeIndices.remove(streamSegmentId);
        }

        result.completeExceptionally(ex);
    }

    private void closeIndices(Collection<Long> segmentIds, boolean cleanCache) {
        synchronized (this.attributeIndices) {
            if (segmentIds == null) {
                segmentIds = new ArrayList<>(this.attributeIndices.keySet());
            }

            for (long streamSegmentId : segmentIds) {
                CompletableFuture<AttributeIndex> indexFuture = this.attributeIndices.remove(streamSegmentId);
                if (indexFuture == null) {
                    continue;
                }

                if (Futures.isSuccessful(indexFuture)) {
                    // Already initialized. We should try as much as we can to clean up synchronously to prevent concurrent
                    // calls from creating new indices which could be affected by us cleaning the cache at the same time.
                    closeIndex((SegmentAttributeIndex) indexFuture.join(), cleanCache);
                } else {
                    // Close it when we're done initializing.
                    indexFuture.thenAcceptAsync(index -> closeIndex((SegmentAttributeIndex) index, cleanCache), this.executor);
                }
            }
        }
    }

    private void closeIndex(SegmentAttributeIndex ai, boolean cleanCache) {
        this.cacheManager.unregister(ai);
        ai.close(cleanCache);
    }

    //endregion
}

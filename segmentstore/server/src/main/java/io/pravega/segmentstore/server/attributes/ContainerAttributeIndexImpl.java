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
import io.pravega.segmentstore.server.ContainerMetadata;
import io.pravega.segmentstore.server.OperationLog;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.storage.Storage;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
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
    @GuardedBy("attributeIndices")
    private final HashMap<Long, CompletableFuture<AttributeIndex>> attributeIndices;
    private final ScheduledExecutorService executor;
    private final String traceObjectId;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ContainerAttributeIndexImpl class.
     *
     * @param containerMetadata The Segment Container's Metadata.
     * @param storage           A Storage adapter which can be used to access the Attribute Segment.
     * @param operationLog      An OperationLog that can be used to atomically update attributes for the main Segment.
     * @param config            Attribute Index Configuration.
     * @param executor          An Executor to run async tasks.
     */
    ContainerAttributeIndexImpl(ContainerMetadata containerMetadata, Storage storage, OperationLog operationLog,
                                AttributeIndexConfig config, ScheduledExecutorService executor) {
        this.containerMetadata = Preconditions.checkNotNull(containerMetadata, "containerMetadata");
        this.storage = Preconditions.checkNotNull(storage, "storage");
        this.operationLog = Preconditions.checkNotNull(operationLog, "operationLog");
        this.config = Preconditions.checkNotNull(config, "config");
        this.executor = Preconditions.checkNotNull(executor, "executor");
        this.attributeIndices = new HashMap<>();
        this.traceObjectId = String.format("ContainerAttributeIndex[%d]", containerMetadata.getContainerId());
    }

    //endregion

    //region ContainerAttributeIndex Implementation

    @Override
    public CompletableFuture<AttributeIndex> forSegment(long streamSegmentId, Duration timeout) {
        SegmentMetadata sm = this.containerMetadata.getStreamSegmentMetadata(streamSegmentId);
        if (sm.isDeleted()) {
            return Futures.failedFuture(new StreamSegmentNotExistsException(sm.getName()));
        }

        // Figure out if we already have this AttributeIndex cached. If not, we need to initialize it.
        CompletableFuture<AttributeIndex> result;
        AtomicReference<SegmentAttributeIndex> toInitialize = new AtomicReference<>();
        synchronized (this.attributeIndices) {
            result = this.attributeIndices.computeIfAbsent(streamSegmentId, id -> {
                toInitialize.set(new SegmentAttributeIndex(sm, this.storage, this.operationLog, this.config, this.executor));
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
    public CompletableFuture<Void> delete(SegmentMetadata sm, Duration timeout) {
        // Check if the Segment is deleted or merged (in other words, unusable or inaccessible).
        Preconditions.checkArgument(sm.isDeleted() || sm.isMerged(), "Segment %s is not deleted.", sm.getId());
        return SegmentAttributeIndex.delete(sm, this.storage, timeout);
    }

    @Override
    public void cleanup(Collection<Long> segmentIds) {
        synchronized (this.attributeIndices) {
            if (segmentIds == null) {
                segmentIds = new ArrayList<>(this.attributeIndices.keySet());
            }

            for (long streamSegmentId : segmentIds) {
                this.attributeIndices.remove(streamSegmentId);
            }
        }

        log.info("{}: Cleaned up Attribute Indices for {} Segment(s).", this.traceObjectId, segmentIds.size());
    }

    //endregion

    //region Helpers

    private void indexInitializationFailed(long streamSegmentId, CompletableFuture<AttributeIndex> result, Throwable ex) {
        synchronized (this.attributeIndices) {
            this.attributeIndices.remove(streamSegmentId);
        }
        result.completeExceptionally(ex);
    }

    //endregion
}

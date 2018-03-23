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
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.server.AttributeIndex;
import io.pravega.segmentstore.server.ContainerMetadata;
import io.pravega.segmentstore.server.OperationLog;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.storage.Storage;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

public class ContainerAttributeIndex {
    //region Members

    private final ContainerMetadata containerMetadata;
    private final Storage storage;
    private final OperationLog operationLog;
    private final ScheduledExecutorService executor;

    //endregion

    //region Constructor

    ContainerAttributeIndex(ContainerMetadata containerMetadata, Storage storage, OperationLog operationLog,
                            ScheduledExecutorService executor) {
        this.containerMetadata = Preconditions.checkNotNull(containerMetadata, "containerMetadata");
        this.storage = Preconditions.checkNotNull(storage, "storage");
        this.operationLog = Preconditions.checkNotNull(operationLog, "operationLog");
        this.executor = Preconditions.checkNotNull(executor, "executor");
    }

    //endregion

    //region Operations

    CompletableFuture<AttributeIndex> forSegment(long streamSegmentId, Duration timeout) {
        SegmentMetadata sm = this.containerMetadata.getStreamSegmentMetadata(streamSegmentId);
        if (sm.isDeleted() || sm.isMerged()) {
            return Futures.failedFuture(new StreamSegmentNotExistsException(sm.getName()));
        }

        SegmentAttributeIndex index = new SegmentAttributeIndex(sm, this.storage, this.operationLog, this.executor);
        return index.initialize(timeout)
                .thenApply(v -> index);
    }

    //endregion
}

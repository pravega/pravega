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
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.server.AttributeIndex;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

public class ContainerAttributeIndex {
    //region Members

    private final UpdateableContainerMetadata containerMetadata;
    private final Storage storage;
    private final ScheduledExecutorService executor;

    //endregion

    //region Constructor

    ContainerAttributeIndex(UpdateableContainerMetadata containerMetadata, Storage storage, ScheduledExecutorService executor) {
        this.containerMetadata = Preconditions.checkNotNull(containerMetadata, "containerMetadata");
        this.storage = Preconditions.checkNotNull(storage, "storage");
        this.executor = Preconditions.checkNotNull(executor, "executor");
    }

    //endregion

    //region Operations

    CompletableFuture<AttributeIndex> forSegment(long streamSegmentId, Duration timeout) {
        UpdateableSegmentMetadata sm = this.containerMetadata.getStreamSegmentMetadata(streamSegmentId);
        if (sm.isDeleted() || sm.isMerged()) {
            return Futures.failedFuture(new StreamSegmentNotExistsException(sm.getName()));
        }

        return openAttributeSegment(sm, timeout)
                .thenApply(handle -> new SegmentAttributeIndex(sm, handle, this.storage, this.executor));
    }

    private CompletableFuture<SegmentHandle> openAttributeSegment(UpdateableSegmentMetadata sm, Duration timeout) {
        String attributeSegmentName = StreamSegmentNameUtils.getAttributeSegmentName(sm.getName());
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return Futures.exceptionallyCompose(
                this.storage.openWrite(attributeSegmentName),
                ex -> {
                    if (Exceptions.unwrap(ex) instanceof StreamSegmentNotExistsException) {
                        // Attribute Segment does not exist yet. Create it now.
                        return this.storage.create(attributeSegmentName, null, timer.getRemaining())
                                           .thenComposeAsync(sp -> this.storage.openWrite(attributeSegmentName));
                    }

                    // Some other kind of exception.
                    return Futures.failedFuture(ex);
                });
    }

    //endregion
}

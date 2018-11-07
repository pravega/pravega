/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables;

import io.pravega.common.util.AsyncIterator;
import io.pravega.segmentstore.contracts.tables.IteratorItem;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import lombok.NonNull;

class TableBucketIterator implements AsyncIterator<IteratorItem<TableBucket>> {
    private final DirectSegmentAccess segment;
    private final ContainerKeyIndex keyIndex;
    private final AtomicReference<IteratorState> state;
    private final ScheduledExecutorService executor;

    TableBucketIterator(@NonNull DirectSegmentAccess segment, @NonNull ContainerKeyIndex keyIndex, IteratorState initialState,
                        @NonNull ScheduledExecutorService executor) {
        this.segment = segment;
        this.keyIndex = keyIndex;
        this.state = new AtomicReference<>(initialState);
        this.executor = executor;
    }

    //region AsyncIterator Implementation

    @Override
    public CompletableFuture<IteratorItem<TableBucket>> getNext() {
        IteratorState state = this.state.get();
        if(state != null && state.isEnd()){
            // We are done.
            return null;
        }

        // TODO
        return null;
    }

    //endregion
}

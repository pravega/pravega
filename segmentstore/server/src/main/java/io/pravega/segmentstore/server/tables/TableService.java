/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables;

import com.google.common.annotations.Beta;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.AsyncIterator;
import io.pravega.segmentstore.contracts.tables.IteratorArgs;
import io.pravega.segmentstore.contracts.tables.IteratorItem;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.SegmentContainerRegistry;
import io.pravega.segmentstore.server.store.SegmentContainerCollection;
import io.pravega.shared.segment.SegmentToContainerMapper;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Table Store Service, that delegates all Table-related operations to the appropriate components.
 */
@Beta
public class TableService extends SegmentContainerCollection implements TableStore {
    //region Constructor

    /**
     * Creates a new instance of the TableService class.
     *
     * @param segmentContainerRegistry The SegmentContainerRegistry to route requests to.
     * @param segmentToContainerMapper The SegmentToContainerMapper to use to map StreamSegments to Containers.
     */
    public TableService(SegmentContainerRegistry segmentContainerRegistry, SegmentToContainerMapper segmentToContainerMapper) {
        super(segmentContainerRegistry, segmentToContainerMapper);
    }

    //endregion

    //region TableStore Implementation

    @Override
    public CompletableFuture<Void> createSegment(String segmentName, boolean sorted, Duration timeout) {
        return invokeExtension(segmentName,
                e -> e.createSegment(segmentName, sorted, timeout),
                "createSegment", segmentName);
    }

    @Override
    public CompletableFuture<Void> deleteSegment(String segmentName, boolean mustBeEmpty, Duration timeout) {
        return invokeExtension(segmentName,
                e -> e.deleteSegment(segmentName, mustBeEmpty, timeout),
                "deleteSegment", segmentName, mustBeEmpty);
    }

    @Override
    public CompletableFuture<Void> merge(String targetSegmentName, String sourceSegmentName, Duration timeout) {
        return invokeExtension(targetSegmentName,
                e -> e.merge(targetSegmentName, sourceSegmentName, timeout),
                "merge", targetSegmentName, sourceSegmentName);
    }

    @Override
    public CompletableFuture<Void> seal(String segmentName, Duration timeout) {
        return invokeExtension(segmentName,
                e -> e.seal(segmentName, timeout),
                "seal", segmentName);
    }

    @Override
    public CompletableFuture<List<Long>> put(String segmentName, List<TableEntry> entries, Duration timeout) {
        return invokeExtension(segmentName,
                e -> e.put(segmentName, entries, timeout),
                "put", segmentName, entries.size());
    }

    @Override
    public CompletableFuture<Void> remove(String segmentName, Collection<TableKey> keys, Duration timeout) {
        return invokeExtension(segmentName,
                e -> e.remove(segmentName, keys, timeout),
                "remove", segmentName, keys.size());
    }

    @Override
    public CompletableFuture<List<TableEntry>> get(String segmentName, List<ArrayView> keys, Duration timeout) {
        return invokeExtension(segmentName,
                e -> e.get(segmentName, keys, timeout),
                "get", segmentName, keys.size());
    }

    @Override
    public CompletableFuture<AsyncIterator<IteratorItem<TableKey>>> keyIterator(String segmentName, IteratorArgs args) {
        return invokeExtension(segmentName,
                e -> e.keyIterator(segmentName, args),
                "get", segmentName, args);
    }

    @Override
    public CompletableFuture<AsyncIterator<IteratorItem<TableEntry>>> entryIterator(String segmentName, IteratorArgs args) {
        return invokeExtension(segmentName,
                e -> e.entryIterator(segmentName, args),
                "get", segmentName, args);
    }

    //endregion

    //region Helpers

    private <T> CompletableFuture<T> invokeExtension(String streamSegmentName, Function<ContainerTableExtension, CompletableFuture<T>> toInvoke,
                                                     String methodName, Object... logArgs) {
        return super.invoke(streamSegmentName,
                segmentContainer -> toInvoke.apply(segmentContainer.getExtension(ContainerTableExtension.class)),
                methodName, logArgs);
    }

    //endregion
}

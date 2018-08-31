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

import com.google.common.annotations.Beta;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.AsyncIterator;
import io.pravega.segmentstore.contracts.tables.IteratorState;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.contracts.tables.UpdateListener;
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
    public CompletableFuture<Void> createSegment(String segmentName, Duration timeout) {
        return invokePlugin(segmentName,
                plugin -> plugin.createSegment(segmentName, timeout),
                "createSegment", segmentName);
    }

    @Override
    public CompletableFuture<Void> deleteSegment(String segmentName, Duration timeout) {
        return invokePlugin(segmentName,
                plugin -> plugin.deleteSegment(segmentName, timeout),
                "deleteSegment", segmentName);
    }

    @Override
    public CompletableFuture<Void> merge(String targetSegmentName, String sourceSegmentName, Duration timeout) {
        return invokePlugin(targetSegmentName,
                plugin -> plugin.merge(targetSegmentName, sourceSegmentName, timeout),
                "merge", targetSegmentName, sourceSegmentName);
    }

    @Override
    public CompletableFuture<Void> seal(String segmentName, Duration timeout) {
        return invokePlugin(segmentName,
                plugin -> plugin.seal(segmentName, timeout),
                "seal", segmentName);
    }

    @Override
    public CompletableFuture<List<Long>> put(String segmentName, List<TableEntry> entries, Duration timeout) {
        return invokePlugin(segmentName,
                plugin -> plugin.put(segmentName, entries, timeout),
                "put", segmentName, entries.size());
    }

    @Override
    public CompletableFuture<Void> remove(String segmentName, Collection<TableKey> keys, Duration timeout) {
        return invokePlugin(segmentName,
                plugin -> plugin.remove(segmentName, keys, timeout),
                "remove", segmentName, keys.size());
    }

    @Override
    public CompletableFuture<List<TableEntry>> get(String segmentName, List<ArrayView> keys, Duration timeout) {
        return invokePlugin(segmentName,
                plugin -> plugin.get(segmentName, keys, timeout),
                "get", segmentName, keys.size());
    }

    @Override
    public CompletableFuture<AsyncIterator<IteratorItem<TableKey>>> keyIterator(String segmentName, IteratorState continuationToken, Duration timeout) {
        return invokePlugin(segmentName,
                plugin -> plugin.keyIterator(segmentName, continuationToken, timeout),
                "keyIterator", segmentName, continuationToken);
    }

    @Override
    public CompletableFuture<AsyncIterator<IteratorItem<TableEntry>>> entryIterator(String segmentName, IteratorState continuationToken, Duration timeout) {
        return invokePlugin(segmentName,
                plugin -> plugin.entryIterator(segmentName, continuationToken, timeout),
                "entryIterator", segmentName, continuationToken);
    }

    @Override
    public CompletableFuture<Void> registerListener(UpdateListener listener, Duration timeout) {
        return invokePlugin(listener.getSegmentName(),
                plugin -> plugin.registerListener(listener, timeout),
                "registerListener", listener.getSegmentName());
    }

    @Override
    public boolean unregisterListener(UpdateListener listener) {
        return invokePluginSync(listener.getSegmentName(),
                plugin -> plugin.unregisterListener(listener),
                "unregisterListener", listener.getSegmentName());
    }

    //endregion

    //region Helpers

    private <T> CompletableFuture<T> invokePlugin(String streamSegmentName, Function<ContainerTablePlugin, CompletableFuture<T>> toInvoke,
                                                  String methodName, Object... logArgs) {
        return super.invoke(streamSegmentName,
                segmentContainer -> toInvoke.apply(segmentContainer.getPlugin(ContainerTablePlugin.class)),
                methodName, logArgs);
    }

    private <T> T invokePluginSync(String streamSegmentName, Function<ContainerTablePlugin, T> toInvoke,
                                   String methodName, Object... logArgs) {
        return super.invokeSync(streamSegmentName,
                segmentContainer -> toInvoke.apply(segmentContainer.getPlugin(ContainerTablePlugin.class)),
                methodName, logArgs);
    }

    //endregion
}

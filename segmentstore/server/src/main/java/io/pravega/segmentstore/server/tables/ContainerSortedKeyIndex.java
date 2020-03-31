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

import io.pravega.common.util.ArrayView;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.tables.TableAttributes;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * Manages {@link SegmentSortedKeyIndex} instances.
 */
@RequiredArgsConstructor
class ContainerSortedKeyIndex {
    //region Members
    private final ConcurrentHashMap<Long, SegmentSortedKeyIndex> sortedKeyIndices = new ConcurrentHashMap<>();
    /**
     * A Function that will be invoked when a Table Segment's Sorted Key Index nodes need to be persisted.
     */
    @NonNull
    private final UpdateNodes updateNodes;
    /**
     * A Function that will be invoked when a Table Segment's Sorted Key Index nodes need to be deleted.
     */
    @NonNull
    private final DeleteNodes deleteNodes;
    /**
     * A Function that will be invoked when a Table Segment's Sorted Key Index nodes need to be retrieved.
     */
    @NonNull
    private final GetNodes getNodes;

    //endregion

    //region Operations

    /**
     * Determines whether the given {@link SegmentProperties} instance indicates the associated Table Segment is a sorted one.
     *
     * @param info The {@link SegmentProperties} to query.
     * @return True if Sorted Table Segment, false otherwise.
     */
    static boolean isSortedTableSegment(SegmentProperties info) {
        return info.getAttributes().getOrDefault(TableAttributes.SORTED, Attributes.BOOLEAN_FALSE) == Attributes.BOOLEAN_TRUE;
    }

    /**
     * Gets a {@link SegmentSortedKeyIndex} instance for the given Segment. If there is no {@link SegmentSortedKeyIndex}
     * currently associated with the given segment, it will be associated (and the same instance will be returned later).
     *
     * @param segmentId   The Id of the Segment.
     * @param segmentInfo A {@link SegmentProperties} associated with the segment.
     * @return A {@link SegmentSortedKeyIndex} if segmentInfo indicates a Sorted Table Segment, or
     * {@link SegmentSortedKeyIndex#noop()} otherwise.
     */
    SegmentSortedKeyIndex getSortedKeyIndex(long segmentId, SegmentProperties segmentInfo) {
        if (isSortedTableSegment(segmentInfo)) {
            return this.sortedKeyIndices.computeIfAbsent(segmentId, id -> createSortedKeyIndex(segmentInfo.getName()));
        } else {
            // Not a Sorted Table Segment.
            return SegmentSortedKeyIndex.noop();
        }
    }

    /**
     * Notifies that the indexed offset for a particular Segment Id has been changed.
     *
     * @param segmentId   The Segment Id whose indexed offset has changed.
     * @param indexOffset The new indexed offset. If -1, and if the given Segment is currently registered, it will be
     *                    de-registered (since -1 indicates it has been evicted).
     */
    void notifyIndexOffsetChanged(long segmentId, long indexOffset) {
        if (indexOffset < 0) {
            this.sortedKeyIndices.remove(segmentId);
        } else {
            val ski = this.sortedKeyIndices.getOrDefault(segmentId, null);
            if (ski != null) {
                // We only care about this if cached.
                ski.updateSegmentIndexOffset(indexOffset);
            }
        }
    }

    private SegmentSortedKeyIndex createSortedKeyIndex(String segmentName) {
        return new SegmentSortedKeyIndexImpl(
                (entries, timeout) -> this.updateNodes.apply(segmentName, entries, timeout),
                (keys, timeout) -> this.deleteNodes.apply(segmentName, keys, timeout),
                (keys, timeout) -> this.getNodes.apply(segmentName, keys, timeout));
    }

    //endregion

    //region Functional Interfaces

    @FunctionalInterface
    public interface UpdateNodes {
        CompletableFuture<?> apply(String segmentName, List<TableEntry> entries, Duration timeout);
    }

    @FunctionalInterface
    public interface DeleteNodes {
        CompletableFuture<?> apply(String segmentName, Collection<TableKey> keys, Duration timeout);
    }

    @FunctionalInterface
    public interface GetNodes {
        CompletableFuture<List<TableEntry>> apply(String segmentName, List<ArrayView> keys, Duration timeout);
    }

    //endregion
}

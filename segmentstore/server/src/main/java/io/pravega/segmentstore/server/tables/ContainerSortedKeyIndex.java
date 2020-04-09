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

import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.tables.TableAttributes;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * Manages {@link SegmentSortedKeyIndex} instances.
 */
@RequiredArgsConstructor
class ContainerSortedKeyIndex {
    //region Members

    static final KeyTranslator EXTERNAL_TRANSLATOR = KeyTranslator.partitioned((byte) 'E');
    static final KeyTranslator INTERNAL_TRANSLATOR = KeyTranslator.partitioned((byte) 'I');

    private final ConcurrentHashMap<Long, SegmentSortedKeyIndex> sortedKeyIndices = new ConcurrentHashMap<>();
    @NonNull
    private final SortedKeyIndexDataSource dataSource;
    @NonNull
    private final Executor executor;

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
     * Gets a {@link KeyTranslator} for the given segment.
     *
     * @param info              A {@link SegmentProperties} describing the segment,
     * @param isExternalRequest True if the request to be translated originated externally, false if internal (such as
     *                          from a {@link ContainerSortedKeyIndex}'s {@link SortedKeyIndexDataSource}.
     * @return A {@link KeyTranslator}, or null if the segment does not need key translation.
     */
    KeyTranslator getKeyTranslator(SegmentProperties info, boolean isExternalRequest) {
        if (!isSortedTableSegment(info)) {
            // Nothing to translate for non-sorted segments.
            return null;
        }

        return isExternalRequest ? EXTERNAL_TRANSLATOR : INTERNAL_TRANSLATOR;
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
        return new SegmentSortedKeyIndexImpl(segmentName, this.dataSource, this.executor);
    }

    //endregion
}

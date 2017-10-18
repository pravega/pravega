/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream.tables;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Data class for storing information about stream's truncation point.
 */
@AllArgsConstructor(access = AccessLevel.PACKAGE)
public class StreamTruncationRecord implements Serializable {
    public static final StreamTruncationRecord EMPTY = new StreamTruncationRecord(Collections.emptyMap(),
            Collections.emptyMap(), Collections.emptySet(), Collections.emptySet());

    /**
     * Stream cut that is applied as part of this truncation.
     */
    private final Map<Integer, Long> streamCut;

    /**
     * If a stream cut spans across multiple epochs then this map captures mapping of segments from the stream cut to
     * epochs they were found in closest to truncation point.
     * This data structure is used to find active segments wrt a stream cut.
     * So for example:
     * epoch 0: 0, 1
     * epoch 1: 0, 2, 3
     * epoch 2: 0, 2, 4, 5
     * epoch 3: 0, 4, 5, 6, 7
     *
     * Following is a valid stream cut {0/offset, 3/offset, 6/offset, 7/offset}
     * This spans from epoch 1 till epoch 3. Any request for segments at epoch 1 or 2 or 3 will need to have this stream cut
     * applied on it to find segments that are available for consumption.
     * Refer to TableHelper.getActiveSegmentsAt
     */
    private final Map<Integer, Integer> cutEpochMap;
    /**
     * All segments that have been deleted for this stream so far.
     */
    private final Set<Integer> deletedSegments;
    /**
     * Segments to delete as part of this truncation.
     * This is non empty while truncation is ongoing.
     * This is reset to empty once truncation completes by calling mergeDeleted method.
     */
    private final Set<Integer> toDelete;

    int getTruncationEpochLow() {
        return cutEpochMap.values().stream().min(Comparator.naturalOrder()).orElse(Integer.MIN_VALUE);
    }

    int getTruncationEpochHigh() {
        return cutEpochMap.values().stream().max(Comparator.naturalOrder()).orElse(Integer.MIN_VALUE);
    }

    public Map<Integer, Long> getStreamCut() {
        return Collections.unmodifiableMap(streamCut);
    }

    public Map<Integer, Integer> getCutEpochMap() {
        return Collections.unmodifiableMap(cutEpochMap);
    }

    public Set<Integer> getDeletedSegments() {
        return Collections.unmodifiableSet(deletedSegments);
    }

    public Set<Integer> getToDelete() {
        return Collections.unmodifiableSet(toDelete);
    }

    public StreamTruncationRecord mergeDeleted() {
        Set<Integer> deleted = new HashSet<>(deletedSegments);
        deleted.addAll(toDelete);
        return new StreamTruncationRecord(streamCut, cutEpochMap, deleted, Collections.emptySet());
    }
}

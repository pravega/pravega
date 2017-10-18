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

@AllArgsConstructor(access = AccessLevel.PACKAGE)
public class StreamTruncationRecord implements Serializable {
    public static final StreamTruncationRecord EMPTY = new StreamTruncationRecord(Collections.emptyMap(),
            Collections.emptyMap(), Collections.emptySet(), Collections.emptySet());

    private final Map<Integer, Long> streamCut;
    private final Map<Integer, Integer> cutSegmentEpochMap;
    private final Set<Integer> deletedSegments;
    private final Set<Integer> toDelete;

    int getTruncationEpochLow() {
        return cutSegmentEpochMap.values().stream().min(Comparator.naturalOrder()).orElse(Integer.MIN_VALUE);
    }

    int getTruncationEpochHigh() {
        return cutSegmentEpochMap.values().stream().max(Comparator.naturalOrder()).orElse(Integer.MIN_VALUE);
    }

    public Map<Integer, Long> getStreamCut() {
        return Collections.unmodifiableMap(streamCut);
    }

    public Map<Integer, Integer> getCutSegmentEpochMap() {
        return Collections.unmodifiableMap(cutSegmentEpochMap);
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
        return new StreamTruncationRecord(streamCut, cutSegmentEpochMap, deleted, Collections.emptySet());
    }
}

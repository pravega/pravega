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
import lombok.Data;

import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Data
@AllArgsConstructor(access = AccessLevel.PACKAGE)
public class StreamTruncationRecord implements Serializable {
    public static final StreamTruncationRecord EMPTY = new StreamTruncationRecord(Collections.emptyMap(),
            Collections.emptyMap(), Collections.emptySet());

    private final Map<Integer, Long> streamCut;
    private final Map<Integer, List<Integer>> truncationEpochMap;
    private final Set<Integer> deletedSegments;

    int getTruncationEpochLow() {
        return truncationEpochMap.keySet().stream().min(Comparator.naturalOrder()).orElse(Integer.MIN_VALUE);
    }

    int getTruncationEpochHigh() {
        return truncationEpochMap.keySet().stream().max(Comparator.naturalOrder()).orElse(Integer.MIN_VALUE);
    }
}

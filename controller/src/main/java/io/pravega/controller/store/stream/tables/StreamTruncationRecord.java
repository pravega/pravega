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

import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

@Data
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class StreamTruncationRecord implements Serializable {
    public static final StreamTruncationRecord EMPTY = new StreamTruncationRecord(Collections.emptyMap(), Integer.MIN_VALUE, Collections.emptySet());

    private final Map<Integer, Long> streamCut;
    private final int truncationEpoch;
    private final Set<Integer> deletedSegments;

    public static StreamTruncationRecord newStreamCut(StreamTruncationRecord previous, Map<Integer, Long> streamCut) {
        Preconditions.checkNotNull(previous);
        return new StreamTruncationRecord(streamCut, previous.getTruncationEpoch(), previous.getDeletedSegments());
    }

    public static StreamTruncationRecord truncated(StreamTruncationRecord previous, int truncationEpoch, Set<Integer> deletedSegments) {
        Preconditions.checkNotNull(previous);
        return new StreamTruncationRecord(previous.getStreamCut(), truncationEpoch, deletedSegments);
    }
}

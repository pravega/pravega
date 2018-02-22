/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.Stream;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;

import lombok.Data;
import lombok.Getter;
import lombok.NonNull;

/**
 * A set of segment/offset pairs for a single stream that represent a consistent position in the
 * stream. (IE: Segment 1 and 2 will not both appear in the set if 2 succeeds 1, and if 0 appears
 * and is responsible for keyspace 0-0.5 then other segments covering the range 0.5-1.0 will also be
 * included.)
 */
@Data
public class StreamCut implements Serializable {

    @NonNull
    private final Stream stream;
    @Getter
    @NonNull
    private final Map<Segment, Long> positions;

    @VisibleForTesting
    public boolean validate(Set<String> segmentNames) {
        for (Segment s: positions.keySet()) {
            if (!segmentNames.contains(s.getScopedName())) {
                return false;
            }
        }

        return true;
    }
}

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
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Implementation of {@link io.pravega.client.stream.StreamCut} interface. {@link StreamCutInternal} abstract class is
 * used as in intermediate class to make StreamCut instances opaque.
 */
@EqualsAndHashCode(callSuper = false)
@ToString
public class StreamCutImpl extends StreamCutInternal {
    private static final long serialVersionUID = 1L;

    private final Stream stream;

    private final Map<Segment, Long> positions;

    public StreamCutImpl(Stream stream, Map<Segment, Long> positions) {
        this.stream = stream;
        this.positions = positions;
    }

    @Override
    public Map<Segment, Long> getPositions() {
        return Collections.unmodifiableMap(positions);
    }

    @Override
    public Stream getStream() {
        return stream;
    }

    @Override
    public StreamCutInternal asImpl() {
        return this;
    }

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

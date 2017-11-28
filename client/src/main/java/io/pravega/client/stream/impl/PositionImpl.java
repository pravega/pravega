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

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.Position;
import io.pravega.common.util.ToStringUtils;
import java.io.ObjectStreamException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = false)
public class PositionImpl extends PositionInternal {

    private static final long serialVersionUID = 1L;
    private final Map<Segment, Long> ownedSegments;

    /**
     * Instantiates Position with current and future owned segments.
     *
     * @param ownedSegments Current segments that the position refers to.
     */
    public PositionImpl(Map<Segment, Long> ownedSegments) {
        this.ownedSegments = new HashMap<>(ownedSegments);
    }

    static PositionImpl createEmptyPosition() {
        return new PositionImpl(new HashMap<>());
    }

    @Override
    public Set<Segment> getOwnedSegments() {
        return Collections.unmodifiableSet(ownedSegments.keySet());
    }

    @Override
    public Map<Segment, Long> getOwnedSegmentsWithOffsets() {
        return Collections.unmodifiableMap(ownedSegments);
    }

    @Override
    public Set<Segment> getCompletedSegments() {
        return ownedSegments.entrySet()
            .stream()
            .filter(x -> x.getValue() < 0)
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    }

    @Override
    public Long getOffsetForOwnedSegment(Segment segmentId) {
        return ownedSegments.get(segmentId);
    }

    @Override
    public PositionImpl asImpl() {
        return this;
    }
    
    @Override
    public String toString() {
        return ToStringUtils.mapToString(ownedSegments);
    }
    
    public static Position fromString(String string) {
       return new PositionImpl(ToStringUtils.stringToMap(string, Segment::fromScopedName, Long::parseLong));
    }
    
    
    private Object writeReplace() throws ObjectStreamException {
        return new SerializedForm(toString());
    }
    
    @Data
    private static class SerializedForm  {
        private final String value;
        Object readResolve() throws ObjectStreamException {
            return Position.fromString(value);
        }
    }
}

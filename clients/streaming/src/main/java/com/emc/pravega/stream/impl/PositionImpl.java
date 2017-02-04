/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.stream.Segment;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode(callSuper = false)
@ToString
public class PositionImpl extends PositionInternal {

    private static final long serialVersionUID = 1L;
    private final Map<Segment, Long> ownedSegments;

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

}

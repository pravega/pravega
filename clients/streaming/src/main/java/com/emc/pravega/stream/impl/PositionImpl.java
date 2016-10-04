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

import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.PositionInternal;
import com.emc.pravega.stream.Segment;
import com.google.common.base.Preconditions;

import lombok.EqualsAndHashCode;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@EqualsAndHashCode
public class PositionImpl implements Position, PositionInternal {

    private static final long serialVersionUID = 1L;
    private final Map<Segment, Long> ownedLogs;
    private final Map<Segment, Long> futureOwnedLogs;

    public PositionImpl(Map<Segment, Long> ownedLogs, Map<Segment, Long> futureOwnedLogs) {
        this.ownedLogs = normalizeOwnedLogs(ownedLogs);
        this.futureOwnedLogs = futureOwnedLogs;
        Preconditions.checkArgument(isWellFormed(ownedLogs, futureOwnedLogs),
                                    "Owned and future logs must be coherent: " + this.toString());
    }

    private Map<Segment, Long> normalizeOwnedLogs(Map<Segment, Long> ownedLogs) {
        // find redundant segmentIds
        Set<Integer> predecessors = ownedLogs.keySet()
            .stream()
            .map(Segment::getPreviousNumber)
            .collect(Collectors.toSet());
        return ownedLogs.entrySet()
            .stream()
            .filter(x -> !predecessors.contains(x.getKey().getSegmentNumber()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private boolean isFutureLogsWellFormed(Map<Segment, Long> ownedLogs, Map<Segment, Long> futureOwnedLogs) {
        // every segment in futures should
        // 1. not be in ownedLogs, and
        // 2. have a predecessor in ownedLogs
        Set<Integer> current = ownedLogs.entrySet()
            .stream()
            .map(x -> x.getKey().getSegmentNumber())
            .collect(Collectors.toSet());
        return futureOwnedLogs.entrySet().stream().allMatch(x -> current.contains(x.getKey().getPreviousNumber())
                && !current.contains(x.getKey().getSegmentNumber()));
    }

    private boolean isOwnedLogsWellFormed(Map<Segment, Long> ownedLogs) {
        Set<Integer> current = ownedLogs.entrySet()
            .stream()
            .map(x -> x.getKey().getSegmentNumber())
            .collect(Collectors.toSet());

        // for every segment in ownedLogs, its predecessor should not be in ownedLogs
        return ownedLogs.entrySet().stream().allMatch(x -> !current.contains(x.getKey().getPreviousNumber()));
    }

    private boolean isWellFormed(Map<Segment, Long> ownedLogs, Map<Segment, Long> futureOwnedLogs) {
        return isFutureLogsWellFormed(ownedLogs, futureOwnedLogs) && isOwnedLogsWellFormed(ownedLogs);
    }

    @Override
    public Set<Segment> getOwnedSegments() {
        return Collections.unmodifiableSet(ownedLogs.keySet());
    }

    @Override
    public Map<Segment, Long> getOwnedSegmentsWithOffsets() {
        return Collections.unmodifiableMap(ownedLogs);
    }

    @Override
    public Set<Segment> getCompletedSegments() {
        return ownedLogs.entrySet()
            .stream()
            .filter(x -> x.getValue() < 0)
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    }

    @Override
    public Long getOffsetForOwnedSegment(Segment segmentId) {
        return ownedLogs.get(segmentId);
    }

    @Override
    public Set<Segment> getFutureOwnedSegments() {
        return Collections.unmodifiableSet(futureOwnedLogs.keySet());
    }

    @Override
    public PositionImpl asImpl() {
        return this;
    }

    @Override
    public PositionImpl asInternalImpl() {
        return this;
    }

    Long getOffsetForOwnedLog(Segment id) {
        return ownedLogs.get(id);
    }

    public Map<Segment, Long> getFutureOwnedLogs() {
        return Collections.unmodifiableMap(futureOwnedLogs);
    }

    public Map<Segment, Long> getOwnedLogs() {
        return Collections.unmodifiableMap(ownedLogs);
    }

}

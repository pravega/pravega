/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import com.emc.pravega.stream.SegmentId;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class PositionImpl implements Position {

    private static final long serialVersionUID = 1L;
    private final Map<SegmentId, Long> ownedLogs;
    private final Map<SegmentId, Long> futureOwnedLogs;

    public PositionImpl(Map<SegmentId, Long> ownedLogs, Map<SegmentId, Long> futureOwnedLogs) {
        this.ownedLogs = ownedLogs;
        this.futureOwnedLogs = futureOwnedLogs;
    }

    @Override
    public Set<SegmentId> getOwnedSegments() {
        return Collections.unmodifiableSet(ownedLogs.keySet());
    }

    @Override
    public Map<SegmentId, Long> getOwnedSegmentsWithOffsets() {
        return Collections.unmodifiableMap(ownedLogs);
    }

    @Override
    public Set<SegmentId> getCompletedSegments() {
        return ownedLogs.entrySet().stream().filter(x -> x.getValue() < 0).map(y -> y.getKey()).collect(Collectors.toSet());
    }

    @Override
    public Long getOffsetForOwnedSegment(SegmentId segmentId) {
        return ownedLogs.get(segmentId);
    }

    @Override
    public Set<SegmentId> getFutureOwnedSegments() {
        return Collections.unmodifiableSet(futureOwnedLogs.keySet());
    }

    Long getOffsetForOwnedLog(SegmentId log) {
        return ownedLogs.get(log);
    }

    @Override
    public PositionImpl asImpl() {
        return this;
    }

    public Map<SegmentId, Long> getFutureOwnedLogs() {
        return Collections.unmodifiableMap(futureOwnedLogs);
    }

    public Map<SegmentId, Long> getOwnedLogs() {
        return Collections.unmodifiableMap(ownedLogs);
    }

}

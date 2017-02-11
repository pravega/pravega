/**
 *  Copyright (c) 2016 Dell Inc. or its subsidiaries. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.Segment;

import java.util.Map;
import java.util.Set;

/**
 * A position has two components
 * 1. ownedSegments -- segments that can be read currently. Each ownedSegment also has an offset indicating the
 * point until which events have been read from that segment. Completely read segments have offset of -1.
 * 2. futureOwnedSegments -- segments that can be read after one of the currently read segment is completely read. Each
 * segment in this set has exactly one previous segment that belongs to the set ownedSegments.
 * <p>
 * Well-formed position object. A position is called well-formed iff the following hold.
 * 1. for each segment s in futureOwnedSegment, s.previous belongs to ownedSegments and s.previous.offset != -1
 * 2. for each segment s in ownedSegment, s.previous does not belongs to ownedSegments
 */
public abstract class PositionInternal implements Position {
    /**
     * Gets the set of segments currently being read, i.e., ownedSegments set.
     */
    abstract Set<Segment> getOwnedSegments();

    /**
     * Completely read segments have offset of -1.
     *
     * @return the read offset for each segment in the ownedSegments set
     */
    abstract Map<Segment, Long> getOwnedSegmentsWithOffsets();

    /**
     * Gets the set of completely read segments.
     */
    abstract Set<Segment> getCompletedSegments();

    /**
     * Gets the offset for a specified the segment.
     *
     * @param segmentId input segment
     */
    abstract Long getOffsetForOwnedSegment(Segment segmentId);

}

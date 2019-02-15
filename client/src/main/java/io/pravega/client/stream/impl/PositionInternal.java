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
import java.nio.ByteBuffer;
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
     * @return The set of segments currently being read
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
     * @return The set of completely read segments.
     */
    abstract Set<Segment> getCompletedSegments();

    /**
     * Gets the offset for a specified the segment.
     *
     * @param segmentId input segment
     * @return The offset for a specified the segment.
     */
    abstract Long getOffsetForOwnedSegment(Segment segmentId);
    
    /**
     * Deserializes the position from its serialized from obtained from calling {@link #toBytes()}.
     * 
     * @param position A serialized position.
     * @return The position object.
     */
    public static Position fromBytes(ByteBuffer position) {
        return PositionImpl.fromBytes(position);
    }

}

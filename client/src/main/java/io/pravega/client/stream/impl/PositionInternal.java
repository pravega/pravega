/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.client.stream.impl;

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.Position;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

/**
 * A position has ownedSegments -- segments that can be read currently (or have been completed and
 * not yet replaced).
 * 
 * A Position obtained at any given point will return a segment for all of the keyspace owned by
 * the reader.
 * 
 * Each ownedSegment also has an offset indicating the point until which events have been read from
 * that segment. Completely read segments have offset of -1.
 * <p>
 * We say that a position object P is well-formed to denote that every segment `s` in `P`
 * is not a predecessor of any segment in ownedSegments.
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
     * Completely read segments have offset of -1.
     *
     * @return the read offset for each segmentWithRange in the ownedSegments set
     */
    abstract Map<SegmentWithRange, Long> getOwnedSegmentRangesWithOffsets();

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

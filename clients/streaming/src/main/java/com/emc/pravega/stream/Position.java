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
package com.emc.pravega.stream;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

import com.emc.pravega.stream.impl.PositionImpl;

/**
 * A position in a stream. Used to restore to a consumer particular point in a stream.
 * Note that this is serializable so that it can be written to an external datastore.
 *
 * A position has two components
 * 1. ownedSegments -- segments that can be read currently. Each ownedSegment also has an offset indicating the
 * point until which events have been read from that segment. Completely read segments have offset of -1.
 * 2. futureOwnedSegments -- segments that can be read after one of the currently read segment is completely read. Each
 * segment in this set has exactly one previous segment that belongs to the set ownedSegments.
 *
 * Well-formed position object. A position is called well-formed iff the following hold.
 * 1. for each segment s in futureOwnedSegment, s.previous belongs to ownedSegments and s.previous.offset != -1
 * 2. for each segment s in ownedSegment, s.previous does not belongs to ownedSegments
 *
 */
public interface Position extends Serializable {
    
    /**
     * Used internally. Do not call.
     */
    PositionImpl asImpl();

    /**
     * Set of segments currently being read
     * @return
     */
    Set<SegmentId> getOwnedSegments();

    /**
     * Returns read offset for each segment currently being read.
     * Completely read segments have offset of -1.
     * @return
     */
    Map<SegmentId, Long> getOwnedSegmentsWithOffsets();

    /**
     * Returns the set of completely read segments.
     * @return
     */
    Set<SegmentId> getCompletedSegments();

    /**
     * Read offset for a specific segment contained in the position.
     * @param segmentId input segment
     * @return
     */
    Long getOffsetForOwnedSegment(SegmentId segmentId);

    /**
     * futureOwnedSegments are those that can be read after one of the currently read segment is completely read.
     * Each segment in this set has exactly one previous segment that belongs to the set ownedSegments.
     *
     * @return
     */
    Set<SegmentId> getFutureOwnedSegments();

}
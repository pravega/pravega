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

import com.emc.pravega.stream.impl.PositionImpl;

import java.util.Map;
import java.util.Set;

public interface PositionInternal {
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
     * @param SegmentId input segment
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

    PositionImpl asInternalImpl();
}

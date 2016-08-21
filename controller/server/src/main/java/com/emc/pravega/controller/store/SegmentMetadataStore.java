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
package com.emc.pravega.controller.store;

import java.util.List;

/**
 * Metadata of stream segments
 */
public interface SegmentMetadataStore {

    Segment getSegment(int number);

    String getStream();

    /**
     * Adds a new active segment to the store, having its number property set to smallest value
     * higher than that of existing segments. End time is set to Max_Value, and successors null,
     * since it is an active segment.
     * @param start start time of this new segment, which should be higher than start times of existing segments.
     * @param keyStart statr of the key range for this segment.
     * @param keyEnd end of the key range for this segment.
     * @param predecessors predecessor segments of this segment.
     * @return the created Segment object
     */
    Segment addActiveSegment(long start, double keyStart, double keyEnd, List<Integer> predecessors);

    /**
     *
     * @param segment new active segment to be added.
     * @return the Segment object updated with number property, endTime set to Max_Value, and successors set to null.
     */
    Segment addActiveSegment(Segment segment);

    /**
     * @return currently active segments
     */
    SegmentFutures getActiveSegments();

    /**
     * @param timestamp point in time.
     * @return the list of segments active at timestamp.
     */
    SegmentFutures getActiveSegments(long timestamp);

    /**
     * @param completedSegments completely read segments.
     * @param currentSegments current consumer positions.
     * @return new consumer positions including new (current or future) segments that can be read from.
     */
    List<SegmentFutures> getNextSegments(List<Integer> completedSegments, List<SegmentFutures> currentSegments);

    /**
     * Scales in or out the currently set of active segments
     * @param sealedSegments segments to be sealed
     * @param newSegments new active segments to added to the stream
     * @param scaleTimestamp scaling timestamp, all sealed segments shall have it as their end time and
     *                       all new segments shall have it as their start time.
     * @return the list of newly created segments
     */
    List<Segment> scale(List<Segment> sealedSegments, List<Segment> newSegments, long scaleTimestamp);
}

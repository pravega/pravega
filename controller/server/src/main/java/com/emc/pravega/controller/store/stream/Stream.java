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
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.stream.StreamConfiguration;

import java.util.AbstractMap;
import java.util.List;

/**
 * Properties of a stream and operations that can be performed on it.
 * Identifier for a stream is its name.
 */
interface Stream {

    String getName();

    /**
     * Create the stream, by creating/modifying underlying data structures.
     * @param configuration stream configuration.
     * @return boolean indicating success.
     */
    boolean create(StreamConfiguration configuration);

    /**
     * Updates the configuration of an existing stream.
     * @param configuration new stream configuration.
     * @return boolean indicating whether the stream was updated.
     */
    boolean updateConfiguration(StreamConfiguration configuration);

    /**
     * Fetches the current stream configuration.
     * @return current stream configuration.
     */
    StreamConfiguration getConfiguration();

    /**
     * Fetches details of specified segment.
     * @param number segment number.
     * @return segment at given number.
     */
    Segment getSegment(int number);

    /**
     * @param number segment.
     * @return successors of specified segment.
     */
    List<Integer> getSuccessors(int number);

    /**
     * @param number
     * @return predecessors of specified segment
     */
    List<Integer> getPredecessors(int number);

    /**
     * @return currently active segments
     */
    List<Integer> getActiveSegments();

    /**
     * @param timestamp point in time.
     * @return the list of segments active at timestamp.
     */
    List<Integer> getActiveSegments(long timestamp);

    /**
     * Scale the stream by sealing few segments and creating few segments
     * @param sealedSegments segments to be sealed
     * @param newRanges key ranges of new segments to be created
     * @param scaleTimestamp scaling timestamp
     * @return sequence of newly created segments
     */
    List<Segment> scale(List<Integer> sealedSegments, List<AbstractMap.SimpleEntry<Double, Double>> newRanges, long scaleTimestamp);
}

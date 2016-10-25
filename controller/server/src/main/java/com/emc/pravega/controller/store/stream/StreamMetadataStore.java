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

import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Set;

/**
 * Stream Metadata.
 */
//TODO: Add scope to most methods.
public interface StreamMetadataStore {

    /**
     * Creates a new stream with the given name and configuration.
     *
     * @param name          stream name.
     * @param configuration stream configuration.
     * @return boolean indicating whether the stream was created.
     */
    boolean createStream(String name, StreamConfiguration configuration);

    /**
     * Updates the configuration of an existing stream.
     *
     * @param name          stream name.
     * @param configuration new stream configuration
     * @return boolean indicating whether the stream was updated
     */
    boolean updateConfiguration(String name, StreamConfiguration configuration);

    /**
     * Fetches the current stream configuration.
     *
     * @param name stream name.
     * @return current stream configuration.
     */
    StreamConfiguration getConfiguration(String name);

    /**
     * Gets a segment for the given stream and segment number.
     *
     * @param name   stream name.
     * @param number segment number.
     * @return segment at given number.
     */
    Segment getSegment(String name, int number);

    /**
     * Gets all the currently active segments for the given stream.
     *
     * @param name stream name.
     * @return currently active segments
     */
    SegmentFutures getActiveSegments(String name);

    /**
     * Gets all the active segments for the given stream at the given timestamp.
     *
     * @param name      stream name.
     * @param timestamp point in time.
     * @return the list of segments active at timestamp.
     */
    SegmentFutures getActiveSegments(String name, long timestamp);

    /**
     * Gets the new consumer positions for the given arguments.
     *
     * @param name              stream name.
     * @param completedSegments completely read segments.
     * @param currentSegments   current consumer positions.
     * @return new consumer positions including new (current or future) segments that can be read from.
     */
    List<SegmentFutures> getNextSegments(String name, Set<Integer> completedSegments, List<SegmentFutures> currentSegments);

    /**
     * Scales in or out the currently set of active segments of a stream.
     *
     * @param name           stream name.
     * @param sealedSegments segments to be sealed
     * @param newRanges      new key ranges to be added to the stream which maps to a new segment per range in the stream
     * @param scaleTimestamp scaling timestamp, all sealed segments shall have it as their end time and
     *                       all new segments shall have it as their start time.
     * @return the list of newly created segments
     */
    List<Segment> scale(String name, List<Integer> sealedSegments, List<SimpleEntry<Double, Double>> newRanges, long scaleTimestamp);
}

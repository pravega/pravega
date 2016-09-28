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

    boolean create(StreamConfiguration configuration);

    /**
     * Updates the configuration of an existing stream.
     * @param configuration new stream configuration
     * @return boolean indicating whether the stream was updated
     */
    boolean updateConfiguration(StreamConfiguration configuration);

    /**
     * Fetches the current stream configuration.
     * @return current stream configuration.
     */
    StreamConfiguration getConfiguration();

    /**
     *
     * @param number segment number.
     * @return segment at given number.
     */
    Segment getSegment(int number);

    List<Integer> getSuccessors(int number);

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

    List<Segment> scale(List<Integer> sealedSegments, List<AbstractMap.SimpleEntry<Double, Double>> newRanges, long scaleTimestamp);
}

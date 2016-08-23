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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * In-memory stream store.
 */
public class InMemoryStreamStore implements StreamMetadataStore {

    Map<String, Stream> streams = new HashMap<>();

    private Stream getStream(String name) {
        if (streams.containsKey(name)) {
            Stream stream = streams.get(name);
            return stream;
        } else {
            throw new StreamNotFoundException(name);
        }
    }

    public void initialize() {
        // TODO initialize from persistent store, create collections of appropriate size
        streams = new HashMap<>();
    }

    @Override
    public boolean createStream(String name, StreamConfiguration configuration) {
        if (!streams.containsKey(name)) {
            Stream stream = new Stream(name, configuration);
            streams.put(name, stream);
            return true;
        } else {
            throw new StreamAlreadyExistsException(name);
        }
    }

    @Override
    public boolean updateConfiguration(String name, StreamConfiguration configuration) {
        Stream stream = getStream(name);
        stream.setConfiguration(configuration);
        return true;
    }

    @Override
    public StreamConfiguration getConfiguration(String name) {
        Stream stream = getStream(name);
        return stream.getStreamConfiguration();
    }

    @Override
    public Segment getSegment(String name, int number) {
        Stream stream = getStream(name);
        return stream.getSegment(number);
    }

    @Override
    public Segment addActiveSegment(String name, long start, double keyStart, double keyEnd, List<Integer> predecessors) {
        Stream stream = getStream(name);
        return stream.addActiveSegment(start, keyStart, keyEnd, predecessors);
    }

    @Override
    public Segment addActiveSegment(String name, Segment segment) {
        Stream stream = getStream(name);
        return stream.addActiveSegment(segment);
    }

    @Override
    public SegmentFutures getActiveSegments(String name) {
        Stream stream = getStream(name);
        return stream.getActiveSegments();
    }

    @Override
    public SegmentFutures getActiveSegments(String name, long timestamp) {
        Stream stream = getStream(name);
        return stream.getActiveSegments(timestamp);
    }

    @Override
    public List<SegmentFutures> getNextSegments(String name, List<Integer> completedSegments, List<SegmentFutures> currentSegments) {
        Stream stream = getStream(name);
        return stream.getNextSegments(completedSegments, currentSegments);
    }

    @Override
    public List<Segment> scale(String name, List<Segment> sealedSegments, List<Segment> newSegments, long scaleTimestamp) {
        Stream stream = getStream(name);
        return stream.scale(sealedSegments, newSegments, scaleTimestamp);
    }
}

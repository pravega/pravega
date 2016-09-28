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
 * ZK Stream. It understands the following.
 * 1. underlying file organization/object structure of stream metadata store.
 * 2. how to evaluate basic read and update queries defined in the Stream interface.
 *
 * It may cache files read from the store for its lifetime.
 * This shall reduce store round trips for answering queries, thus making them efficient.
 */
class ZKStream implements Stream {

    private String name;

    public ZKStream(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public boolean create(StreamConfiguration configuration) {
        return false;
    }

    @Override
    public boolean updateConfiguration(StreamConfiguration configuration) {
        return false;
    }

    @Override
    public StreamConfiguration getConfiguration() {
        return null;
    }

    @Override
    public Segment getSegment(int number) {
        return null;
    }

    @Override
    public List<Integer> getSuccessors(int number) {
        return null;
    }

    @Override
    public List<Integer> getPredecessors(int number) {
        return null;
    }

    @Override
    public List<Integer> getActiveSegments() {
        return null;
    }

    @Override
    public List<Integer> getActiveSegments(long timestamp) {
        return null;
    }

    @Override
    public List<Segment> scale(List<Integer> sealedSegments, List<AbstractMap.SimpleEntry<Double, Double>> newRanges, long scaleTimestamp) {
        return null;
    }
}

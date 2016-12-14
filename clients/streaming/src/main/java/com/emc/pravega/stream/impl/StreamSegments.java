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
package com.emc.pravega.stream.impl;

import com.emc.pravega.stream.Segment;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;

import java.util.Collection;
import java.util.Collections;
import java.util.NavigableMap;

/**
 * The segments that within a stream at a particular point in time.
 */
@EqualsAndHashCode
public class StreamSegments {
    private final NavigableMap<Double, Segment> segments;

    /**
     * Creates a new instance of the StreamSegments class.
     *
     * @param segments Segments of a stream, keyed by the largest key in their key range.
     *                 IE: If there are two segments split evenly, the first should have a value of 0.5 and the second 1.0.
     */
    public StreamSegments(NavigableMap<Double, Segment> segments) {
        this.segments = Collections.unmodifiableNavigableMap(segments);
        verifySegments();
    }

    private void verifySegments() {
        Preconditions.checkArgument(segments.firstKey() > 0.0, "Nonsense value for segment.");
        Preconditions.checkArgument(segments.lastKey() >= 1.0, "Last segment missing.");
        Preconditions.checkArgument(segments.lastKey() < 1.00001, "Segments should only go up to 1.0");
    }

    public Segment getSegmentForKey(double key) {
        Preconditions.checkArgument(key >= 0.0);
        Preconditions.checkArgument(key <= 1.0);
        return segments.ceilingEntry(key).getValue();
    }

    public Collection<Segment> getSegments() {
        return segments.values();
    }
}
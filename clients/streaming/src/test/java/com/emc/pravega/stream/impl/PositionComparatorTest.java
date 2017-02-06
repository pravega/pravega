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
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class PositionComparatorTest {
    @Test
    public void testCompareSameSegments() {
        List<Segment> segments = new LinkedList<>();
        IntStream.range(0, 2).boxed().forEach(i -> segments.add(new Segment("a", "a", i)));

        Map<Segment, Long> segments1 = new HashMap<>();
        segments1.put(segments.get(0), 10L);
        segments1.put(segments.get(1), 10L);
        PositionImpl pos1 = new PositionImpl(segments1, Collections.emptyMap());

        Map<Segment, Long> segments2 = new HashMap<>();
        segments2.put(segments.get(0), 20L);
        segments2.put(segments.get(1), 10L);
        PositionImpl pos2 = new PositionImpl(segments2, Collections.emptyMap());

        assert new PositionComparator().compare(pos1, pos2) < 0;
    }

    @Test
    public void testCompareExtraSegments() {
        List<Segment> segments = new LinkedList<>();
        IntStream.range(0, 2).boxed().forEach(i -> segments.add(new Segment("a", "a", i)));

        Map<Segment, Long> segments1 = new HashMap<>();
        segments1.put(segments.get(0), 10L);
        PositionImpl pos1 = new PositionImpl(segments1, Collections.emptyMap());

        Map<Segment, Long> segments2 = new HashMap<>();
        segments2.put(segments.get(0), 10L);
        segments2.put(segments.get(1), 10L);
        PositionImpl pos2 = new PositionImpl(segments2, Collections.emptyMap());

        assert new PositionComparator().compare(pos1, pos2) < 0;
    }

    @Test
    public void testCompareDifferentSegments() {
        List<Segment> segments = new LinkedList<>();
        IntStream.range(0, 3).boxed().forEach(i -> segments.add(new Segment("a", "a", i)));

        Map<Segment, Long> segments1 = new HashMap<>();
        segments1.put(segments.get(0), 10L);
        PositionImpl pos1 = new PositionImpl(segments1, Collections.emptyMap());

        Map<Segment, Long> segments2 = new HashMap<>();
        segments2.put(segments.get(1), 10L);
        PositionImpl pos2 = new PositionImpl(segments2, Collections.emptyMap());

        assert new PositionComparator().compare(pos1, pos2) < 0;
    }
}

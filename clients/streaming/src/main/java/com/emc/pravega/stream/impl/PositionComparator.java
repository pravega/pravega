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

import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.Segment;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class is used to compare two position objects from the same reader.
 * <p>
 * It is useful to compare two position objects if the reader is processing incoming events concurrently but wants to
 * sort the received position objects or compare them to identify which event is ahead in the stream.
 * <p>
 * While comparing position objects, if both have exact same segments then the one with any segment with
 * higher offset than the other is treated as greater of the two.
 * If they have different number of segments, but there are overlaps, then the one which is ahead in the overlap
 * segment set is treated as greater.
 * If they have no overlaps then the one with higher segment number is treated as being ahead.
 */
public class PositionComparator implements Comparator<Position> {

    @Override
    public int compare(Position p1, Position p2) {
        // Assumption is that both positions are from the same reader. So for any common segment
        // if offset in one is higher than the other, then that position is ahead.
        // If these position objects have no common segments, then the one with higher highest segment is ahead.

        int retVal = 0;

        Map<Segment, Long> s1 = p1.asImpl().getOwnedSegmentsWithOffsets();
        Map<Segment, Long> s2 = p2.asImpl().getOwnedSegmentsWithOffsets();
        List<Map.Entry<Segment, Long>> sorted1 = s1.entrySet().stream()
                .sorted(Comparator.comparingInt(x -> x.getKey().getSegmentNumber()))
                .collect(Collectors.toList());
        List<Map.Entry<Segment, Long>> sorted2 = s2.entrySet().stream()
                .sorted(Comparator.comparingInt(x -> x.getKey().getSegmentNumber()))
                .collect(Collectors.toList());

        List<Long> bothS1 = new ArrayList<>();
        List<Long> bothS2 = new ArrayList<>();

        int s1OnlyMax = -1, s2OnlyMax = -1, bothMin = -1, bothMax = -1;

        for (Map.Entry<Segment, Long> x : sorted1) {
            if (s2.containsKey(x.getKey())) {
                bothS1.add(x.getValue());
                bothMax = Math.max(bothMax, x.getKey().getSegmentNumber());
                bothMin = bothMin < 0 ? x.getKey().getSegmentNumber() : Math.min(bothMin, x.getKey().getSegmentNumber());
            } else {
                s1OnlyMax = Math.max(s1OnlyMax, x.getKey().getSegmentNumber());
            }
        }

        for (Map.Entry<Segment, Long> x : sorted2) {
            if (s1.containsKey(x.getKey())) {
                bothS2.add(x.getValue());
            } else {
                s2OnlyMax = Math.max(s2OnlyMax, x.getKey().getSegmentNumber());
            }
        }

        retVal = compareOverlap(bothS1, bothS2);
        // if(identical overlap) -- one with higher segment number is treated as ahead

        if (retVal == 0) {
            if (s1OnlyMax >= 0 && s2OnlyMax >= 0) {
                return Integer.compare(s1OnlyMax, s2OnlyMax);
            } else if (s1OnlyMax >= 0) {
                // overlaps are identical and there is only is S1.
                if (s1OnlyMax < bothMin) {
                    retVal = -1;
                } else if (s1OnlyMax > bothMax) {
                    retVal = 1;
                } else {
                    retVal = 0; // cant compare
                }
            } else if (s2OnlyMax >= 0) {
                // overlaps are identical and there is only is S2.
                if (s2OnlyMax < bothMin) {
                    retVal = 1;
                } else if (s2OnlyMax > bothMax) {
                    retVal = -1;
                } else {
                    retVal = 0; // cant compare
                }
            }
        }

        return retVal;
    }

    private int compareOverlap(List<Long> sorted1, List<Long> sorted2) {

        for (int i = 0; i < sorted1.size(); i++) {
            int comparison = Long.compare(sorted1.get(i), sorted2.get(i));
            if (comparison != 0) {
                return comparison;
            }
        }
        return 0;
    }
}

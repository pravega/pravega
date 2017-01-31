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

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

public class PositionComparator implements Comparator<Position> {

    @Override
    public int compare(Position p1, Position p2) {
        // Assumption is that both positions are from the same reader. So for any common segment
        // if offset in one is higher than the other, then that position is ahead.
        // If these position objects have no common segments, then the one with higher highest segment is ahead.

        int retVal = 0;

        final Map<Segment, Long> s1 = p1.asImpl().getOwnedSegmentsWithOffsets();
        final Map<Segment, Long> s2 = p2.asImpl().getOwnedSegmentsWithOffsets();

        final Set<Segment> diff1 = s1.keySet();
        diff1.removeAll(s2.keySet());

        final Set<Segment> diff2 = s2.keySet();
        diff2.removeAll(s1.keySet());

        if (diff1.isEmpty()) { // all keys from s1 are in s2
            if (s1.entrySet().stream().anyMatch(x -> s2.get(x.getKey()) > x.getValue())) {
                retVal = -1;
            } else {
                retVal = 1;
            }
        } else if (diff2.isEmpty()) { // all keys from s2 are in s1
            if (s2.entrySet().stream().anyMatch(x -> x.getValue() > s1.get(x.getKey()))) {
                retVal = -1;
            } else {
                retVal = 1;
            }
        } else {
            int max1 = diff1.stream().mapToInt(Segment::getSegmentNumber).max().getAsInt();
            int max2 = diff2.stream().mapToInt(Segment::getSegmentNumber).max().getAsInt();
            if (max1 < max2) {
                retVal = -1;
            } else {
                retVal = 1;
            }
        }

        return retVal;
    }
}

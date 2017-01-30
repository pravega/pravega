package com.emc.pravega.stream.impl;

import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.Segment;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

public class PositionComparator implements Comparator<Position> {

    // Assumption is that both positions are from the same reader. So for any common segment
    // if offset in one is higher than the other, then that position is ahead.
    // If these position objects have no common segments, then the one with higher highest segment is ahead.
    @Override
    public int compare(Position p1, Position p2) {
        final Map<Segment, Long> s1 = p1.asImpl().getOwnedSegmentsWithOffsets();
        final Map<Segment, Long> s2 = p2.asImpl().getOwnedSegmentsWithOffsets();

        final Set<Segment> diff1 = s1.keySet();
        diff1.removeAll(s2.keySet());

        final Set<Segment> diff2 = s2.keySet();
        diff2.removeAll(s1.keySet());

        if (diff1.isEmpty()) { // all keys from s1 are in s2
            if (s1.entrySet().stream().anyMatch(x -> s2.get(x.getKey()) > x.getValue())) {
                return -1;
            } else {
                return 1;
            }
        } else if (diff2.isEmpty()) { // all keys from s2 are in s1
            if (s2.entrySet().stream().anyMatch(x -> x.getValue() > s1.get(x.getKey()))) {
                return -1;
            } else {
                return 1;
            }
        } else {
            int max1 = diff1.stream().mapToInt(Segment::getSegmentNumber).max().getAsInt();
            int max2 = diff2.stream().mapToInt(Segment::getSegmentNumber).max().getAsInt();
            if(max1 < max2) {
                return -1;
            } else {
                return 1;
            }
        }
    }
}

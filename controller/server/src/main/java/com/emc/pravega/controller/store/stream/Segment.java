/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.store.stream;

import lombok.Data;
import lombok.ToString;

import java.util.AbstractMap;

/**
 * Properties of a stream segment that don't change over its lifetime.
 */
@Data
@ToString(includeFieldNames = true)
public class Segment {

    protected final int number;
    protected final long start;
    protected final double keyStart;
    protected final double keyEnd;

    public boolean overlaps(final Segment segment) {
        return segment.getKeyEnd() > keyStart && segment.getKeyStart() < keyEnd;
    }

    public boolean overlaps(final double keyStart, final double keyEnd) {
        return keyEnd > this.keyStart && keyStart < this.keyEnd;
    }

    public static boolean overlaps(final AbstractMap.SimpleEntry<Double, Double> first,
                                   final AbstractMap.SimpleEntry<Double, Double> second) {
        return second.getValue() > first.getKey() && second.getKey() < first.getValue();
    }
}

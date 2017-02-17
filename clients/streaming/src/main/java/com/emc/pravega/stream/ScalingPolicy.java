/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream;

import lombok.Data;

import java.io.Serializable;

/**
 * A policy that specifies how the number of segments in a stream should scale over time.
 */
@Data
public class ScalingPolicy implements Serializable {
    public enum Type {
        /**
         * No scaling, there will only ever be {@link ScalingPolicy#minNumSegments} at any given time.
         */
        FIXED_NUM_SEGMENTS,
        /**
         * Scale based on the rate in bytes specified in {@link ScalingPolicy#targetRate}.
         */
        BY_RATE_IN_BYTES,
        /**
         * Scale based on the rate in events specified in {@link ScalingPolicy#targetRate}.
         */
        BY_RATE_IN_EVENTS,
    }

    private final Type type;
    private final long targetRate;
    private final int scaleFactor;
    private final int minNumSegments;
}

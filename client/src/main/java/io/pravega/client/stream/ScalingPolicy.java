/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream;

import java.io.Serializable;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;

/**
 * A policy that specifies how the number of segments in a stream should scale over time.
 */
@Data
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
public class ScalingPolicy implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum ScalingType {
        /**
         * No scaling, there will only ever be {@link ScalingPolicy#minNumSegments} at any given time.
         */
        FIXED_NUM_SEGMENTS,
        /**
         * Scale based on the rate in bytes specified in {@link ScalingPolicy#targetRate}.
         */
        BY_RATE_IN_KBYTES_PER_SEC,
        /**
         * Scale based on the rate in events specified in {@link ScalingPolicy#targetRate}.
         */
        BY_RATE_IN_EVENTS_PER_SEC,
    }

    private final ScalingType scalingType;
    private final int targetRate;
    private final int scaleFactor;
    private final int minNumSegments;

    /**
     * Create a scaling policy to configure a stream to have a fixed number of segments.
     *
     * @param numSegments Fixed number of segments for the stream.
     * @return Scaling policy object.
     */
    public static ScalingPolicy fixed(int numSegments) {
        return new ScalingPolicy(ScalingType.FIXED_NUM_SEGMENTS, 0, 0, numSegments);
    }

    /**
     * Create a scaling policy to configure a stream to scale up and down according
     * to event rate. Pravega scales a stream segment up in the case that:
     *   - The two-minute rate is greater than 5x the target rate
     *   - The five-minute rate is greater than 2x the target rate
     *   - The ten-minute rate is greater than the target rate
     *
     * It scales a segment down (merges with a neighbor segment) in the case that:
     *   - The two-, five-, ten-minute rate is smaller than the target rate
     *   - The twenty-minute rate is smaller than half of the target rate
     *
     * The scale factor bounds the number of new segments that can be created upon
     * a scaling event. In the case the controller computes the number of splits
     * to be greater than the scale factor for a given scale-up event, the number
     * of splits for the event is going to be equal to the scale factor.
     *
     * The policy is configured with a minimum number of segments for the stream,
     * independent of the number of scale down events.
     *
     * @param targetRate Target rate in events per second to enable scaling events.
     * @param scaleFactor Maximum number of splits of a segment for a scale-up event.
     * @param minNumSegments Minimum number of segments that a stream can have
     *                       independent of the number of scale down events.
     * @return Scaling policy object.
     */
    public static ScalingPolicy byEventRate(int targetRate, int scaleFactor, int minNumSegments) {
        return new ScalingPolicy(ScalingType.BY_RATE_IN_EVENTS_PER_SEC, targetRate, scaleFactor, minNumSegments);
    }

    /**
     * Create a scaling policy to configure a stream to scale up and down according
     * to byte rate. Pravega scales a stream segment up in the case that:
     *   - The two-minute rate is greater than 5x the target rate
     *   - The five-minute rate is greater than 2x the target rate
     *   - The ten-minute rate is greater than the target rate
     *
     * It scales a segment down (merges with a neighbor segment) in the case that:
     *   - The two-, five-, ten-minute rate is smaller than the target rate
     *   - The twenty-minute rate is smaller than half of the target rate
     *
     * The scale factor bounds the number of new segments that can be created upon
     * a scaling event. In the case the controller computes the number of splits
     * to be greater than the scale factor for a given scale-up event, the number
     * of splits for the event is going to be equal to the scale factor.
     *
     * The policy is configured with a minimum number of segments for a stream,
     * independent of the number of scale down events.
     *
     * @param targetKBps Target rate in kilo bytes per second to enable scaling events.
     * @param scaleFactor Maximum number of splits of a segment for a scale-up event.
     * @param minNumSegments Minimum number of segments that a stream can have
     *                       independent of the number of scale down events.
     * @return Scaling policy object.
     */
    public static ScalingPolicy byDataRate(int targetKBps, int scaleFactor, int minNumSegments) {
        return new ScalingPolicy(ScalingType.BY_RATE_IN_KBYTES_PER_SEC, targetKBps, scaleFactor, minNumSegments);
    }
}

/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.client.stream;

import com.google.common.base.Preconditions;
import java.io.Serializable;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/**
 * A policy that specifies how the number of segments in a stream should scale over time.
 */
@Data
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
@ToString
public class ScalingPolicy implements Serializable {
    private static final long serialVersionUID = 1L;

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public enum ScaleType {
        /**
         * No scaling, there will only ever be {@link ScalingPolicy#minNumSegments} at any given time.
         */
        FIXED_NUM_SEGMENTS(io.pravega.shared.segment.ScaleType.NoScaling.getValue()),
        /**
         * Scale based on the rate in bytes specified in {@link ScalingPolicy#targetRate}.
         */
        BY_RATE_IN_KBYTES_PER_SEC(io.pravega.shared.segment.ScaleType.Throughput.getValue()),
        /**
         * Scale based on the rate in events specified in {@link ScalingPolicy#targetRate}.
         */
        BY_RATE_IN_EVENTS_PER_SEC(io.pravega.shared.segment.ScaleType.EventRate.getValue());

        @Getter
        private final byte value;
    }

    private final ScaleType scaleType;
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
        Preconditions.checkArgument(numSegments > 0, "Number of segments should be > 0.");
        return new ScalingPolicy(ScaleType.FIXED_NUM_SEGMENTS, 0, 0, numSegments);
    }

    /**
     * Create a scaling policy to configure a stream to scale up and down according
     * to event rate. Pravega scales a stream segment up in the case that one of these
     * conditions holds:
     *   - The two-minute rate is greater than 5x the target rate
     *   - The five-minute rate is greater than 2x the target rate
     *   - The ten-minute rate is greater than the target rate
     *
     * It scales a segment down (merges with a neighbor segment) in the case that both
     * these conditions hold:
     *
     *   - The two-, five-, ten-minute rate is smaller than the target rate
     *   - The twenty-minute rate is smaller than half of the target rate
     *
     * We additionally consider a cool-down period during which the segment is not
     * considered for scaling. This period is determined by the configuration
     * parameter autoScale.cooldownInSeconds; the default value is 10 minutes.
     *
     * The scale factor bounds the number of new segments that can be created upon
     * a scaling event. In the case the controller computes the number of splits
     * to be greater than the scale factor for a given scale-up event, the number
     * of splits for the event is going to be equal to the scale factor.
     *
     * The policy is configured with a minimum number of segments for the stream,
     * independent of the number of scale down events.
     *
     * @param targetRate Target rate in events per second to enable scaling events
     *                   per segment.
     * @param scaleFactor Maximum number of splits of a segment for a scale-up event.
     * @param minNumSegments Minimum number of segments that a stream can have
     *                       independent of the number of scale down events.
     * @return Scaling policy object.
     */
    public static ScalingPolicy byEventRate(int targetRate, int scaleFactor, int minNumSegments) {
        Preconditions.checkArgument(targetRate > 0, "Target rate should be > 0.");
        Preconditions.checkArgument(scaleFactor > 0, "Scale factor should be > 0. Otherwise use fixed scaling policy.");
        Preconditions.checkArgument(minNumSegments > 0, "Minimum number of segments should be > 0.");
        return new ScalingPolicy(ScaleType.BY_RATE_IN_EVENTS_PER_SEC, targetRate, scaleFactor, minNumSegments);
    }

    /**
     * Create a scaling policy to configure a stream to scale up and down according
     * to byte rate. Pravega scales a stream segment up in the case that one of these
     * conditions holds:
     *   - The two-minute rate is greater than 5x the target rate
     *   - The five-minute rate is greater than 2x the target rate
     *   - The ten-minute rate is greater than the target rate
     *
     * It scales a segment down (merges with a neighbor segment) in the case that
     * both these conditions hold:
     *   - The two-, five-, ten-minute rate is smaller than the target rate
     *   - The twenty-minute rate is smaller than half of the target rate
     *
     * We additionally consider a cool-down period during which the segment is not
     * considered for scaling. This period is determined by the configuration
     * parameter autoScale.cooldownInSeconds; the default value is 10 minutes.
     *
     * The scale factor bounds the number of new segments that can be created upon
     * a scaling event. In the case the controller computes the number of splits
     * to be greater than the scale factor for a given scale-up event, the number
     * of splits for the event is going to be equal to the scale factor.
     *
     * The policy is configured with a minimum number of segments for a stream,
     * independent of the number of scale down events.
     *
     * @param targetKBps Target rate in kilo bytes per second to enable scaling events
     *                   per segment.
     * @param scaleFactor Maximum number of splits of a segment for a scale-up event.
     * @param minNumSegments Minimum number of segments that a stream can have
     *                       independent of the number of scale down events.
     * @return Scaling policy object.
     */
    public static ScalingPolicy byDataRate(int targetKBps, int scaleFactor, int minNumSegments) {
        Preconditions.checkArgument(targetKBps > 0, "KBps should be > 0.");
        Preconditions.checkArgument(scaleFactor > 0, "Scale factor should be > 0. Otherwise use fixed scaling policy.");
        Preconditions.checkArgument(minNumSegments > 0, "Minimum number of segments should be > 0.");
        return new ScalingPolicy(ScaleType.BY_RATE_IN_KBYTES_PER_SEC, targetKBps, scaleFactor, minNumSegments);
    }
}

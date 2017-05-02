/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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

package io.pravega.stream;

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

    public enum Type {
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

    private final Type type;
    private final int targetRate;
    private final int scaleFactor;
    private final int minNumSegments;
    
    public static ScalingPolicy fixed(int numSegments) {
        return new ScalingPolicy(Type.FIXED_NUM_SEGMENTS, 0, 0, numSegments);
    }
    
    public static ScalingPolicy byEventRate(int targetRate, int scaleFactor, int minNumSegments) {
        return new ScalingPolicy(Type.BY_RATE_IN_EVENTS_PER_SEC, targetRate, scaleFactor, minNumSegments);
    }
    
    public static ScalingPolicy byDataRate(int targetKBps, int scaleFactor, int minNumSegments) {
        return new ScalingPolicy(Type.BY_RATE_IN_KBYTES_PER_SEC, targetKBps, scaleFactor, minNumSegments);
    }
}

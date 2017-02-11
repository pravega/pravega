/**
 *  Copyright (c) 2016 Dell Inc. or its subsidiaries. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.controller.server.rest.contract.common;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * REST representation of scaling policy of a stream.
 */
@Getter
@AllArgsConstructor
public class ScalingPolicyCommon {

    public enum Type {
        /**
         * No scaling, there will only ever be {@link ScalingPolicyCommon#minNumSegments} at any given time.
         */
        FIXED_NUM_SEGMENTS,
        /**
         * Scale based on the rate in bytes specified in {@link ScalingPolicyCommon#targetRate}.
         */
        BY_RATE_IN_BYTES,
        /**
         * Scale based on the rate in events specified in {@link ScalingPolicyCommon#targetRate}.
         */
        BY_RATE_IN_EVENTS,
    }

    private ScalingPolicyCommon.Type type;
    private long targetRate;
    private int scaleFactor;
    private int minNumSegments;
}

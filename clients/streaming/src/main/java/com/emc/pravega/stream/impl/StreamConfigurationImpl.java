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
package com.emc.pravega.stream.impl;

import com.emc.pravega.stream.RetentionPolicy;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class StreamConfigurationImpl implements StreamConfiguration {

    private final String scope;
    private final String name;
    private final ScalingPolicy scalingPolicy;
    private final RetentionPolicy retentionPolicy;

    public StreamConfigurationImpl(String scope, String name, ScalingPolicy scalingPolicy) {
        this(scope, name, scalingPolicy, new RetentionPolicy(Long.MAX_VALUE));
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public ScalingPolicy getScalingPolicy() {
        return scalingPolicy;
    }
}

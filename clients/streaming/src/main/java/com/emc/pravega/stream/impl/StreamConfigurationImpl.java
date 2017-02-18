/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
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

    /**
     * Creates a new instance of the StreamConfiguration class.
     *
     * @param scope         The scope of the stream.
     * @param name          The name of the stream.
     * @param scalingPolicy The scaling policy for the stream.
     */
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

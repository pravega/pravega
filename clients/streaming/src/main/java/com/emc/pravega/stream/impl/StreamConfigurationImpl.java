package com.emc.pravega.stream.impl;

import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class StreamConfigurationImpl implements StreamConfiguration {

    private final String name;
    private final ScalingPolicy scalingPolicy;

    @Override
    public String getName() {
        return name;
    }

    @Override
    public ScalingPolicy getScalingingPolicy() {
        return scalingPolicy;
    }
}

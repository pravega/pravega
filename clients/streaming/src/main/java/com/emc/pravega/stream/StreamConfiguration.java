/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream;

import java.io.Serializable;

import lombok.Builder;
import lombok.Data;

/**
 * The configuration of a Stream.
 */
@Data
@Builder
public class StreamConfiguration implements Serializable {
    
    /**
     * Api to return scope.
     *
     */
    private final String scope;

    /**
     * Api to return stream name.
     *
     */
    private final String streamName;

    /**
     * Api to return scaling policy.
     *
     */
    private final ScalingPolicy scalingPolicy;

    /**
     * Api to return retention policy.
     *
     */
    private final RetentionPolicy retentionPolicy;
    
    public static final class StreamConfigurationBuilder {
        private RetentionPolicy retentionPolicy = RetentionPolicy.INFINITE;
        private ScalingPolicy scalingPolicy = ScalingPolicy.fixed(1);
    }
}

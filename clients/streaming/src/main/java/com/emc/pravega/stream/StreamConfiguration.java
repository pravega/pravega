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
     * @return The scope of the stream.
     */
    private final String scope;

    /**
     * Api to return stream name.
     *
     * @return The name of the stream.
     */
    private final String streamName;

    /**
     * Api to return scaling policy.
     *
     * @return The stream's scaling policy.
     */
    private final ScalingPolicy scalingPolicy;

    /**
     * Returns the retention policy for this stream.
     *
     * @return The stream's retention policy
     */
    private final RetentionPolicy retentionPolicy;
    
    public static final class StreamConfigurationBuilder {
        private RetentionPolicy retentionPolicy = RetentionPolicy.builder().retentionTimeMillis(Long.MAX_VALUE).build();
        private ScalingPolicy scalingPolicy = ScalingPolicy.fixed(1);
    }
}

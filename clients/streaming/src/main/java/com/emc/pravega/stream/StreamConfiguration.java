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
     * The scope of the stream.
     */
    private final String scope;

    /**
     * The name of the stream.
     */
    private final String streamName;

    /**
     * The stream's scaling policy.
     */
    private final ScalingPolicy scalingPolicy;

    /**
     * The retention policy for this stream.
     */
    private final RetentionPolicy retentionPolicy;
    
    public static final class StreamConfigurationBuilder {
        private RetentionPolicy retentionPolicy = RetentionPolicy.builder().retentionTimeMillis(Long.MAX_VALUE).build();
    }
}

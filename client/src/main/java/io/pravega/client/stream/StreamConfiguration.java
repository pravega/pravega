/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.client.stream;

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
     * API to return scope.
     *
     */
    private final String scope;

    /**
     * API to return stream name.
     *
     */
    private final String streamName;

    /**
     * API to return scaling policy.
     *
     */
    private final ScalingPolicy scalingPolicy;

    /**
     * API to return retention policy.
     *
     */
    private final RetentionPolicy retentionPolicy;
    
    public static final class StreamConfigurationBuilder {
        private RetentionPolicy retentionPolicy = RetentionPolicy.INFINITE;
        private ScalingPolicy scalingPolicy = ScalingPolicy.fixed(1);
    }
}

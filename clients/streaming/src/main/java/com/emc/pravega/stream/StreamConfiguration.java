/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream;

import java.io.Serializable;

/**
 * The configuration of a Stream.
 */
public interface StreamConfiguration extends Serializable {
    
    /**
     * Api to return scope.
     * @return The scope of the stream.
     */
    String getScope();

    /**
     * Api to return stream name.
     * @return The name of the stream.
     */
    String getName();

    /**
     * Api to return scaling policy.
     * @return The stream's scaling policy.
     */
    ScalingPolicy getScalingPolicy();

    /**
     * Returns the retention policy for this stream.
     */
    RetentionPolicy getRetentionPolicy();
}

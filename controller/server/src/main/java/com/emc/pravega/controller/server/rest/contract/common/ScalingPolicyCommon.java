/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.server.rest.contract.common;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Getter;

import javax.validation.constraints.NotNull;

/**
 * REST representation of scaling policy of a stream.
 */
@Getter
@AllArgsConstructor
public class ScalingPolicyCommon implements Serializable {

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

    @NotNull
    private ScalingPolicyCommon.Type type;

    @NotNull
    private Long targetRate;

    @NotNull
    private Integer scaleFactor;

    @NotNull
    private Integer minNumSegments;
}

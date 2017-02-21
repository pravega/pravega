/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.server.rest.contract.request;

import com.emc.pravega.controller.server.rest.contract.common.RetentionPolicyCommon;
import com.emc.pravega.controller.server.rest.contract.common.ScalingPolicyCommon;
import lombok.AllArgsConstructor;
import lombok.Getter;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

/**
 * Conforms to createStream REST API request object.
 */
@Getter
@AllArgsConstructor
public class CreateStreamRequest {
    @NotNull
    private final String streamName;

    @Valid
    @NotNull
    private final ScalingPolicyCommon scalingPolicy;

    @Valid
    @NotNull
    private final RetentionPolicyCommon retentionPolicy;
}

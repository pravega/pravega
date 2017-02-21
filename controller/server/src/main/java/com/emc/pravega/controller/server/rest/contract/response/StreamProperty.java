/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.server.rest.contract.response;

import com.emc.pravega.controller.server.rest.contract.common.RetentionPolicyCommon;
import com.emc.pravega.controller.server.rest.contract.common.ScalingPolicyCommon;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * REST representation of properties of a stream.
 */
@AllArgsConstructor
@Getter
@Setter
public class StreamProperty implements Serializable {
    private String scope;
    private String streamName;
    private ScalingPolicyCommon scalingPolicy;
    private RetentionPolicyCommon retentionPolicy;
}

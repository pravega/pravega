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
 * REST representation of retention policy of a stream.
 */
@Getter
@AllArgsConstructor
public class RetentionPolicyCommon implements Serializable {
    @NotNull
    private Long retentionTimeMillis;
}

/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.common.cluster;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;
import lombok.ToString;

import java.io.Serializable;

@AllArgsConstructor
@Data
@ToString(includeFieldNames = true)
public class Host implements Serializable {
    @NonNull
    private final String ipAddr;
    private final int port;
}

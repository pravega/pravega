/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.common.cluster;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;

import java.io.Serializable;

@AllArgsConstructor
@Data
public class Host implements Serializable {
    @NonNull
    private final String ipAddr;
    private final int port;

    @Override
    public String toString() {
        return this.getIpAddr() + ":" + this.getPort();
    }
}

/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.common.cluster;

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
    private final String endpointId;

    @Override
    public String toString() {
        return this.getIpAddr() + ":" + this.getPort() + ((this.getEndpointId() == null) ? "" : ":" + this.getEndpointId());
    }

    public String getHostId() {
        return this.getIpAddr() + "-" + this.getEndpointId();
    }
}

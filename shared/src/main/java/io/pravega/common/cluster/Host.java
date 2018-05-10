/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.cluster;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;

import java.io.Serializable;

@AllArgsConstructor
@Data
public class Host implements Serializable {
    private static final long serialVersionUID = 1L;
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

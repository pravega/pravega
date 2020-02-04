/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.protocol.netty;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Data
@AllArgsConstructor
@Slf4j
public class PravegaNodeUri {
    @NonNull
    private final String endpoint;
    private final int port;
    @EqualsAndHashCode.Exclude
    private final InetAddress ipAddress;

    public PravegaNodeUri(@NonNull String endpoint, int port) {
        this.endpoint = endpoint;
        this.port = port;
        this.ipAddress = getInetAddress(endpoint);
    }

    private InetAddress getInetAddress(String endpoint) {
        try {
            return InetAddress.getByName(endpoint);
        } catch (UnknownHostException e) {
            log.error("Unable to to fetch IP address for endpoint {}", endpoint, e);
            return null;
        }
    }

    public boolean isModified() {
        InetAddress currentIPAddress = getInetAddress(this.endpoint);
        if (currentIPAddress != null && currentIPAddress.equals(this.ipAddress)) {
            return false;
        } else {
            log.warn("IP address of node : {} has changed to {}", this, currentIPAddress);
            return true;
        }
    }

}

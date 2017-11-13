/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.rpc.grpc.impl;

import io.pravega.common.Exceptions;
import io.pravega.controller.server.rpc.grpc.GRPCServerConfig;
import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.Data;

import java.util.Optional;

/**
 * gRPC server config.
 */
@Data
public class GRPCServerConfigImpl implements GRPCServerConfig {
    private final int port;
    private final boolean authorizationEnabled;
    private final Optional<String> publishedRPCHost;
    private final Optional<Integer> publishedRPCPort;
    private final String users;
    private final String passwords;
    private final String guardianIP;

    @Builder
    public GRPCServerConfigImpl(final int port, boolean authorizationEnabled, final String publishedRPCHost, final Integer publishedRPCPort, String users, String passwords, String guardianIP) {
        Preconditions.checkArgument(port > 0, "Invalid port.");
        if (publishedRPCHost != null) {
            Exceptions.checkNotNullOrEmpty(publishedRPCHost, "publishedRPCHost");
        }
        if (publishedRPCPort != null) {
            Preconditions.checkArgument(publishedRPCPort > 0, "publishedRPCPort should be a positive integer");
        }

        this.port = port;
        this.publishedRPCHost = Optional.ofNullable(publishedRPCHost);
        this.publishedRPCPort = Optional.ofNullable(publishedRPCPort);
        this.authorizationEnabled = authorizationEnabled;
        this.users = users;
        this.passwords = passwords;
        this.guardianIP = guardianIP;
    }
}

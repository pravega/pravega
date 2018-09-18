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

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.controller.server.rpc.grpc.GRPCServerConfig;
import java.util.Optional;
import lombok.Builder;
import lombok.Data;

/**
 * gRPC server config.
 */
@Data
public class GRPCServerConfigImpl implements GRPCServerConfig {
    private final int port;
    private final Optional<String> publishedRPCHost;
    private final Optional<Integer> publishedRPCPort;
    private final boolean authorizationEnabled;
    private final String userPasswordFile;
    private final boolean tlsEnabled;
    private final String tlsCertFile;
    private final String tlsKeyFile;
    private final String tokenSigningKey;
    private final String tlsTrustStore;
    private final boolean replyWithStackTraceOnError;

    @Builder
    public GRPCServerConfigImpl(final int port, final String publishedRPCHost, final Integer publishedRPCPort,
                                boolean authorizationEnabled, String userPasswordFile, boolean tlsEnabled,
                                String tlsCertFile, String tlsKeyFile, String tokenSigningKey, String tlsTrustStore,
                                boolean replyWithStackTraceOnError) {

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
        this.userPasswordFile = userPasswordFile;
        this.tlsEnabled = tlsEnabled;
        this.tlsCertFile = tlsCertFile;
        this.tlsKeyFile = tlsKeyFile;
        this.tlsTrustStore = tlsTrustStore;
        this.tokenSigningKey = tokenSigningKey;
        this.replyWithStackTraceOnError = replyWithStackTraceOnError;
    }
}

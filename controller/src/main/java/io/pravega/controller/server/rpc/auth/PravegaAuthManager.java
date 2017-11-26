/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.rpc.auth;

import io.grpc.ServerBuilder;
import io.pravega.controller.server.rpc.grpc.GRPCServerConfig;
import java.util.ServiceLoader;

public class PravegaAuthManager {
    private final GRPCServerConfig serverConfig;

    public PravegaAuthManager(GRPCServerConfig serverConfig) {
        this.serverConfig = serverConfig;
    }

    public void registerInterceptors(ServerBuilder<?> builder) {
        ServiceLoader<PravegaAuthHandler> loader = ServiceLoader.load(PravegaAuthHandler.class);
        for ( PravegaAuthHandler handler : loader) {
            handler.setServerConfig(serverConfig);
            builder.intercept(new PravegaInterceptor(handler));
        }
    }
}

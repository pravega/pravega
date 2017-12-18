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
import io.pravega.client.auth.PravegaAuthHandler;
import io.pravega.client.auth.PravegaAuthenticationException;
import io.pravega.controller.server.rpc.grpc.GRPCServerConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import javax.ws.rs.core.MultivaluedMap;

public class PravegaAuthManager {
    private final GRPCServerConfig serverConfig;
    private final Map<String, PravegaAuthHandler> handlerMap;

    public PravegaAuthManager(GRPCServerConfig serverConfig) {
        this.serverConfig = serverConfig;
        this.handlerMap = new HashMap<>();
    }

    private PravegaAuthHandler getHandler(String handlerName) throws PravegaAuthenticationException {
        if (handlerMap.containsKey(handlerName)) {
            return handlerMap.get(handlerName);
        }
        throw new PravegaAuthenticationException("Handler does not exist for method " + handlerName);

    }

    public boolean authenticate(String resource, MultivaluedMap<String, String> headers, PravegaAuthHandler.PravegaAccessControlEnum level) throws PravegaAuthenticationException {
        Map<String, String> paramMap = new HashMap<>();
        headers.keySet().stream().forEach(key -> {
            try {
                paramMap.put(key, headers.getFirst(key));
            } catch (IllegalArgumentException e) {
            }
        });
        String method = paramMap.get("method");

        PravegaAuthHandler handler = getHandler(method);
        return handler.authenticate(paramMap) &&
                handler.authorize(resource, paramMap).ordinal() >= level.ordinal();
    }



    public void registerInterceptors(ServerBuilder<?> builder) {
        if (serverConfig.isAuthorizationEnabled()) {
            ServiceLoader<PravegaAuthHandler> loader = ServiceLoader.load(PravegaAuthHandler.class);
            for (PravegaAuthHandler handler : loader) {
                handler.setServerConfig(serverConfig);
                handlerMap.put(handler.getHandlerName(), handler);
                builder.intercept(new PravegaInterceptor(handler));
            }
        }
    }
}

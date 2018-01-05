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
import lombok.extern.slf4j.Slf4j;

@Slf4j
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
        boolean retVal = false;
        try {
            Map<String, String> paramMap = new HashMap<>();
            headers.keySet().stream().forEach(key -> {
                try {
                    paramMap.put(key, headers.getFirst(key));
                } catch (IllegalArgumentException e) {
                }
            });
            String method = paramMap.get("method");

            PravegaAuthHandler handler = getHandler(method);
            retVal = handler.authenticate(paramMap) &&
                    handler.authorize(resource, paramMap).ordinal() >= level.ordinal();
        } catch (RuntimeException e) {
            throw new PravegaAuthenticationException(e);
        }
        return retVal;
    }



    public void registerInterceptors(ServerBuilder<?> builder) {
        try {
            if (serverConfig.isAuthorizationEnabled()) {
                ServiceLoader<PravegaAuthHandler> loader = ServiceLoader.load(PravegaAuthHandler.class);
                for (PravegaAuthHandler handler : loader) {
                    try {
                        handler.initialize(serverConfig);
                        if (handlerMap.putIfAbsent(handler.getHandlerName(), handler) != null) {
                            log.warn("Handler with name {} already exists. Not replacing it with the latest handler");
                            continue;
                        }
                        builder.intercept(new PravegaInterceptor(handler));
                    } catch (Exception e) {
                        log.warn("Exception {} while initializing auth handler {}", e, handler);
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Exception {} while loading the auth handlers", e);
        }
    }
}

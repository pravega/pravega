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
import io.pravega.auth.AuthHandler;
import io.pravega.auth.AuthenticationException;
import io.pravega.controller.server.rpc.grpc.GRPCServerConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import javax.ws.rs.core.MultivaluedMap;
import lombok.extern.slf4j.Slf4j;

/**
 * Auth manager class for Pravega controller. This manages the handlers for grpc and REST together.
 * In case of grpc, the routing of the authenticate function to specific registered interceptor is taken care by grpc
 * interceptor mechanism.
 * In case of REST calls, this class routes the call to specific AuthHandler.
 */
@Slf4j
public class PravegaAuthManager {
    private final GRPCServerConfig serverConfig;
    @GuardedBy("this")
    private final Map<String, AuthHandler> handlerMap;

    public PravegaAuthManager(GRPCServerConfig serverConfig) {
        this.serverConfig = serverConfig;
        this.handlerMap = new HashMap<>();
    }

    private AuthHandler getHandler(String handlerName) throws AuthenticationException {
        AuthHandler retVal;
        synchronized (this) {
                retVal = handlerMap.get(handlerName);
            }
            if (retVal == null) {
            throw new AuthenticationException("Handler does not exist for method " + handlerName);
        }
        return retVal;
    }

    /**
     * API to authenticate and authroize access to a given resource.
     * @param resource The resource identifier for which the access needs to be controlled.
     * @param headers  Custom headers used for authentication.
     * @param level    Expected level of access.
     * @return         Returns true if the entity represented by the custom auth headers had given level of access to the resource.
     * @throws AuthenticationException Exception faced during authentication/authorization.
     */
    public boolean authenticate(String resource, MultivaluedMap<String, String> headers, AuthHandler.Permissions level) throws AuthenticationException {
        Map<String, String> paramMap = headers.entrySet().stream().collect(Collectors.toMap(k -> k.getKey(), k -> k.getValue().get(0)));
        return authenticate(resource, paramMap, level);
    }

    /**
     * API to authenticate and authroize access to a given resource.
     * @param resource The resource identifier for which the access needs to be controlled.
     * @param paramMap  Custom headers used for authentication.
     * @param level    Expected level of access.
     * @return         Returns true if the entity represented by the custom auth headers had given level of access to the resource.
     *                 Returns false if the entity does not have access.
     * @throws AuthenticationException Exception if an authentication failure occured.
     */
    public boolean authenticate(String resource, Map<String, String> paramMap, AuthHandler.Permissions level) throws AuthenticationException {
        boolean retVal = false;
        try {
            String method = paramMap.get("method");
            AuthHandler handler = getHandler(method);

            if (!handler.authenticate(paramMap)) {
                throw new AuthenticationException("Authentication failure");
            }
            retVal = handler.authorize(resource, paramMap).ordinal() >= level.ordinal();
        } catch (RuntimeException e) {
            throw new AuthenticationException(e);
        }
        return retVal;
    }

    /**
     * Loads the custom implementations of the AuthHandler interface dynamically. Registers the interceptors with grpc.
     * Stores the implementation in a local map for routing the REST auth request.
     * @param builder The grpc service builder to register the interceptors.
     */
    public void registerInterceptors(ServerBuilder<?> builder) {
        try {
            if (serverConfig.isAuthorizationEnabled()) {
                ServiceLoader<AuthHandler> loader = ServiceLoader.load(AuthHandler.class);
                for (AuthHandler handler : loader) {
                    try {
                        handler.initialize(serverConfig);
                        synchronized (this) {
                            if (handlerMap.putIfAbsent(handler.getHandlerName(), handler) != null) {
                                log.warn("Handler with name {} already exists. Not replacing it with the latest handler");
                                continue;
                            }
                        }
                        builder.intercept(new PravegaInterceptor(handler));
                    } catch (Exception e) {
                        log.warn("Exception while initializing auth handler {}", handler, e);
                    }

                }
            }
        } catch (Throwable e) {
            log.warn("Exception while loading the auth handlers", e);
        }
    }
}

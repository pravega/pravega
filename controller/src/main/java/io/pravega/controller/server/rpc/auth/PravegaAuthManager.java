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

import com.google.common.base.Preconditions;
import io.grpc.ServerBuilder;
import io.pravega.auth.AuthHandler;
import io.pravega.common.auth.AuthenticationException;
import io.pravega.controller.server.rpc.grpc.GRPCServerConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import javax.annotation.concurrent.GuardedBy;
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
     * API to authenticate and authorize access to a given resource.
     * @param resource The resource identifier for which the access needs to be controlled.
     * @param credentials  Credentials used for authentication.
     * @param level    Expected level of access.
     * @return         Returns true if the entity represented by the custom auth headers had given level of access to the resource.
     *                 Returns false if the entity does not have access. 
     * @throws AuthenticationException if an authentication failure occurred.
     */
    public boolean authenticate(String resource, String credentials, AuthHandler.Permissions level) throws AuthenticationException {
        Preconditions.checkNotNull(credentials, "credentials");
        boolean retVal = false;
        try {
            String[] parts = credentials.split("\\s+", 2);
            if (parts.length != 2) {
                throw new AuthenticationException("Malformed request");
            }
            String method = parts[0];
            String token = parts[1];
            AuthHandler handler = getHandler(method);
            assert handler != null;
            if (!handler.authenticate(token)) {
                throw new AuthenticationException("Authentication failure");
            }
            retVal = handler.authorize(resource, token).ordinal() >= level.ordinal();
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

/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.rpc.auth;

import io.pravega.auth.AuthHandler;
import lombok.Getter;

import java.security.Principal;

/**
 * A gateway to objects held in gRPC {@link io.grpc.Context} by the {@link AuthInterceptor}, for the current gRPC
 * request.
 */
public class AuthContext {

    @Getter
    private Principal principal;

    @Getter
    private AuthHandler authHandler;

    private AuthContext(Principal principal, AuthHandler authHandler) {
        this.principal = principal;
        this.authHandler = authHandler;
    }

    /**
     * Initiates a new instance using the current {@link io.grpc.Context}.
     *
     * @return an instance containing {@ink Principal} and {@link AuthHandler} from the current {@link io.grpc.Context}
     */
    public static AuthContext current() {
        Principal principal = AuthInterceptor.AUTH_CONTEXT_TOKEN.get();
        AuthInterceptor serverAuthInterceptor = AuthInterceptor.INTERCEPTOR_OBJECT.get();
        AuthHandler authHandler = null;
        if (serverAuthInterceptor != null) {
            authHandler = serverAuthInterceptor.getHandler();
        }
        return new AuthContext(principal, authHandler);
    }
}

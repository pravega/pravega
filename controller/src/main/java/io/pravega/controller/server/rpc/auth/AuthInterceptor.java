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
import com.google.common.base.Strings;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.pravega.auth.AuthConstants;
import io.pravega.auth.AuthException;
import io.pravega.auth.AuthHandler;
import java.security.Principal;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Intercepts gRPC requests and sets up Auth context.
 */
@Slf4j
public class AuthInterceptor implements ServerInterceptor {

    private static final String AUTH_CONTEXT = "PravegaContext";
    private static final String INTERCEPTOR_CONTEXT = "InterceptorContext";
    static final Context.Key<Principal> AUTH_CONTEXT_TOKEN = Context.key(AUTH_CONTEXT);
    static final Context.Key<AuthInterceptor> INTERCEPTOR_OBJECT = Context.key(INTERCEPTOR_CONTEXT);

    @Getter
    private final AuthHandler handler;

    AuthInterceptor(AuthHandler handler) {
        Preconditions.checkNotNull(handler, "handler can not be null");
        this.handler = handler;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

        Context context = Context.current();

        // The authorization header has the credentials (e.g., username and password for Basic Authentication)
        String credentials = headers.get(Metadata.Key.of(AuthConstants.AUTHORIZATION, Metadata.ASCII_STRING_MARSHALLER));

        if (!Strings.isNullOrEmpty(credentials)) {
            String[] parts = credentials.split("\\s+", 2);
            if (parts.length == 2) {
                String method = parts[0];
                String token = parts[1];
                if (!Strings.isNullOrEmpty(method)) {
                    if (method.equals(handler.getHandlerName())) {
                        Principal principal;
                        try {
                            if ((principal = handler.authenticate(token)) == null) {
                                call.close(Status.fromCode(Status.Code.UNAUTHENTICATED), headers);
                                return null;
                            }
                        } catch (AuthException e) {
                            call.close(Status.fromCode(Status.Code.UNAUTHENTICATED), headers);
                            return null;
                        }
                        context = context.withValues(AUTH_CONTEXT_TOKEN, principal, INTERCEPTOR_OBJECT, this);
                    }
                }
            }
        }

        // reaching this point means that the handler wasn't applicable to this request.
        return Contexts.interceptCall(context, call, headers, next);
    }
}
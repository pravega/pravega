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
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.pravega.auth.AuthHandler;
import io.pravega.common.auth.AuthConstants;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import static io.grpc.Context.ROOT;
import static io.pravega.auth.AuthHandler.Permissions.READ_UPDATE;

@Slf4j
public class PravegaInterceptor implements ServerInterceptor {
    private static final boolean AUTH_ENABLED = true;
    private static final String AUTH_CONTEXT = "PravegaContext";
    private static final String INTERCEPTOR_CONTEXT = "InterceptorContext";
    private static final Context.Key<String> AUTH_CONTEXT_TOKEN = Context.key(AUTH_CONTEXT);
    public static final Context.Key<PravegaInterceptor> INTERCEPTOR_OBJECT = Context.key(INTERCEPTOR_CONTEXT);

    private final AuthHandler handler;
    @Getter
    private String delegationToken;

    PravegaInterceptor(AuthHandler handler) {
        Preconditions.checkNotNull(handler, "handler can not be null");
        this.handler = handler;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

        Context context = Context.current();

        String credentials = headers.get(Metadata.Key.of(AuthConstants.AUTHORIZATION, Metadata.ASCII_STRING_MARSHALLER));
        if (!Strings.isNullOrEmpty(credentials)) {
            String[] parts = credentials.split("\\s+", 2);
            if (parts.length == 2) {
                String method = parts[0];
                String token = parts[1];
                if (!Strings.isNullOrEmpty(method)) {
                    if (method.equals(handler.getHandlerName())) {
                        if (!handler.authenticate(token)) {
                            call.close(Status.fromCode(Status.Code.UNAUTHENTICATED), headers);
                            return null;
                        }
                        context = context.withValue(AUTH_CONTEXT_TOKEN, token);
                        context = context.withValue(INTERCEPTOR_OBJECT, this);
                    }
                }
            }
        }

        // reaching this point means that the handler wasn't applicable to this request.
        return Contexts.interceptCall(context, call, headers, next);
    }

    public AuthHandler.Permissions authorize(String resource) {
        return this.handler.authorize(resource, AUTH_CONTEXT_TOKEN.get());
    }

    /**
     * Retrieves the current delegation token if one exists.
     * @return The current delegation token. Empty string if a token does not exist.
     */
    public static String retrieveDelegationToken() {
        PravegaInterceptor interceptor = INTERCEPTOR_OBJECT.get();
        if (interceptor != null) {
            return interceptor.getDelegationToken();
        } else {
            return "";
        }
    }

    /**
     * Retrieves a master token for internal controller to segmentstore communication.
     * @param tokenSigningKey Signing key for the JWT token.
     * @return A new master token which has highest privileges.
     * Throws an error if this is called from a grpc context.
     */
    public static String retrieveMastertoken(String tokenSigningKey) {
        Preconditions.checkArgument(Context.current() == ROOT, "Getting master token from a grpc context.");
        Map<String, Object> claims = new HashMap<>();

        claims.put("*", String.valueOf(READ_UPDATE));

        return Jwts.builder()
                   .setSubject("segmentstoreresource")
                   .setAudience("segmentstore")
                   .setClaims(claims)
                   .signWith(SignatureAlgorithm.HS512, tokenSigningKey.getBytes())
                   .compact();
    }

    public void setDelegationToken(String resource, AuthHandler.Permissions expectedLevel, String tokenSigningKey) {
        if (AUTH_ENABLED) {
            Map<String, Object> claims = new HashMap<>();

            claims.put(resource, String.valueOf(expectedLevel));

            delegationToken = Jwts.builder()
                                  .setSubject("segmentstoreresource")
                                  .setAudience("segmentstore")
                                  .setClaims(claims)
                                  .signWith(SignatureAlgorithm.HS512, tokenSigningKey.getBytes())
                                  .compact();
        }
    }
}

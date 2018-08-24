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
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.auth.AuthHandler.Permissions.READ_UPDATE;

@Slf4j
public class PravegaInterceptor implements ServerInterceptor {
    private static final String AUTH_CONTEXT = "PravegaContext";
    private static final String INTERCEPTOR_CONTEXT = "InterceptorContext";
    private static final Context.Key<Map<String, String>> AUTH_CONTEXT_PARAMS = Context.key(AUTH_CONTEXT);
    public static final Context.Key<PravegaInterceptor> INTERCEPTOR_OBJECT = Context.key(INTERCEPTOR_CONTEXT);

    private final AuthHandler handler;

    PravegaInterceptor(AuthHandler handler) {
        Preconditions.checkNotNull(handler, "handler can not be null");
        this.handler = handler;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

        Map<String, String> paramMap = new HashMap<>();
        headers.keys().stream()
               .filter(key -> !key.endsWith(Metadata.BINARY_HEADER_SUFFIX))
               .forEach(key -> {
                   try {
                       paramMap.put(key,
                               headers.get(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER)));
                   } catch (IllegalArgumentException e) {
                       log.warn("Error while marshalling some of the headers {}", e.toString());
                   }
               });
        String method = paramMap.get("method");
        Context context = Context.current();
        if (!Strings.isNullOrEmpty(method)) {
            if (method.equals(handler.getHandlerName())) {

                if (!handler.authenticate(paramMap)) {
                    call.close(Status.fromCode(Status.Code.UNAUTHENTICATED), headers);
                    return null;
                }
                context = context.withValue(AUTH_CONTEXT_PARAMS, paramMap);
                context = context.withValue(INTERCEPTOR_OBJECT, this);
            }
        } else {
            call.close(Status.fromCode(Status.Code.UNAUTHENTICATED), headers);
            return null;
        }
        return Contexts.interceptCall(context, call, headers, next);
    }

    public AuthHandler.Permissions authorize(String resource) {
        return this.handler.authorize(resource, AUTH_CONTEXT_PARAMS.get());
    }

    /**
     * Retrieves a master token for internal controller to segmentstore communication.
     * @param tokenSigningKey Signing key for the JWT token.
     * @return A new master token which has highest privileges.
     */
    public static String retrieveMasterToken(String tokenSigningKey) {
        Map<String, Object> claims = new HashMap<>();

        claims.put("*", String.valueOf(READ_UPDATE));

        return Jwts.builder()
                   .setSubject("segmentstoreresource")
                   .setAudience("segmentstore")
                   .setClaims(claims)
                   .signWith(SignatureAlgorithm.HS512, tokenSigningKey.getBytes())
                   .compact();
    }
}

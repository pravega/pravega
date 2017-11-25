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

import com.google.common.base.Strings;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.util.HashMap;
import java.util.Map;

public class PravegaInterceptor implements ServerInterceptor {
    private static final String AUTH_CONTEXT = "PravegaContext";
    private static final String INTERCEPTOR_CONTEXT = "InterceptorContext";
    private static final Context.Key<Map<String, String>> AUTH_CONTEXT_PARAMS = Context.key(AUTH_CONTEXT);
    private static final Context.Key<PravegaInterceptor> INTERCEPTOR_OBJECT = Context.key(INTERCEPTOR_CONTEXT);

    private final PravegaAuthHandler handler;

    PravegaInterceptor(PravegaAuthHandler handler) {
        this.handler = handler;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

        Map<String, String> paramMap = new HashMap<>();
        headers.keys().stream().forEach(key -> {
            try {
                paramMap.put(key,
                        headers.get(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER)));
            } catch (IllegalArgumentException e) {
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
        }
        return Contexts.interceptCall(context, call, headers, next);
    }

    public static PravegaAuthHandler.PravegaAccessControlEnum Authorize(String resource) {
        PravegaInterceptor currentInterceptor = INTERCEPTOR_OBJECT.get();

        if (currentInterceptor == null) {
            //No interceptor, means no authorization enabled
            return PravegaAuthHandler.PravegaAccessControlEnum.READ_UPDATE;
        } else {
            return currentInterceptor.authorize(resource);
        }
    }

    private PravegaAuthHandler.PravegaAccessControlEnum authorize(String resource) {
        return this.handler.authorize(resource, AUTH_CONTEXT_PARAMS.get());
    }
}

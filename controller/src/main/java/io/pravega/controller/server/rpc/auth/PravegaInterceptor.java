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
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.util.HashMap;
import java.util.Map;

public class PravegaInterceptor implements ServerInterceptor {
    private final PravegaDefaultAuthHandler handler;

    PravegaInterceptor(PravegaDefaultAuthHandler handler) {
        this.handler = handler;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

        Map<String, String> paramMap = new HashMap<>();
        headers.keys().stream().map(key -> paramMap.put(key,
                headers.get(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER))));
        String method = paramMap.get("method");
        if ( !Strings.isNullOrEmpty(method)) {
            if (method.equals(handler.getHandlerName())) {
               if ( !handler.authenticate(paramMap)) {
                   call.close(Status.fromCode(Status.Code.UNAUTHENTICATED), headers);
                   return null;
               }
            }
        }
        return next.startCall(call, headers);
    }
}

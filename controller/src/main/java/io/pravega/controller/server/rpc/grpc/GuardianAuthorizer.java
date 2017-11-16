/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.rpc.grpc;

import com.emc.nautilus.auth.client.GuardianClient;
import com.emc.nautilus.auth.client.GuardianClientFactory;
import com.google.common.base.Strings;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class GuardianAuthorizer implements ServerInterceptor {
    private final GuardianClientFactory guardianClientFactory;
    private final ConcurrentMap<String, GuardianClient> clientMap;

    public GuardianAuthorizer(String guardianIp) {
        this.guardianClientFactory = new GuardianClientFactory(guardianIp);
        clientMap = new ConcurrentHashMap<>();
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        String method = headers.get(Metadata.Key.of("method", Metadata.ASCII_STRING_MARSHALLER));

        if (!Strings.isNullOrEmpty(method) && method.equals("guardian")) {
            String userName = headers.get(Metadata.Key.of("userName", Metadata.ASCII_STRING_MARSHALLER));
            String token = headers.get(Metadata.Key.of("token", Metadata.ASCII_STRING_MARSHALLER));

            GuardianClient client;
            synchronized (this) {
                if (!clientMap.containsKey(token)) {
                    client = guardianClientFactory.withToken(token);
                    clientMap.putIfAbsent(token, client);
                } else {
                    client = clientMap.get(token);
                }
            }
            // TODO: check ACLs for the resource string
            //String resourcePath = "";
            //TargetedACLs acls = client.getUserTargetedACLsForResourcePath(resourcePath);
            return next.startCall(call, headers);
        } else {
            return next.startCall(call, headers);
        }
    }
}

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

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Implements a authorizer for open source Pravega. Currently it checks user id and password against
 * a list of username and password stored in a configuration.
 */
class PravegaAuthorizer implements io.grpc.ServerInterceptor {
    private final ConcurrentMap<String, String> userDb = new ConcurrentHashMap<>();

    public PravegaAuthorizer(String users, String passwords) {
        String[] usersArray = users.split(",");
        String[] passwordsArray = passwords.split(",");
        int i = 0;
        for (String s : usersArray) {
            userDb.putIfAbsent(s, passwordsArray[ i++ / passwordsArray.length]);
        }
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        String userName = headers.get(Metadata.Key.of("userName", Metadata.ASCII_STRING_MARSHALLER));
        String password = headers.get(Metadata.Key.of("password", Metadata.ASCII_STRING_MARSHALLER));

        if (userDb.containsKey(userName) && password.equals(userDb.get(userName))) {
            return next.startCall(call, headers);
        } else {
            call.close( Status.fromCode( Status.Code.UNAUTHENTICATED), headers);
            return null;
        }
    }
}

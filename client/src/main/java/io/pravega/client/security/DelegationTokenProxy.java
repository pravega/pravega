/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.security;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import io.pravega.client.stream.impl.Controller;
import lombok.NonNull;

import java.util.Base64;
import java.util.concurrent.atomic.AtomicReference;


public class DelegationTokenProxy {

    private final Controller controllerClient;

    private final String scopeName;

    private final String streamName;

    private final AtomicReference<String> delegationToken = new AtomicReference<>();

    //private final boolean hasTokenExpirySet;

    @VisibleForTesting
    DelegationTokenProxy() {
        this("", null, "", "");
    }

    public DelegationTokenProxy(String token, Controller controllerClient, String scopeName, String streamName) {
        delegationToken.set(token);
        this.controllerClient = controllerClient;
        this.scopeName = scopeName;
        this.streamName = streamName;
        //hasTokenExpirySet =
    }

    public String getToken() {
        String token = delegationToken.get();
        return null;
    }

    /*private boolean hasTokenExpirySet(@NonNull String token) {
        if ()
    }*/

    @VisibleForTesting
    Integer extractExpirationTime(@NonNull String token) {
        if (token.trim().equals("")) {
            return null;
        }
        String[] tokenParts = token.split("\\.");

        // JWT is of the format abc.def.ghe. The middle part is the body.
        String encodedBody = tokenParts[1];
        String decodedBody = new String(Base64.getDecoder().decode(encodedBody));

        Jwt jwt = new Gson().fromJson(decodedBody, Jwt.class);
        return jwt.getExp();
    }
}

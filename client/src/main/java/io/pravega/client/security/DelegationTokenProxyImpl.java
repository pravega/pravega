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
import com.google.common.base.Strings;
import com.google.gson.Gson;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.impl.Controller;
import io.pravega.common.concurrent.Futures;
import lombok.NonNull;
import java.time.Instant;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;


public class DelegationTokenProxyImpl implements DelegationTokenProxy {

    private final Controller controllerClient;

    private final String scopeName;

    private final String streamName;

    private final AtomicReference<String> delegationToken = new AtomicReference<>();

    private AtomicInteger currentTokenExpirationTime;

    /**
     * Creates an object containing an empty ("") delegation token.
     */
    public DelegationTokenProxyImpl() {
        this.controllerClient = null;
        this.scopeName = null;
        this.streamName = null;
        delegationToken.set("");
    }

    public DelegationTokenProxyImpl(@NonNull Controller controllerClient,
                                    @NonNull String scopeName, @NonNull String streamName) {
        this(null, controllerClient, scopeName, streamName);
    }

    public DelegationTokenProxyImpl(String token, @NonNull Controller controllerClient, Segment segment) {
        this(token, controllerClient, segment.getScope(), segment.getStream().getStreamName());
    }

    public DelegationTokenProxyImpl(String token, @NonNull Controller controllerClient,
                                    @NonNull String scopeName, @NonNull String streamName) {
        if (token != null) {
            delegationToken.set(token);
        }
        Integer expTime = extractExpirationTime(token);
        if (expTime != null) {
            this.currentTokenExpirationTime = new AtomicInteger(expTime);
        }

        this.controllerClient = controllerClient;
        this.scopeName = scopeName;
        this.streamName = streamName;
    }

    /**
     * Returns the delegation token. It returns existing delegation token if it is not close to expiry. If the token
     * is close to expiry, it obtains a new delegation token and returns that one instead.
     *
     * @return
     */
    public String retrieveToken() {
        if (delegationToken.get() == null || isTokenNearingExpiry()) {
            return refreshToken();
        } else {
            return delegationToken.get();
        }
    }

    @Override
    public String refreshToken() {
        String token = delegationToken.get();
        if (token.isEmpty()) {
            return token;
        } else {
            String newDelegationToken = newToken();
            resetToken(newDelegationToken);
            return delegationToken.get();
        }
    }

    private String newToken() {
        return Futures.getAndHandleExceptions(
                    controllerClient.getOrRefreshDelegationTokenFor(scopeName, streamName), RuntimeException::new);
    }

    private boolean isTokenNearingExpiry() {
        return (this.currentTokenExpirationTime != null &&
                (Instant.now().getEpochSecond() - currentTokenExpirationTime.get()) <= 10);
    }

    private synchronized void resetToken(String newToken) {
        delegationToken.set(newToken);
        this.currentTokenExpirationTime.set(extractExpirationTime(newToken));
    }

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

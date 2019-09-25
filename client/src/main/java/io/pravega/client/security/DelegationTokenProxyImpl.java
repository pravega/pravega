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
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.impl.Controller;
import io.pravega.common.concurrent.Futures;
import lombok.NonNull;
import java.time.Instant;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;


public class DelegationTokenProxyImpl implements DelegationTokenProxy {

    /**
     * The Controller gRPC client used for interacting with the server.
     */
    private final Controller controllerClient;

    private final String scopeName;

    private final String streamName;

    /**
     * Wraps the delegation token.
     *
     * The delegation token can take these values:
     *   1) null: the value was not set at construction time, so it'll be obtaining lazily here.
     *   2) ""  : an empty value used by callers when auth is disabled.
     *   3) a non-empty JWT compact string of the format
     *       <base64-encoded-header-json>.<base64-encoded-body-json>.<base64-encoded-signature>
     */
    private final AtomicReference<String> delegationToken = new AtomicReference<>();

    /**
     * Caches the expiration time of the current token, so that the token does not need to be parsed upon every retrieval.
     */
    private AtomicInteger currentTokenExpirationTime;

    /**
     * Creates an object containing an empty ("") delegation token. Useful mainly for testing purposes.
     */
    @VisibleForTesting
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
            Integer expTime = extractExpirationTime(token);
            if (expTime != null) {
                this.currentTokenExpirationTime = new AtomicInteger(expTime);
            }
        }
        this.controllerClient = controllerClient;
        this.scopeName = scopeName;
        this.streamName = streamName;
    }

    /**
     * Returns the delegation token. It returns existing delegation token if it is not close to expiry. If the token
     * is close to expiry, it obtains a new delegation token and returns that one instead.
     *
     * @return String the delegation token JWT compact value
     */
    public String retrieveToken() {
        // Delegation token null represents it wasn't set at construction time, so we'll lazily create it here.
        if (isTokenNotInitialized() || isTokenNearingExpiry()) {
            return refreshToken();
        } else {
            return delegationToken.get();
        }
    }

    private boolean isTokenEmpty() {
        return delegationToken.get().equals("");
    }

    private boolean isTokenNotInitialized() {
        return delegationToken.get() != null;
    }

    @Override
    public String refreshToken() {
        //if ()
        String token = delegationToken.get();
        if (token.equals("")) {
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
        return this.currentTokenExpirationTime != null &&
                (Instant.now().getEpochSecond() - currentTokenExpirationTime.get()) <= 10;
    }

    private synchronized void resetToken(String newToken) {
        delegationToken.set(newToken);
        this.currentTokenExpirationTime.set(extractExpirationTime(newToken));
    }

    @VisibleForTesting
    Integer extractExpirationTime(@NonNull String token) {
        if (token == null || token.trim().equals("")) {
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

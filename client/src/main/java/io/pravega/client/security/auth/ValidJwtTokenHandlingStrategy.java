/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.security.auth;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import io.pravega.client.stream.impl.Controller;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.time.Instant;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicReference;

public class ValidJwtTokenHandlingStrategy implements DelegationTokenHandlingStrategy {

    /**
     * The Controller gRPC client used for interacting with the server.
     */
    @Getter(AccessLevel.PROTECTED)
    private final Controller controllerClient;

    @Getter(AccessLevel.PROTECTED)
    private final String scopeName;

    @Getter(AccessLevel.PROTECTED)
    private final String streamName;

    @Getter(AccessLevel.PROTECTED)
    @Setter(AccessLevel.PROTECTED)
    private AtomicReference<DelegationToken> delegationToken = new AtomicReference<>();

    public ValidJwtTokenHandlingStrategy(String token, Controller controllerClient, String scopeName, String streamName) {
        Exceptions.checkNotNullOrEmpty(token, "token");
        Exceptions.checkNotNullOrEmpty(scopeName, "scopeName");
        Preconditions.checkNotNull(controllerClient, "controllerClient is null");
        Exceptions.checkNotNullOrEmpty(streamName, "streamName");

        Long expTime = extractExpirationTime(token);

        // Preconditions.checkNotNull(expTime, "No expiry is set in the delegation token");
        delegationToken.set(new DelegationToken(token, expTime));
        this.scopeName = scopeName;
        this.streamName = streamName;
        this.controllerClient = controllerClient;
    }

    @Override
    public String retrieveToken() {
        if (isTokenNearingExpiry()) {
            return refreshToken();
        } else {
            return delegationToken.get().getValue();
        }
    }

    @Override
    public String refreshToken() {
        resetToken(newToken());
        return delegationToken.get().getValue();
    }

    private String newToken() {
        return Futures.getAndHandleExceptions(
                controllerClient.getOrRefreshDelegationTokenFor(scopeName, streamName), RuntimeException::new);
    }

    protected boolean isTokenNearingExpiry() {
        Long currentTokenExpirationTime = this.delegationToken.get().getExpiryTime();

        // currentTokenExpirationTime can be null if the server returns a null delegation token
        if (currentTokenExpirationTime != null && isWithinThresholdForRefresh(currentTokenExpirationTime)) {
            return true;
        } else {
            return false;
        }
    }

    protected boolean isWithinThresholdForRefresh(Long expirationTime) {
        return (Instant.now().getEpochSecond() - expirationTime) <= 10;
    }

    private void resetToken(String newToken) {
        delegationToken.set(new DelegationToken(newToken, extractExpirationTime(newToken)));
    }

    @VisibleForTesting
    Long extractExpirationTime(@NonNull String token) {
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



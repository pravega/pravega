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
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class ValidJwtTokenHandlingStrategy implements DelegationTokenHandlingStrategy {

    /**
     * Represents the threshold for triggering delegation token refresh.
     */
    private static final int REFRESH_THRESHOLD = 10;

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

    ValidJwtTokenHandlingStrategy(Controller controllerClient, String scopeName, String streamName) {
        Exceptions.checkNotNullOrEmpty(scopeName, "scopeName");
        Preconditions.checkNotNull(controllerClient, "controllerClient is null");
        Exceptions.checkNotNullOrEmpty(streamName, "streamName");

        this.scopeName = scopeName;
        this.streamName = streamName;
        this.controllerClient = controllerClient;
    }

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
            log.info("Token is nearing expiry, so refreshing it");
            return refreshToken();
        } else {
            return delegationToken.get().getValue();
        }
    }

    @Override
    public String refreshToken() {
        if (delegationToken.get().getExpiryTime() != null) {
            resetToken(newToken());
        } else {
            throw new IllegalStateException("Token expiry not set");
        }
        return delegationToken.get().getValue();
    }

    protected String newToken() {
        return Futures.getAndHandleExceptions(
                controllerClient.getOrRefreshDelegationTokenFor(scopeName, streamName), RuntimeException::new);
    }

    private synchronized boolean isTokenNearingExpiry() {
        Long currentTokenExpirationTime = this.delegationToken.get().getExpiryTime();

        // currentTokenExpirationTime can be null if the server returns a null delegation token
        return currentTokenExpirationTime != null && isWithinRefreshThreshold(currentTokenExpirationTime);
    }

    private boolean isWithinRefreshThreshold(Long expirationTime) {
        Preconditions.checkNotNull(expirationTime);
        return isWithinRefreshThreshold(Instant.now(), Instant.ofEpochSecond(expirationTime));
    }

    @VisibleForTesting
    boolean isWithinRefreshThreshold(Instant currentInstant, Instant expiration) {
        return currentInstant.plusSeconds(REFRESH_THRESHOLD).getEpochSecond() >= expiration.getEpochSecond();
    }

    protected void resetToken(String newToken) {
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

        JwtBody jwt = new Gson().fromJson(decodedBody, JwtBody.class);
        return jwt.getExp();
    }
}



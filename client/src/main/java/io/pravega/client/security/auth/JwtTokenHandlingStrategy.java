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
import io.pravega.common.util.ConfigurationOptionsExtractor;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class JwtTokenHandlingStrategy implements DelegationTokenHandlingStrategy {

    /**
     * Represents the threshold for triggering delegation token refresh.
     */
    @VisibleForTesting
    static final int DEFAULT_REFRESH_THRESHOLD = 5;

    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final int tokenRefreshThreshold;

    /**
     * The Controller gRPC client used for interacting with the server.
     */
    private final Controller controllerClient;

    private final String scopeName;

    private final String streamName;

    private final AtomicReference<DelegationToken> delegationToken = new AtomicReference<>();

    JwtTokenHandlingStrategy(Controller controllerClient, String scopeName, String streamName) {
        this(controllerClient, scopeName, streamName, ConfigurationOptionsExtractor.extractInt(
                "pravega.client.auth.token-refresh.threshold",
                "pravega_client_auth_token-refresh.threshold",
                DEFAULT_REFRESH_THRESHOLD));
    }

    JwtTokenHandlingStrategy(Controller controllerClient, String scopeName, String streamName, int tokenRefreshThreshold) {
        Exceptions.checkNotNullOrEmpty(scopeName, "scopeName");
        Preconditions.checkNotNull(controllerClient, "controllerClient is null");
        Exceptions.checkNotNullOrEmpty(streamName, "streamName");

        this.scopeName = scopeName;
        this.streamName = streamName;
        this.controllerClient = controllerClient;
        this.tokenRefreshThreshold = tokenRefreshThreshold;
    }

    public JwtTokenHandlingStrategy(String token, Controller controllerClient, String scopeName, String streamName) {
        this(token, controllerClient, scopeName, streamName, ConfigurationOptionsExtractor.extractInt(
                "pravega.client.auth.token-refresh.threshold",
                "pravega_client_auth_token-refresh.threshold",
                DEFAULT_REFRESH_THRESHOLD));
    }

    public JwtTokenHandlingStrategy(String token, Controller controllerClient, String scopeName, String streamName,
                                    int tokenRefreshThreshold) {
        Exceptions.checkNotNullOrEmpty(token, "token");
        Exceptions.checkNotNullOrEmpty(scopeName, "scopeName");
        Preconditions.checkNotNull(controllerClient, "controllerClient is null");
        Exceptions.checkNotNullOrEmpty(streamName, "streamName");

        Long expTime = extractExpirationTime(token);
        delegationToken.set(new DelegationToken(token, expTime));
        this.scopeName = scopeName;
        this.streamName = streamName;
        this.controllerClient = controllerClient;
        this.tokenRefreshThreshold = tokenRefreshThreshold;
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
        resetToken(newToken());
        return delegationToken.get().getValue();
    }

    protected String newToken() {
        return Futures.getAndHandleExceptions(
                controllerClient.getOrRefreshDelegationTokenFor(scopeName, streamName), RuntimeException::new);
    }

    protected DelegationToken getCurrentToken() {
        return this.delegationToken.get();
    }

    private boolean isTokenNearingExpiry() {
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
        return currentInstant.plusSeconds(tokenRefreshThreshold).getEpochSecond() >= expiration.getEpochSecond();
    }

    protected void resetToken(String newToken) {
        delegationToken.set(new DelegationToken(newToken, extractExpirationTime(newToken)));
    }

    @VisibleForTesting
    Long extractExpirationTime(String token) {
        if (token == null || token.trim().equals("")) {
            return null;
        }
        String[] tokenParts = token.split("\\.");

        // JWT is of the format abc.def.ghe. The middle part is the body.
        String encodedBody = tokenParts[1];
        String decodedBody = new String(Base64.getDecoder().decode(encodedBody));

        JwtBody jwt = new Gson().fromJson(decodedBody, JwtBody.class);
        return jwt.getExpirationTime();
    }
}



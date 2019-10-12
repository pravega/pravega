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
import io.pravega.client.stream.impl.Controller;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.util.ConfigurationOptionsExtractor;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.Base64;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Provides JWT-based delegation tokens.
 */
@Slf4j
public class JwtTokenProviderImpl implements DelegationTokenProvider {

    /**
     * Represents the default threshold (in seconds) for triggering delegation token refresh.
     */
    @VisibleForTesting
    static final int DEFAULT_REFRESH_THRESHOLD_SECONDS = 5;

    private static final String REFRESH_THRESHOLD_SYSTEM_PROPERTY = "pravega.client.auth.token-refresh.threshold";

    private static final String REFRESH_THRESHOLD_ENV_VARIABLE = "pravega_client_auth_token-refresh.threshold";

    /**
     * The regex pattern for extracting "exp" field from the JWT.
     *
     * Examples:
     *    Input:- {"sub":"subject","aud":"segmentstore","iat":1569837384,"exp":1569837434}, output:- "exp":1569837434
     *    Input:- {"sub": "subject","aud": "segmentstore","iat": 1569837384,"exp": 1569837434}, output:- "exp": 1569837434
     */
    private static final Pattern JWT_EXPIRATION_PATTERN = Pattern.compile("\"exp\":\\s?(\\d+)");

    /**
     * Represents the hreshold (in seconds) for triggering delegation token refresh.
     */
    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final int refreshThresholdInSeconds;

    /**
     * The Controller gRPC client used for interacting with the server.
     */
    private final Controller controllerClient;

    private final String scopeName;

    private final String streamName;

    private final AtomicReference<DelegationToken> delegationToken = new AtomicReference<>();

    private final AtomicReference<CompletableFuture<Void>> tokenRefreshFuture = new AtomicReference<>();

    JwtTokenProviderImpl(Controller controllerClient, String scopeName, String streamName) {
        this(controllerClient, scopeName, streamName, ConfigurationOptionsExtractor.extractInt(
                REFRESH_THRESHOLD_SYSTEM_PROPERTY, REFRESH_THRESHOLD_ENV_VARIABLE, DEFAULT_REFRESH_THRESHOLD_SECONDS));
    }

    private JwtTokenProviderImpl(Controller controllerClient, String scopeName, String streamName,
                                 int refreshThresholdInSeconds) {
        Exceptions.checkNotNullOrEmpty(scopeName, "scopeName");
        Preconditions.checkNotNull(controllerClient, "controllerClient is null");
        Exceptions.checkNotNullOrEmpty(streamName, "streamName");

        this.scopeName = scopeName;
        this.streamName = streamName;
        this.controllerClient = controllerClient;
        this.refreshThresholdInSeconds = refreshThresholdInSeconds;
    }

    /**
     * Constructs a new object with the specified {delegationToken}, {@code controllerClient}, {@code scopeName} and
     * {@code streamName}.
     *
     * @param token the initial delegation token
     * @param controllerClient the {@link Controller} client used for obtaining a delegation token from the Controller
     * @param scopeName the name of the scope tied to the segment, for which a delegation token is to be obtained
     * @param streamName the name of the stream tied to the segment, for which a delegation token is to be obtained
     */
    JwtTokenProviderImpl(String token, Controller controllerClient, String scopeName,
                                String streamName) {
        this(token, controllerClient, scopeName, streamName, ConfigurationOptionsExtractor.extractInt(
                "pravega.client.auth.token-refresh.threshold",
                "pravega_client_auth_token-refresh.threshold",
                DEFAULT_REFRESH_THRESHOLD_SECONDS));
    }

    /**
     * Constructs a new object with the specified {delegationToken}, {@code controllerClient}, {@code scopeName} and
     * {@code streamName}.
     *
     * @param token the initial delegation token
     * @param controllerClient the {@link Controller} client used for obtaining a delegation token from the Controller
     * @param scopeName the name of the scope tied to the segment, for which a delegation token is to be obtained
     * @param streamName the name of the stream tied to the segment, for which a delegation token is to be obtained
     * @param refreshThresholdInSeconds the time in seconds before expiry that should trigger a token refresh
     */
    JwtTokenProviderImpl(String token, Controller controllerClient, String scopeName, String streamName,
                                    int refreshThresholdInSeconds) {
        Exceptions.checkNotNullOrEmpty(token, "delegationToken");
        Exceptions.checkNotNullOrEmpty(scopeName, "scopeName");
        Preconditions.checkNotNull(controllerClient, "controllerClient is null");
        Exceptions.checkNotNullOrEmpty(streamName, "streamName");

        Long expTime = extractExpirationTime(token);
        delegationToken.set(new DelegationToken(token, expTime));
        this.scopeName = scopeName;
        this.streamName = streamName;
        this.controllerClient = controllerClient;
        this.refreshThresholdInSeconds = refreshThresholdInSeconds;
    }

    @VisibleForTesting
    static Long extractExpirationTime(String token) {
        if (token == null || token.trim().equals("")) {
            return null;
        }
        String[] tokenParts = token.split("\\.");

        //A JWT token has 3 parts: the header, the body and the signature.
        if (tokenParts == null || tokenParts.length != 3) {
            return null;
        }

        // The second part of the JWT token is the body, which contains the expiration time if present.
        String encodedBody = tokenParts[1];
        String decodedJsonBody = new String(Base64.getDecoder().decode(encodedBody));

        return parseExpirationTime(decodedJsonBody);
    }

    @VisibleForTesting
    static Long parseExpirationTime(String jwtBody) {
        Long result = null;
        if (jwtBody != null && !jwtBody.trim().equals("")) {
            Matcher matcher = JWT_EXPIRATION_PATTERN.matcher(jwtBody);
            if (matcher.find()) {
               // Should look like this, if a proper match is found: "exp": 1569837434
               String matchedString = matcher.group();

               String[] expiryTimeFieldParts = matchedString.split(":");
               if (expiryTimeFieldParts != null && expiryTimeFieldParts.length == 2) {
                   try {
                       result = Long.parseLong(expiryTimeFieldParts[1].trim());
                   } catch (NumberFormatException e) {
                       // ignore
                       log.warn("Encountered this exception when parsing JWT body for expiration time: {}", e.getMessage());
                   }
               }
            }
        }
        return result;
    }

    /**
     * Returns the delegation token. It returns existing delegation token if it is not close to expiry. If the token
     * is close to expiry, it obtains a new delegation token and returns that one instead.
     *
     * @return String the delegation token JWT compact value
     */
    @Override
    public String retrieveToken() {
        DelegationToken currentToken = this.delegationToken.get();

        if (currentToken == null) {
            return this.refreshToken();
        } else if (currentToken.getExpiryTime() == null) {
            return currentToken.getValue();
        } else if (isTokenNearingExpiry(currentToken)) {
            log.debug("Token is nearing expiry for scope/stream {}/{}", this.scopeName, this.streamName);
            return refreshToken();
        } else {
            return currentToken.getValue();
        }
    }

    @Override
    public boolean populateToken(String token) {
        DelegationToken currentToken = this.delegationToken.get();
        if (token == null || (currentToken != null && currentToken.getValue().equals(""))) {
            return false;
        } else {
            return this.delegationToken.compareAndSet(currentToken, new DelegationToken(token, extractExpirationTime(token)));
        }
    }

    private boolean isTokenNearingExpiry(DelegationToken token) {
        Long currentTokenExpirationTime = token.getExpiryTime();

        // currentTokenExpirationTime can be null if the server returns a null delegation token
        return currentTokenExpirationTime != null && isWithinRefreshThreshold(currentTokenExpirationTime);
    }

    private boolean isWithinRefreshThreshold(Long expirationTime) {
        assert expirationTime != null;
        return isWithinRefreshThreshold(Instant.now(), Instant.ofEpochSecond(expirationTime));
    }

    @VisibleForTesting
    boolean isWithinRefreshThreshold(Instant currentInstant, Instant expiration) {
        return currentInstant.plusSeconds(refreshThresholdInSeconds).getEpochSecond() >= expiration.getEpochSecond();
    }

    @VisibleForTesting
    String refreshToken() {
        long traceEnterId = LoggerHelpers.traceEnter(log, "refreshToken", this.scopeName, this.streamName);
        CompletableFuture<Void> currentRefreshFuture = tokenRefreshFuture.get();
        if (currentRefreshFuture == null) {
            log.debug("Initiated token refresh for scope {} and stream {}", this.scopeName, this.streamName);
            currentRefreshFuture = this.recreateToken();
            this.tokenRefreshFuture.compareAndSet(null, currentRefreshFuture);
        } else {
            log.debug("Token is already under refresh for scope {} and stream {}", this.scopeName, this.streamName);
        }
        try {
            currentRefreshFuture.join(); // Block until the token is refreshed
        } finally {
            this.tokenRefreshFuture.compareAndSet(currentRefreshFuture, null); // Token is already refreshed, so resetting the future to null.
        }
        LoggerHelpers.traceLeave(log, "refreshToken", traceEnterId, this.scopeName, this.streamName);
        return delegationToken.get().getValue();
    }

    private CompletableFuture<Void> recreateToken() {
        return controllerClient.getOrRefreshDelegationTokenFor(scopeName, streamName)
                .thenAccept(token -> this.delegationToken.set(new DelegationToken(token, extractExpirationTime(token))));
    }
}

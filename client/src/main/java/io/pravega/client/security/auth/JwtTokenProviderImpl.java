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
import com.google.common.base.Strings;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Provides JWT-based delegation tokens.
 */
@Slf4j
public class JwtTokenProviderImpl implements DelegationTokenProvider {

    /**
     * Represents the threshold for triggering delegation token refresh.
     */
    @VisibleForTesting
    static final int DEFAULT_REFRESH_THRESHOLD = 5;

    private static final String REFRESH_THRESHOLD_SYSTEM_PROPERTY = "pravega.client.auth.token-refresh.threshold";

    private static final String REFRESH_THRESHOLD_ENV_VARIABLE = "pravega_client_auth_token-refresh.threshold";

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

    protected JwtTokenProviderImpl(Controller controllerClient, String scopeName, String streamName) {
        this(controllerClient, scopeName, streamName, ConfigurationOptionsExtractor.extractInt(
                REFRESH_THRESHOLD_SYSTEM_PROPERTY, REFRESH_THRESHOLD_ENV_VARIABLE, DEFAULT_REFRESH_THRESHOLD));
    }

    private JwtTokenProviderImpl(Controller controllerClient, String scopeName, String streamName,
                                 int tokenRefreshThreshold) {
        Exceptions.checkNotNullOrEmpty(scopeName, "scopeName");
        Preconditions.checkNotNull(controllerClient, "controllerClient is null");
        Exceptions.checkNotNullOrEmpty(streamName, "streamName");

        this.scopeName = scopeName;
        this.streamName = streamName;
        this.controllerClient = controllerClient;
        this.tokenRefreshThreshold = tokenRefreshThreshold;
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
    public JwtTokenProviderImpl(String token, Controller controllerClient, String scopeName,
                                String streamName) {
        this(token, controllerClient, scopeName, streamName, ConfigurationOptionsExtractor.extractInt(
                "pravega.client.auth.token-refresh.threshold",
                "pravega_client_auth_token-refresh.threshold",
                DEFAULT_REFRESH_THRESHOLD));
    }

    /**
     * Constructs a new object with the specified {delegationToken}, {@code controllerClient}, {@code scopeName} and
     * {@code streamName}.
     *
     * @param token the initial delegation token
     * @param controllerClient the {@link Controller} client used for obtaining a delegation token from the Controller
     * @param scopeName the name of the scope tied to the segment, for which a delegation token is to be obtained
     * @param streamName the name of the stream tied to the segment, for which a delegation token is to be obtained
     * @param tokenRefreshThreshold the time in seconds before expiry that should trigger a token refresh
     */
    public JwtTokenProviderImpl(String token, Controller controllerClient, String scopeName, String streamName,
                                    int tokenRefreshThreshold) {
        Exceptions.checkNotNullOrEmpty(token, "delegationToken");
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

    /**
     * Returns the delegation token. It returns existing delegation token if it is not close to expiry. If the token
     * is close to expiry, it obtains a new delegation token and returns that one instead.
     *
     * @return String the delegation token JWT compact value
     */
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
        log.trace("Refreshing token");
        resetToken(newToken());
        String result = delegationToken.get().getValue();
        log.trace("Finished refreshing token. Token is [{}]", Strings.isNullOrEmpty(result) ? result : "not empty");
        return result;
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
        String decodedJsonBody = new String(Base64.getDecoder().decode(encodedBody));

        return parseExpirationTime(decodedJsonBody);
    }

    @VisibleForTesting
    Long parseExpirationTime(String json) {
        Long result = null;
        if (json != null && !json.trim().equals("")) {
            // Sample inputs:
            //    {"sub":"subject","aud":"segmentstore","iat":1569837384,"exp":1569837434}
            //    {"sub": "subject","aud": "segmentstore","iat": 1569837384,"exp": 1569837434}
            Pattern pattern = Pattern.compile("\"exp\":\\s?(\\d+)");
            Matcher matcher = pattern.matcher(json);
            if (matcher.find()) {
               // Will look like this, if a match is found: "exp": 1569837434
               String matchedString = matcher.group();

               String[] expiryTimeFieldParts = matchedString.split(":");
               if (expiryTimeFieldParts != null && expiryTimeFieldParts.length == 2) {
                   result = Long.parseLong(expiryTimeFieldParts[1].trim());
               }
            }
        }
        return result;
    }
}

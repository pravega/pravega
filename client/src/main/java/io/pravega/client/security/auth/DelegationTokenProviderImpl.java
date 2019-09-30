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
import io.pravega.client.stream.impl.Controller;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DelegationTokenProviderImpl implements DelegationTokenProvider {

    /**
     * The delegate.
     */
    @VisibleForTesting
    @Getter(AccessLevel.PACKAGE)
    private final DelegationTokenHandlingStrategy strategy;

    /**
     * Constructs a new object with the specified specified delegation token.
     *
     * <p>
     * Intended for use mainly for testing purposes.
     */
    @VisibleForTesting
    DelegationTokenProviderImpl(@NonNull String token) {
        if (token.trim().equals("")) {
            strategy = new EmptyTokenHandlingStrategy();
            log.debug("Set DelegationTokenHandlingStrategy as {}", EmptyTokenHandlingStrategy.class);
        } else {
            strategy = new StringTokenHandlingStrategy(token);
            log.debug("Set DelegationTokenHandlingStrategy as {}", JwtTokenHandlingStrategy.class);
        }
    }

    /**
     * Constructs a new object with the specified {@code controller}, {@code scopeName} and {@code streamName}.
     *
     * @param controller the {@link Controller} client used for obtaining a delegation token from the Controller
     * @param scopeName the name of the scope tied to the segment, for which a delegation token is to be obtained
     * @param streamName the name of the stream tied to the segment, for which a delegation token is to be obtained
     */
    DelegationTokenProviderImpl(@NonNull Controller controller, @NonNull String scopeName, @NonNull String streamName) {
        strategy = new NullJwtTokenHandlingStrategy(controller, scopeName, streamName);
        log.debug("Set DelegationTokenHandlingStrategy as {}", NullJwtTokenHandlingStrategy.class);
    }

    /**
     * Constructs a new object with the specified {delegationToken}, {@code Controller}, {@code scopeName} and
     * {@code streamName}.
     *
     * @param delegationToken the initial delegation token
     * @param controller the {@link Controller} client used for obtaining a delegation token from the Controller
     * @param scopeName the name of the scope tied to the segment, for which a delegation token is to be obtained
     * @param streamName the name of the stream tied to the segment, for which a delegation token is to be obtained
     */
    DelegationTokenProviderImpl(String delegationToken, @NonNull Controller controller,
                                       @NonNull String scopeName, @NonNull String streamName) {
        if (delegationToken == null) {
            strategy = new NullJwtTokenHandlingStrategy(controller, scopeName, streamName);
            log.debug("Set DelegationTokenHandlingStrategy as {}", NullJwtTokenHandlingStrategy.class);
        } else if (delegationToken.equals("")) {
            strategy = new EmptyTokenHandlingStrategy();
            log.debug("Set DelegationTokenHandlingStrategy as {}", EmptyTokenHandlingStrategy.class);
        } else if (delegationToken.split("\\.").length == 3) {
            strategy = new JwtTokenHandlingStrategy(delegationToken, controller, scopeName,
                    streamName);
            log.debug("Set DelegationTokenHandlingStrategy as {}", JwtTokenHandlingStrategy.class);
        } else {
            strategy = new StringTokenHandlingStrategy(delegationToken);
            log.debug("Set DelegationTokenHandlingStrategy as {}", JwtTokenHandlingStrategy.class);
        }
    }

    /**
     * Returns the delegation token. It returns existing delegation token if it is not close to expiry. If the token
     * is close to expiry, it obtains a new delegation token and returns that one instead.
     *
     * @return String the delegation token JWT compact value
     */
    public String retrieveToken() {
        return strategy.retrieveToken();
    }

    public String refreshToken() {
        return strategy.refreshToken();
    }
}

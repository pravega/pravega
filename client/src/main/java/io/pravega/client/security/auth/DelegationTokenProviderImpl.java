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
import io.pravega.client.segment.impl.Segment;
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
     * Creates an object containing the specified delegation token. Intended for use mainly for testing purposes.
     */
    @VisibleForTesting
    DelegationTokenProviderImpl(@NonNull String token) {
        if (token.trim().equals("")) {
            strategy = new EmptyTokenHandlingStrategy();
            log.debug("Set DelegationTokenHandlingStrategy as {}", EmptyTokenHandlingStrategy.class);
        } else {
            strategy = new NonJwtTokenHandlingStrategy(token);
            log.debug("Set DelegationTokenHandlingStrategy as {}", ValidJwtTokenHandlingStrategy.class);
        }
    }

    public DelegationTokenProviderImpl(@NonNull Controller controllerClient,
                                       @NonNull String scopeName, @NonNull String streamName) {
        strategy = new NullTokenHandlingStrategy(controllerClient, scopeName, streamName);
        log.debug("Set DelegationTokenHandlingStrategy as {}", NullTokenHandlingStrategy.class);
    }

    public DelegationTokenProviderImpl(String token, @NonNull Controller controllerClient, Segment segment) {
        this(token, controllerClient, segment.getScope(), segment.getStreamName());
    }

    public DelegationTokenProviderImpl(@NonNull Controller controllerClient, Segment segment) {
        this(null, controllerClient, segment.getScope(), segment.getStreamName());
    }

    public DelegationTokenProviderImpl(String token, @NonNull Controller controllerClient,
                                       @NonNull String scopeName, @NonNull String streamName) {
        if (token == null) {
            strategy = new NullTokenHandlingStrategy(controllerClient, scopeName, streamName);
            log.debug("Set DelegationTokenHandlingStrategy as {}", NullTokenHandlingStrategy.class);
        } else if (token.equals("")) {
            strategy = new EmptyTokenHandlingStrategy();
            log.debug("Set DelegationTokenHandlingStrategy as {}", EmptyTokenHandlingStrategy.class);
        } else if (token.split("\\.").length == 3) {
            strategy = new ValidJwtTokenHandlingStrategy(token, controllerClient, scopeName,
                    streamName);
            log.debug("Set DelegationTokenHandlingStrategy as {}", ValidJwtTokenHandlingStrategy.class);
        } else {
            strategy = new NonJwtTokenHandlingStrategy(token);
            log.debug("Set DelegationTokenHandlingStrategy as {}", ValidJwtTokenHandlingStrategy.class);
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

    @Override
    public String refreshToken() {
        return strategy.refreshToken();
    }
}

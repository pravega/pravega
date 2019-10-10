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

import io.pravega.common.Exceptions;

import java.util.concurrent.atomic.AtomicReference;

/**
 * A provider for handling non-JWT, non-empty delegation tokens. Used solely for testing purposes.
 */
public class StringTokenProviderImpl implements DelegationTokenProvider {

    private final AtomicReference<String> token = new AtomicReference<>();

    StringTokenProviderImpl(String token) {
        Exceptions.checkNotNullOrEmpty(token, "token");
        this.token.set(token);
    }

    @Override
    public String retrieveToken() {
        return this.token.get();
    }

    @Override
    public boolean populateToken(String token) {
        this.token.set(token);
        return true;
    }
}

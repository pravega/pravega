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

/**
 * A provider for handling non-JWT, non-empty delegation tokens. Used solely for testing purposes.
 */
public class StringTokenProviderImpl implements DelegationTokenProvider {

    private final String token;

    StringTokenProviderImpl(String token) {
        Exceptions.checkNotNullOrEmpty(token, "token");
        this.token = token;
    }

    @Override
    public String retrieveToken() {
        return token;
    }

    @Override
    public String refreshToken() {
        return token;
    }
}

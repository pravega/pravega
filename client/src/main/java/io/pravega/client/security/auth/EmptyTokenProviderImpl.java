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

import java.util.concurrent.CompletableFuture;

/**
 * Provides empty delegation tokens. This provider is useful when auth is disabled.
 */
public class EmptyTokenProviderImpl implements DelegationTokenProvider {

    @Override
    public CompletableFuture<String> retrieveToken() {
        return CompletableFuture.completedFuture("");
    }

    @Override
    public boolean populateToken(String token) {
        return false;
    }

    @Override
    public void signalTokenExpired() {
        // Do nothing
    }
}

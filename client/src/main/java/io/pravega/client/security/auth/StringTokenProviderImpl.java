/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.client.security.auth;

import io.pravega.common.Exceptions;

import java.util.concurrent.CompletableFuture;
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
    public CompletableFuture<String> retrieveToken() {
        return CompletableFuture.completedFuture(this.token.get());
    }

    @Override
    public boolean populateToken(String token) {
        this.token.set(token);
        return true;
    }

    @Override
    public void signalTokenExpired() {
        // Do nothing
    }
}

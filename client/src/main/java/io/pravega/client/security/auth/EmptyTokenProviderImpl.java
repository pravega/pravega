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

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
 * A client-side proxy for obtaining a delegation token from the server.
 *
 * Note: Delegation tokens are used by Segment Store services to authorize requests. They are created by Controller at
 * client's behest.
 */
public interface DelegationTokenProvider {

    /**
     * Retrieve delegation token.
     *
     * @return a CompletableFuture that, when completed, will return the retrieved delegation token
     */
    CompletableFuture<String> retrieveToken();

    /**
     * Populates the object with the specified delegation token.
     *
     * @param token the token to populate the object with
     * @return whether the population was successful
     */
    boolean populateToken(String token);

    /**
     * Signals the object that the token it may be holding has expired.
     */
    void signalTokenExpired();
}

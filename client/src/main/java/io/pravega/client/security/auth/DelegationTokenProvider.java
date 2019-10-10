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
     * @return a delegation token
     */
    String retrieveToken();

    /**
     * Populates the object with the specified delegation token.
     *
     * @param token the token to populate the object with
     * @return whether the population was successful
     */
    boolean populateToken(String token);
}

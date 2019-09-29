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
 * Note:
 * Delegation tokens are used by Segment Store services to authorize requests. They are created by Controllers at
 * client's behest.
 *
 */
public interface DelegationTokenProvider {

    /**
     * Retrieve delegation token.
     *
     * @return a delegation token in JWT format
     */
    String retrieveToken();

    /**
     * Refresh and retrieve delegation token.
     *
     * @return a new delegation token in JWT format
     */
    String refreshToken();
}

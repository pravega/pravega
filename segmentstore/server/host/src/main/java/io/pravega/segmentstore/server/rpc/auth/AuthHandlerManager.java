/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.rpc.auth;

import io.pravega.auth.AuthException;
import lombok.extern.slf4j.Slf4j;
import java.security.Principal;
import com.google.common.base.Preconditions;

/**
 * Manages instances of {@link AuthHandler} for the SegmentStore REST interfaces.
 *
 * In case of grpc, the routing of the authenticate function to specific registered interceptor is taken care by grpc
 * interceptor mechanism. In case of REST calls, this class routes the call to specific AuthHandler.
 */
@Slf4j
public class AuthHandlerManager {
    /**
     *
     * API to authenticate a given credential.
     * @param credentials  Credentials used for authentication.
     *
     * @return Returns the Principal if the entity represented by credentials is authenticated.
     * @throws AuthException if an authentication failure occurred.
     */
    public Principal authenticate(String credentials) throws AuthException {
        Preconditions.checkNotNull(credentials, "credentials");
        return null;
    }
}

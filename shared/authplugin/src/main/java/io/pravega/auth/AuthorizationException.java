/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.auth;

/**
 * Exception thrown when there is any error during authorization.
 */
public class AuthorizationException extends AuthException {
    private static final long serialVersionUID = 1L;
    public AuthorizationException(String message) {
        this(message, 401);
    }

    public AuthorizationException(String message, int responseCode) {
        super(message, responseCode);
    }

    public AuthorizationException(Exception e) {
        this(e, 403);
    }

    public AuthorizationException(Exception e, int responseCode) {
        super(e, responseCode);
    }
}

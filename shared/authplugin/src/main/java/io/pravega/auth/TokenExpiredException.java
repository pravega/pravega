/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.auth;

/**
 * Indicates that the token has expired.
 *
 * Expiry for a JWT is determined based on 'exp' claim, as described here: https://tools.ietf.org/html/rfc7519#section-4.1.4.
 */
public class TokenExpiredException extends TokenException {

    private static final long serialVersionUID = 1L;

    public TokenExpiredException(String message) {
        super(message);
    }

    public TokenExpiredException(Exception e) {
        super(e);
    }
}

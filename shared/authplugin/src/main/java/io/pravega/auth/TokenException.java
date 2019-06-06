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

public class TokenException extends AuthException {

    private static final long serialVersionUID = 1L;

    public TokenException(String message) {
        super(message, 401);
    }

    public TokenException(Exception e) {
        super(e, 401);
    }
}

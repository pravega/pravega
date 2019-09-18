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
 * Indicates that the delegation token is invalid.
 *
 * The token may be invalid due to these and other reasons:
 *     - The token is malformed.
 *     - The token has unexpected content. An example of this is when the token doesn't contain any standard or custom claim.
 *     - Failed to calculate token signature or to verify it.
 */
public class InvalidTokenException extends TokenException {

    private static final long serialVersionUID = 1L;

    public InvalidTokenException(String message) {
        super(message);
    }

    public InvalidTokenException(Exception e) {
        super(e);
    }
}

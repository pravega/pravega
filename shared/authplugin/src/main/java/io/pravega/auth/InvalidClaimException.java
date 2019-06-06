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

public class InvalidClaimException extends TokenException {

    private static final long serialVersionUID = 1L;

    public InvalidClaimException(String message) {
        super(message);
    }

    public InvalidClaimException(Exception e) {
        super(e);
    }
}


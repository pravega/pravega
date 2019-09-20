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

//Represents exceptions during authorization/authentication

public class AuthException extends Exception {
    private static final long serialVersionUID = 1L;
    private final int responseCode;

    public AuthException(String message, int responseCode) {
        super(message);
        this.responseCode = responseCode;
    }

    public AuthException(Exception e, int responseCode) {
        super(e);
        this.responseCode = responseCode;
    }

    public int getResponseCode() {
        return responseCode;
    }
}

/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import io.pravega.auth.AuthConstants;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import lombok.EqualsAndHashCode;

/**
 * Username/password credentials for basic authentication.
 */
@EqualsAndHashCode
public class DefaultCredentials implements Credentials {
    
    private static final long serialVersionUID = 1L;

    private final String token;

    public DefaultCredentials(String password, String userName) {
        String decoded = userName + ":" + password;
        this.token = Base64.getEncoder().encodeToString(decoded.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public String getAuthenticationType() {
        return AuthConstants.BASIC;
    }

    @Override
    public String getAuthenticationToken() {
        return token;
    }
}

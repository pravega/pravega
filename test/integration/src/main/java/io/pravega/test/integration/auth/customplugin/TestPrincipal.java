/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.auth.customplugin;

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.security.Principal;

@Slf4j
@ToString
public class TestPrincipal implements Principal {

    private String token;

    TestPrincipal(String token) {
        if (token == null || token.trim().isEmpty()) {
            log.warn("Token is blank");
            throw new IllegalArgumentException("token is blank");
        }
        this.token = token;
    }

    @Override
    public String getName() {
        return token;
    }
}
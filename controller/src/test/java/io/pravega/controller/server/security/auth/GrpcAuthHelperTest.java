/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.security.auth;

import io.pravega.auth.AuthHandler;
import io.pravega.shared.security.auth.AuthorizationResource;
import io.pravega.shared.security.auth.AuthorizationResourceImpl;
import io.pravega.shared.security.token.JsonWebToken;
import io.pravega.shared.security.token.JwtParser;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Slf4j
public class GrpcAuthHelperTest {

    private AuthorizationResource authResource = new AuthorizationResourceImpl();

    @Test
    public void createsNonEmptyDelegationTokenWhenAuthIsEnabled() {
        GrpcAuthHelper helper = new GrpcAuthHelper(true, "tokenSigningKey", 600);
        String resource = authResource.ofStreamInScope("testScope", "testStream");
        String token = helper.createDelegationToken(resource, AuthHandler.Permissions.READ);
        assertNotNull(token);

        JsonWebToken jwt = JwtParser.parse(token, "tokenSigningKey".getBytes());
        assertNotNull(jwt);

        assertEquals("READ", jwt.getPermissionsByResource().get(resource));
    }

    @Test
    public void createsEmptyDelegationTokenWhenAuthIsDisabled() {
        GrpcAuthHelper helper = new GrpcAuthHelper(false, "tokenSigningKey", 600);
        String token = helper.createDelegationToken(
                authResource.ofStreamInScope("testScope", "testStream"),
                AuthHandler.Permissions.READ);
        assertEquals("", token);
    }
}

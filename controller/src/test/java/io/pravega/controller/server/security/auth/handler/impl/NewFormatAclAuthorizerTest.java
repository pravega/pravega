/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.security.auth.handler.impl;

import io.pravega.auth.AuthHandler;
import io.pravega.controller.server.security.auth.AuthorizationResource;
import io.pravega.controller.server.security.auth.AuthorizationResourceImpl;
import org.junit.Test;

import java.util.Arrays;

import static io.pravega.controller.server.security.auth.AuthorizationResourceImpl.DOMAIN_PART_SUFFIX;
import static org.junit.Assert.assertEquals;

public class NewFormatAclAuthorizerTest {

    private final static String DUMMY_ENCRYPTED_PWD = "Dummy encrypted value";

    private AuthorizationResource authorizationResource = new AuthorizationResourceImpl();

    @Test
    public void testSuperUserAcl() {
        AccessControlList acl = new AccessControlList(DUMMY_ENCRYPTED_PWD, Arrays.asList(
                new AccessControlEntry(DOMAIN_PART_SUFFIX + "*", AuthHandler.Permissions.READ_UPDATE)
        ));

        NewFormatAclAuthorizer authorizer = new NewFormatAclAuthorizer();
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authorizer.authorize(acl, authorizationResource.ofScopes()));
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authorizer.authorize(acl, authorizationResource.ofScope("testScope")));
        assertEquals(AuthHandler.Permissions.READ_UPDATE, authorizer.authorize(acl, authorizationResource.ofStreamInScope("testScope", "testStream")));

        /*assertTrue("pravega::/".matches(aceResourcePattern));
        assertTrue("pravega::/scope:testscope".matches(aceResourcePattern));
        assertTrue("pravega::/scope:testscope/stream:teststream".matches(aceResourcePattern));
        assertTrue("pravega::/scope:testscope/readerGroup:testrg".matches(aceResourcePattern));*/
    }
}

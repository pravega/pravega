/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.security.auth;

import io.pravega.test.common.AssertExtensions;
import org.junit.Test;

import java.util.Base64;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DefaultCredentialsTest {

    @Test
    public void ctorThrowsExceptionIfInputIsNull() {
        AssertExtensions.assertThrows(NullPointerException.class, () -> new DefaultCredentials(null, "pwd"));
        AssertExtensions.assertThrows(NullPointerException.class, () -> new DefaultCredentials("username", null));
    }

    @Test
    public void ctorCreatesValidObjectForValidInput() {
        String userName = "username";
        String password = "pwd";
        Credentials credentials = new DefaultCredentials(password, userName);
        assertEquals("Basic", credentials.getAuthenticationType());

        String token = credentials.getAuthenticationToken();
        assertNotNull(token);

        String credentialsString = new String(Base64.getDecoder().decode(token));
        assertEquals(userName + ":" + password, credentialsString);
    }

    @Test
    public void testEqualsAndHashCode() {
        Credentials credentials1 = new DefaultCredentials("pwd", "user");
        Credentials credentials2 = new DefaultCredentials("pwd", "user");
        assertEquals(credentials1, credentials2);
        assertEquals(credentials1.hashCode(), credentials2.hashCode());
    }
}

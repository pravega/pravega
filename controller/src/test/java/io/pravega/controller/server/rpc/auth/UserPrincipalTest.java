/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.rpc.auth;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Unit tests for the UserPrincipal class.
 */
public class UserPrincipalTest {

    @Test (expected = NullPointerException.class)
    public void testCtorThrowsExceptionWhenInputIsNull() {
        new UserPrincipal(null);
    }

    @Test (expected = IllegalArgumentException.class)
    public void testCtorThrowsExceptionWhenInputIsEmpty() {
        new UserPrincipal("");
    }

    @Test
    public void testObjectsWithSameNameAreEqual() {
        assertEquals(new UserPrincipal("abc"), new UserPrincipal("abc"));
    }
}

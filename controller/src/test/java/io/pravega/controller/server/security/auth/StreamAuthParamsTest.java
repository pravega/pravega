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
import io.pravega.test.common.AssertExtensions;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StreamAuthParamsTest {

    @Test
    public void rejectsConstructionWhenInputIsInvalid() {
        AssertExtensions.assertThrows("Scope was null",
                () -> new StreamAuthParams(null, "_internalStream", true),
                e -> e instanceof NullPointerException);

        AssertExtensions.assertThrows("Stream was null",
                () -> new StreamAuthParams("scope", null, true),
                e -> e instanceof NullPointerException);

        AssertExtensions.assertThrows("Stream is not internal",
                () -> new StreamAuthParams("scope", "externalStream", false),
                e -> e instanceof IllegalArgumentException);
    }

    @Test
    public void returnsReadPermissionWhenConfigIsTrue() {
        StreamAuthParams params = new StreamAuthParams("scope", "_internalStream", true);
        assertEquals(AuthHandler.Permissions.READ, params.requiredPermissionForWrites());
    }

    @Test
    public void returnsReadUpdatePermissionWhenConfigIsTrue() {
        StreamAuthParams params = new StreamAuthParams("scope", "_internalStream", false);
        assertEquals(AuthHandler.Permissions.READ_UPDATE, params.requiredPermissionForWrites());
    }
}

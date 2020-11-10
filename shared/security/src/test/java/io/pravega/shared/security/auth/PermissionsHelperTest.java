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

import io.pravega.auth.AuthHandler;
import io.pravega.test.common.AssertExtensions;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PermissionsHelperTest {

    @Test
    public void translatesValidPermissions() {
        assertEquals(AuthHandler.Permissions.READ, PermissionsHelper.toAuthHandlerPermissions(AccessOperation.READ));
        assertEquals(AuthHandler.Permissions.READ_UPDATE, PermissionsHelper.toAuthHandlerPermissions(AccessOperation.WRITE));
        assertEquals(AuthHandler.Permissions.READ_UPDATE, PermissionsHelper.toAuthHandlerPermissions(AccessOperation.READ_WRITE));
    }

    @Test
    public void throwsExceptionForNotUnderstoodPermissions() {
        AssertExtensions.assertThrows(IllegalArgumentException.class,
                () -> PermissionsHelper.toAuthHandlerPermissions(AccessOperation.ANY));

        AssertExtensions.assertThrows(NullPointerException.class,
                () -> PermissionsHelper.toAuthHandlerPermissions(null));
    }

    @Test
    public void parsesEmptyPermissionStringToDefault() {
        assertEquals(AuthHandler.Permissions.READ, PermissionsHelper.parse(null, AuthHandler.Permissions.READ));
    }

    @Test
    public void parsesNonEmptyPermissionStrings() {
        assertEquals(AuthHandler.Permissions.READ, PermissionsHelper.parse(AccessOperation.READ,
                AuthHandler.Permissions.NONE));
        assertEquals(AuthHandler.Permissions.READ_UPDATE, PermissionsHelper.parse(AccessOperation.WRITE,
                AuthHandler.Permissions.NONE));
        AssertExtensions.assertThrows(IllegalArgumentException.class,
                () -> PermissionsHelper.parse(AccessOperation.ANY, null));
    }
}

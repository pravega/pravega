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
import lombok.NonNull;

public class PermissionsHelper {

    public static AuthHandler.Permissions parse(@NonNull String permissions, AuthHandler.Permissions defaultPermissions) {
        if (permissions.equals("")) {
            return defaultPermissions;
        }
        try {
            return AuthHandler.Permissions.valueOf(permissions);
        } catch (IllegalArgumentException e) {
            return defaultPermissions;
        }
    }
}

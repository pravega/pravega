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
import lombok.NonNull;

public class PermissionsHelper {

    public static AuthHandler.Permissions toAuthHandlerPermissions(AccessOperation accessOperation) {
        if (accessOperation.equals(AccessOperation.READ)) {
            return AuthHandler.Permissions.READ;
        } else if (accessOperation.equals(AccessOperation.WRITE) ||
                accessOperation.equals(AccessOperation.READ_UPDATE)) {
            return AuthHandler.Permissions.READ_UPDATE;
        } else {
            throw new IllegalArgumentException("Cannot translate");
        }
    }

    public static AuthHandler.Permissions parse(@NonNull String accessOperation, AuthHandler.Permissions defaultPermissions) {
        if (accessOperation.equals("")) {
            return defaultPermissions;
        }
        try {
            return toAuthHandlerPermissions(AccessOperation.valueOf(accessOperation));
        } catch (IllegalArgumentException e) {
            return defaultPermissions;
        }
    }
}

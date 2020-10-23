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

import com.google.common.base.Strings;
import io.pravega.auth.AuthHandler;
import lombok.NonNull;

/**
 * Helper methods for permissions.
 */
public class PermissionsHelper {

    /**
     * Translates the specified {@code accessOperation} to an {@link AuthHandler.Permissions} object.
     *
     * @param accessOperation accessOperation to translate
     * @return a {@link AuthHandler.Permissions} object that represents the specified {@code accessOperation}
     */
    public static AuthHandler.Permissions toAuthHandlerPermissions(@NonNull AccessOperation accessOperation) {
        if (accessOperation.equals(AccessOperation.READ)) {
            return AuthHandler.Permissions.READ;
        } else if (accessOperation.equals(AccessOperation.WRITE) || accessOperation.equals(AccessOperation.READ_WRITE)) {
            return AuthHandler.Permissions.READ_UPDATE;
        } else {
            throw new IllegalArgumentException("Cannot translate access operation " + accessOperation.name());
        }
    }

    /**
     * Parse the specified {@code accessOperationStr} string and translate it to an {@link AuthHandler.Permissions} object.
     *
     * @param accessOperationStr a string value of an {@link AccessOperation} object
     * @param defaultPermissions the default {@link AuthHandler.Permissions} object to return in case the
     *                           {@code accessOperationStr} can't be parsed.
     * @return the parsed or default {@link AuthHandler.Permissions} object,
     */
    public static AuthHandler.Permissions parse(String accessOperationStr, AuthHandler.Permissions defaultPermissions) {
        if (Strings.isNullOrEmpty(accessOperationStr)) {
            return defaultPermissions;
        }
        try {
            return toAuthHandlerPermissions(AccessOperation.valueOf(accessOperationStr));
        } catch (IllegalArgumentException e) {
            return defaultPermissions;
        }
    }
}
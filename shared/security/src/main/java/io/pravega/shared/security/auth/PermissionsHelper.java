/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.shared.security.auth;

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
        } else if (accessOperation.equals(AccessOperation.NONE)) {
            return AuthHandler.Permissions.NONE;
        } else {
            throw new IllegalArgumentException("Cannot translate access operation " + accessOperation.name());
        }
    }

    /**
     * Parse the specified {@code accessOperationStr} string and translate it to an {@link AuthHandler.Permissions} object.
     *
     * @param accessOperation the intended {@link AccessOperation}
     * @param defaultPermissions the default {@link AuthHandler.Permissions} object to return in case the
     *                           {@code accessOperationStr} can't be parsed.
     * @return the parsed or default {@link AuthHandler.Permissions} object,
     */
    public static AuthHandler.Permissions parse(AccessOperation accessOperation, AuthHandler.Permissions defaultPermissions) {
        if (accessOperation  == null || accessOperation.equals(AccessOperation.UNSPECIFIED)) {
            return defaultPermissions;
        }
        return toAuthHandlerPermissions(accessOperation);
    }
}
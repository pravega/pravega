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

class OldFormatAclAuthorizer extends AclAuthorizer {

    @Override
    public AuthHandler.Permissions authorize(AccessControlList accessControlList, String resource) {
        AuthHandler.Permissions result = AuthHandler.Permissions.NONE;

        /*
         *  `*` Means a wildcard.
         *  If It is a direct match, return the ACLs.
         *  If it is a partial match, the target has to end with a `/`
         */
        for (AccessControlEntry accessControlEntry : accessControlList.getEntries()) {

            // Separating into different blocks, to make the code more understandable.
            // It makes the code look a bit strange, but it is still simpler and easier to decipher than what it be
            // if we combine the conditions.

            if (accessControlEntry.isResource(resource)) {
                // Example: resource = "myscope", accessControlEntry-resource = "myscope"
                result = accessControlEntry.getPermissions();
                break;
            }

            if (accessControlEntry.isResource("/*") && !resource.contains("/")) {
                // Example: resource = "myscope", accessControlEntry-resource ="/*"
                result = accessControlEntry.getPermissions();
                break;
            }

            if (accessControlEntry.resourceEndsWith("/") && accessControlEntry.resourceStartsWith(resource)) {
                result = accessControlEntry.getPermissions();
                break;
            }

            // Say, resource is myscope/mystream. ACL specifies permission for myscope/*.
            // Auth should return the the ACL's permissions in that case.
            if (resource.contains("/") && !resource.endsWith("/")) {
                String[] values = resource.split("/");
                if (accessControlEntry.isResource(values[0] + "/*")) {
                    result = accessControlEntry.getPermissions();
                    break;
                }
            }

            if (accessControlEntry.isResource("*") && accessControlEntry.hasHigherPermissionsThan(result)) {
                result = accessControlEntry.getPermissions();
                break;
            }
        }
        return result;
    }
}

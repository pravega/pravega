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
import lombok.NonNull;

class AclAuthorizerImpl extends AclAuthorizer {

    @Override
    public AuthHandler.Permissions authorize(@NonNull AccessControlList accessControlList, @NonNull String resource) {
        AuthHandler.Permissions result = AuthHandler.Permissions.NONE;
        String resourceDomain = resource.split("::")[0];
        for (AccessControlEntry accessControlEntry : accessControlList.getEntries()) {
            // You could have a null ACE in the ACL if you had a malformed entry such as `prn::/scope:readresource`
            // having no permissions set.
            if (accessControlEntry != null && accessControlEntry.resourceStartsWith(resourceDomain)) {
                if (resource.matches(accessControlEntry.getResourcePattern())) {
                    result = accessControlEntry.getPermissions();
                    break;
                }
            }
        }
        return result;
    }
}

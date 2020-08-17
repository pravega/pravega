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
import lombok.AccessLevel;
import lombok.Getter;

/**
 * An entry of an {@link AccessControlList}.
 */
class AccessControlEntry {
    @Getter(AccessLevel.PACKAGE)
    private final String resourcePattern;

    @Getter(AccessLevel.PACKAGE)
    private final AuthHandler.Permissions permissions;

    AccessControlEntry(String aceResource, AuthHandler.Permissions permissions) {
        if (aceResource != null) {
            this.resourcePattern = aceResource;
            this.permissions = permissions;
        } else {
            this.resourcePattern = null;
            this.permissions = null;
        }
    }

    boolean isResource(String resource) {
        return resourcePattern.equals(resource);
    }

    boolean resourceEndsWith(String resource) {
        return resourcePattern.endsWith(resource);
    }

    boolean resourceStartsWith(String resource) {
        return resourcePattern.startsWith(resource);
    }

    boolean hasHigherPermissionsThan(AuthHandler.Permissions input) {
        return this.permissions.ordinal() > input.ordinal();
    }
}

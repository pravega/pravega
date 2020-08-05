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

import com.google.common.annotations.VisibleForTesting;
import io.pravega.auth.AuthHandler;
import lombok.Data;

@VisibleForTesting
@Data
class AccessControlEntry {
    private final String resourcePattern;
    private final AuthHandler.Permissions permissions;

    public boolean isResource(String resource) {
        return resourcePattern.equals(resource);
    }

    public boolean resourceEndsWith(String resource) {
        return resourcePattern.endsWith(resource);
    }

    public boolean resourceStartsWith(String resource) {
        return resourcePattern.startsWith(resource);
    }

    public boolean hasHigherPermissionsThan(AuthHandler.Permissions input) {
        return this.permissions.ordinal() > input.ordinal();
    }
}

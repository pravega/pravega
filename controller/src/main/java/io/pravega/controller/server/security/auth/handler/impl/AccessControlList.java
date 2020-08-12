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

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.List;

/**
 * Represents an access control list (ACL). An ACL specifies the permissions that a user has, for a set of resources.
 */
@RequiredArgsConstructor
class AccessControlList {
    @Getter(AccessLevel.PACKAGE)
    private final String encryptedPassword;

    @Getter(AccessLevel.PACKAGE)
    private final List<AccessControlEntry> entries;
}

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
import lombok.Data;

import java.util.List;

@VisibleForTesting
@Data
class AccessControlList {
    private final String encryptedPassword;
    private final List<AccessControlEntry> acl;
}

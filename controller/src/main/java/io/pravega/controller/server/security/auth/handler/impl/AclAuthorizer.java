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

abstract class AclAuthorizer {

    private final static NewFormatAclAuthorizer AUTHORIZER_FOR_NEW_FORMAT = new NewFormatAclAuthorizer();
    private final static OldFormatAclAuthorizer AUTHORIZER_FOR_LEGACY_FORMAT = new OldFormatAclAuthorizer();

    static AclAuthorizer instance(boolean isOldAclAuthorizer) {
        if (isOldAclAuthorizer) {
            return AUTHORIZER_FOR_LEGACY_FORMAT;
        } else {
            return AUTHORIZER_FOR_NEW_FORMAT;
        }
    }

    abstract AuthHandler.Permissions authorize(AccessControlList accessControlList, String resource);
}

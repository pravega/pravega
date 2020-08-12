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

/**
 * Authorizes resources based on supplied ACLs.
 */
abstract class AclAuthorizer {

    private final static AclAuthorizerImpl AUTHORIZER_FOR_NEW_FORMAT = new AclAuthorizerImpl();
    private final static LegacyAclAuthorizerImpl AUTHORIZER_FOR_LEGACY_FORMAT = new LegacyAclAuthorizerImpl();

    /**
     * Returns a cached instance of the legacy implementation.
     *
     * @return an instance
     */
    static AclAuthorizer legacyAuthorizerInstance() {
        return AUTHORIZER_FOR_LEGACY_FORMAT;
    }

    /**
     * Returns a cached instance of the implementation.
     *
     * @return an instance
     */
    static AclAuthorizer instance() {
        return AUTHORIZER_FOR_NEW_FORMAT;
    }

    /**
     * Authorize resource based on the specified {@code accessControlList}.
     *
     * @param accessControlList the ACLs describing the permissions that the user has
     * @param resource the resource for which authorization is being seeked.
     * @return the permissions that the user has on the specified resource, based on the specified acl
     */
    abstract AuthHandler.Permissions authorize(AccessControlList accessControlList, String resource);
}

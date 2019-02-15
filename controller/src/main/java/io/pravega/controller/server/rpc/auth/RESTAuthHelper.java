/**
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.rpc.auth;

import com.google.common.base.Preconditions;
import io.pravega.auth.AuthException;
import io.pravega.auth.AuthHandler;
import io.pravega.auth.AuthenticationException;
import io.pravega.auth.AuthorizationException;

import javax.ws.rs.core.Response;
import java.security.Principal;
import java.util.List;

/**
 * Helper class for handling auth (authentication and authorization) for the REST API.
 */
public class RESTAuthHelper {

    private final PravegaAuthManager pravegaAuthManager;

    public RESTAuthHelper(PravegaAuthManager pravegaAuthManager) {
        this.pravegaAuthManager = pravegaAuthManager;
    }

    public boolean isAuthorized(List<String> authHeader, String resourceName, Principal principal,
                                AuthHandler.Permissions permissionLevel)
            throws AuthException {
        if (pravegaAuthManager == null ) {
            // Implies auth is disabled. Since auth is disabled, every request is deemed to have been authorized.
            return true;
        } else {
            return pravegaAuthManager.authorize(resourceName,
                    principal,
                    parseCredentials(authHeader),
                    permissionLevel);
        }
    }

    public void authorize(List<String> authHeader, String resourceName, Principal principal,
                          AuthHandler.Permissions permissionLevel) throws AuthException {
        if (!isAuthorized(authHeader, resourceName, principal, permissionLevel)) {
            throw new AuthorizationException("Authorization failed for " + resourceName,
                    Response.Status.FORBIDDEN.getStatusCode());
        }
    }

    public  Principal authenticate(List<String> authHeader) throws AuthException {
        if (pravegaAuthManager != null ) {
            String credentials = parseCredentials(authHeader);
            return pravegaAuthManager.authenticate(credentials);
        }
        return null;
    }

    public void authenticateAuthorize(List<String> authHeader, String resourceName, AuthHandler.Permissions level)
            throws AuthException {
        if (pravegaAuthManager != null) {
            String credentials = parseCredentials(authHeader);

            if (!pravegaAuthManager.authenticateAndAuthorize(resourceName, credentials, level)) {
                throw new AuthorizationException("Auth failed for " + resourceName,
                        Response.Status.FORBIDDEN.getStatusCode());
            }
        }
    }

    private String parseCredentials(List<String> authHeader) throws AuthenticationException {
        if (authHeader == null || authHeader.isEmpty()) {
            throw new AuthenticationException("Missing authorization header.");
        }

        // Expecting a single value here. If there are multiple, we'll deal with just the first one.
        String credentials = authHeader.get(0);
        Preconditions.checkNotNull(credentials, "Missing credentials.");
        return credentials;
    }
}
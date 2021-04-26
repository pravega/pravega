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
package io.pravega.shared.rest.security;

import com.google.common.annotations.VisibleForTesting;
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

    /**
     * The delegate used for performing authentication and authorization.
     */
    private final AuthHandlerManager pravegaAuthManager;

    public RESTAuthHelper(AuthHandlerManager pravegaAuthManager) {
        this.pravegaAuthManager = pravegaAuthManager;
    }

    /**
     * Determines whether the given {@code principal} has specified {@code permission} on the given {@code resource}.
     *
     * @param authHeader contents of an HTTP Authorization header
     * @param resource   representation of the resource being accessed
     * @param principal  the identity of the subject accessing the resource
     * @param permission the permission
     * @return {@code true} if either auth is disabled or authorization is granted, and  {@code false}
     *         if auth is enabled and authorization is not granted
     * @throws AuthException if either authentication or authorization fails
     */
    public boolean isAuthorized(List<String> authHeader, String resource, Principal principal,
                                AuthHandler.Permissions permission)
            throws AuthException {
        if (isAuthEnabled()) {
            return pravegaAuthManager.authorize(resource,
                    principal,
                    parseCredentials(authHeader),
                    permission);
        } else {
            // Since auth is disabled, every request is deemed to have been authorized.
            return true;
        }
    }

    /**
     * Ensures that the given {@code principal} has specified {@code permission} on the given {@code resource}.
     *
     * @param authHeader contents of an HTTP Authorization header
     * @param resource   representation of the resource being accessed
     * @param principal  the identity of the subject accessing the resource
     * @param permission the permission
     * @throws AuthException if authentication or authorization fails
     */
    public void authorize(List<String> authHeader, String resource, Principal principal,
                          AuthHandler.Permissions permission) throws AuthException {
        if (!isAuthorized(authHeader, resource, principal, permission)) {
            throw new AuthorizationException(
                    String.format("Failed to authorize for resource [%s]", resource),
                    Response.Status.FORBIDDEN.getStatusCode());
        }
    }

    /**
     * Authenticates the subject represented by the specified HTTP Authorization Header value.
     *
     * @param authHeader contents of an HTTP Authorization header
     * @return a {@code principal} representing the identity of the subject if auth is enabled; otherwise {@code null}
     * @throws AuthException if authentication fails
     */
    public Principal authenticate(List<String> authHeader) throws AuthException {
        if (isAuthEnabled()) {
            String credentials = parseCredentials(authHeader);
            return pravegaAuthManager.authenticate(credentials);
        }
        return null;
    }

    /**
     * Ensures that the subject represented by the given {@code authHeader} is authenticated and that the subject is
     * authorized for the specified {@code permission} on the given {@code resource}.
     *
     * @param authHeader contents of an HTTP Authorization header
     * @param resource   representation of the resource being accessed
     * @param permission the permission
     * @throws AuthException if authentication/authorization fails
     */
    public void authenticateAuthorize(List<String> authHeader, String resource, AuthHandler.Permissions permission)
            throws AuthException {
        if (isAuthEnabled()) {
            String credentials = parseCredentials(authHeader);
            if (!pravegaAuthManager.authenticateAndAuthorize(resource, credentials, permission)) {
                throw new AuthorizationException(
                        String.format("Failed to authorize for resource [%s]", resource),
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

    @VisibleForTesting
    public boolean isAuthEnabled() {
        return pravegaAuthManager != null;
    }
}
/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.rpc.auth;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.auth.AuthHandler;
import io.pravega.auth.AuthorizationException;
import io.pravega.shared.security.token.JsonWebToken;

import java.util.HashMap;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.auth.AuthHandler.Permissions.READ_UPDATE;

/**
 * Helper class containing APIs related to delegation token and authorization/authentication, for gRPC interface.
 */
@AllArgsConstructor
@Slf4j
public class GrpcAuthHelper {

    @Getter
    private final boolean isAuthEnabled;
    private final String tokenSigningKey;
    private final Integer accessTokenTTLInSeconds;

    @VisibleForTesting
    public static GrpcAuthHelper getDisabledAuthHelper() {
        return new GrpcAuthHelper(false, "", -1);
    }

    public boolean isAuthorized(String resource, AuthHandler.Permissions permission, AuthContext authContext) {
        if (isAuthEnabled) {
            AuthHandler.Permissions allowedLevel;
            if (authContext == null) {
                log.warn("Auth is enabled but 'authContext'  is null. Defaulting to no permissions.");
                allowedLevel = AuthHandler.Permissions.NONE;
            } else {
                allowedLevel = authContext.getAuthHandler().authorize(resource, authContext.getPrincipal());
            }
            return (allowedLevel.ordinal() < permission.ordinal()) ? false : true;
        } else {
            log.debug("Since auth is disabled, returning [true]");
            return true;
        }
    }

    public String checkAuthorization(String resource, AuthHandler.Permissions expectedLevel, AuthContext ctx) {
        if (isAuthorized(resource, expectedLevel, ctx)) {
            return "";
        } else {
            if (ctx == null) {
                log.warn("AuthContext is null");
            }
            String message = String.format("Principal [%s] not allowed [%s] access for resource [%s]",
                    ctx != null ? ctx.getPrincipal() : null, expectedLevel.toString(), resource);
            throw new RuntimeException(new AuthorizationException(message));
        }
    }

    public String checkAuthorization(String resource, AuthHandler.Permissions expectedLevel) {
        return checkAuthorization(resource, expectedLevel, AuthContext.current());
    }

    public String checkAuthorizationAndCreateToken(String resource, AuthHandler.Permissions expectedLevel) {
        if (isAuthEnabled) {
            checkAuthorization(resource, expectedLevel);
            return createDelegationToken(resource, expectedLevel, tokenSigningKey);
        }
        return "";
    }

    private String createDelegationToken(String resource, AuthHandler.Permissions expectedLevel, String tokenSigningKey) {
        if (isAuthEnabled) {
            Map<String, Object> claims = new HashMap<>();
            claims.put(resource, String.valueOf(expectedLevel));

            return new JsonWebToken("segmentstoreresource", "segmentstore", tokenSigningKey.getBytes(),
                            this.accessTokenTTLInSeconds, claims).toCompactString();
        } else {
            return "";
        }
    }

    public String retrieveMasterToken() {
        if (isAuthEnabled) {
            return GrpcAuthHelper.retrieveMasterToken(tokenSigningKey);
        } else {
            return "";
        }
    }

    /**
     * Retrieves a master token for internal controller to segment store communication.
     *
     * @param tokenSigningKey Signing key for the JWT token.
     * @return A new master token which has highest privileges.
     */
    public static String retrieveMasterToken(String tokenSigningKey) {
        Map<String, Object> customClaims = new HashMap<>();
        customClaims.put("*", String.valueOf(READ_UPDATE));

        return new JsonWebToken("segmentstoreresource", "segmentstore", tokenSigningKey.getBytes(),
                null, customClaims).toCompactString();
    }
}

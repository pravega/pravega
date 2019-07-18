/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
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

import java.security.Principal;
import java.util.HashMap;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Helper class containing APIs related to delegation token and authorization/authentication, for gRPC interface.
 */
@AllArgsConstructor
@Slf4j
public class GrpcAuthHelper {

    private final boolean isAuthEnabled;
    private final String tokenSigningKey;
    private final Integer accessTokenTTLInSeconds;

    public AuthHandler.Permissions authorize(AuthHandler handler, String resource, Principal principal) {
        return handler.authorize(resource, principal);
    }

    @VisibleForTesting
    public static GrpcAuthHelper getDisabledAuthHelper() {
        return new GrpcAuthHelper(false, "", -1);
    }

    public boolean isAuthorized(String resource, AuthHandler.Permissions permission, AuthInterceptor currentInterceptor, Principal principal) {
        if (isAuthEnabled) {
            AuthHandler.Permissions allowedLevel;
            if (currentInterceptor == null) {
                //No interceptor, and authorization is enabled. That means no access is granted.
                log.warn("Auth is enabled but current interceptor is null. Defaulting to no permissions.");
                allowedLevel = AuthHandler.Permissions.NONE;
            } else {
                 AuthHandler handler = currentInterceptor.getHandler();
                 allowedLevel = handler.authorize(resource, principal);
            }
            return (allowedLevel.ordinal() < permission.ordinal()) ? false : true;
        } else {
            return true;
        }
    }

    public boolean isAuthorized(String resource, AuthHandler.Permissions permission, AuthInterceptor currentInterceptor) {
        if (isAuthEnabled) {
            AuthHandler.Permissions allowedLevel;
            if (currentInterceptor == null) {
                //No interceptor, and authorization is enabled. That means no access is granted.
                log.warn("Auth is enabled but current interceptor is null. Defaulting to no permissions.");
                allowedLevel = AuthHandler.Permissions.NONE;
            } else {
                allowedLevel = currentInterceptor.getHandler().authorize(resource, AuthInterceptor.principal());
            }
            return (allowedLevel.ordinal() < permission.ordinal()) ? false : true;
        } else {
            return true;
        }
    }

    public boolean isAuthorized(String resource, AuthHandler.Permissions permission) {
        return isAuthorized(resource, permission, AuthInterceptor.INTERCEPTOR_OBJECT.get());
    }

    public String checkAuthorization(String resource, AuthHandler.Permissions expectedLevel, AuthInterceptor interceptor) {
        if (isAuthorized(resource, expectedLevel, interceptor)) {
            return "";
        } else {
            throw new RuntimeException(new AuthorizationException("Access not allowed"));
        }
    }

    public String checkAuthorization(String resource, AuthHandler.Permissions expectedLevel) {
        return checkAuthorization(resource, expectedLevel, AuthInterceptor.INTERCEPTOR_OBJECT.get());
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
            return AuthInterceptor.retrieveMasterToken(tokenSigningKey);
        } else {
            return "";
        }
    }
}

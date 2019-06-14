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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import lombok.AllArgsConstructor;

/**
 * Helper class containing APIs related to delegation token and authorization/authentication.
 */
@AllArgsConstructor
public class AuthHelper {

    private final boolean isAuthEnabled;
    private final String tokenSigningKey;
    private final Optional<Integer> accessTokenTtlInSeconds;

    public AuthHelper(boolean isAuthEnabled, String tokenSigningKey, Integer tokenTtlInSeconds) {
        this(isAuthEnabled, tokenSigningKey, Optional.ofNullable(tokenTtlInSeconds));
    }

    @VisibleForTesting
    public static AuthHelper getDisabledAuthHelper() {
        return new AuthHelper(false, "", Optional.of(-1));
    }

    public boolean isAuthorized(String resource, AuthHandler.Permissions permission) {
        if (isAuthEnabled) {
            PravegaInterceptor currentInterceptor = PravegaInterceptor.INTERCEPTOR_OBJECT.get();

            AuthHandler.Permissions allowedLevel;
            if (currentInterceptor == null) {
                //No interceptor, and authorization is enabled. That means no access is granted.
                allowedLevel = AuthHandler.Permissions.NONE;
            } else {
                allowedLevel = currentInterceptor.authorize(resource);
            }
            return (allowedLevel.ordinal() < permission.ordinal()) ? false : true;
        } else {
            return true;
        }
    }

    public String checkAuthorization(String resource, AuthHandler.Permissions expectedLevel) {
        if (isAuthorized(resource, expectedLevel)) {
            return "";
        } else {
            throw new RuntimeException(new AuthorizationException("Access not allowed"));
        }
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

            return new JsonWebToken("segmentstoreresource",
                            "segmentstore",
                                    tokenSigningKey.getBytes(),
                                    this.accessTokenTtlInSeconds,
                                    claims)
                    .toCompactString();
        } else {
            return "";
        }
    }

    public String retrieveMasterToken() {
        if (isAuthEnabled) {
            return PravegaInterceptor.retrieveMasterToken(tokenSigningKey);
        } else {
            return "";
        }
    }
}

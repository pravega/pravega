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
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.pravega.auth.AuthHandler;
import io.pravega.auth.AuthorizationException;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;

/**
 * Helper class containing APIs related to delegation token and authorization/authentication.
 */
@AllArgsConstructor
public class AuthHelper {
    private final boolean isAuthEnabled;
    private final String tokenSigningKey;

    @VisibleForTesting
    public static AuthHelper getDisabledAuthHelper() {
        return new AuthHelper(false, "");
    }

    public String checkAuthorization(String resource, AuthHandler.Permissions expectedLevel) {
        if (isAuthEnabled) {
            PravegaInterceptor currentInterceptor = PravegaInterceptor.INTERCEPTOR_OBJECT.get();

            AuthHandler.Permissions allowedLevel;
            if (currentInterceptor == null) {
                //No interceptor, and authorization is enabled. Means no access is granted.
                allowedLevel = AuthHandler.Permissions.NONE;
            } else {
                allowedLevel = currentInterceptor.authorize(resource);
            }
            if (allowedLevel.ordinal() < expectedLevel.ordinal()) {
                throw new RuntimeException(new AuthorizationException("Access not allowed"));
            }
        }
        return "";
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

            return Jwts.builder()
                       .setSubject("segmentstoreresource")
                       .setAudience("segmentstore")
                       .setClaims(claims)
                       .signWith(SignatureAlgorithm.HS512, tokenSigningKey.getBytes())
                       .compact();
        }
        return "";
    }

    public String retrieveMasterToken() {
        if (isAuthEnabled) {
            return PravegaInterceptor.retrieveMasterToken(tokenSigningKey);
        } else {
            return "";
        }
    }
}

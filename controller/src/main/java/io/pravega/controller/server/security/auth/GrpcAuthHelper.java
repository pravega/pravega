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
package io.pravega.controller.server.security.auth;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.ServerBuilder;
import io.pravega.auth.AuthHandler;
import io.pravega.auth.AuthenticationException;
import io.pravega.auth.AuthorizationException;
import io.pravega.controller.server.security.auth.handler.AuthContext;
import io.pravega.controller.server.security.auth.handler.AuthInterceptor;
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
            if (authContext == null || authContext.getAuthHandler() == null) {
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
            log.trace("Successfully authorized principal {} for {} access to resource {}",
                    ctx == null ? null : ctx.getPrincipal(), expectedLevel, resource);
            return "";
        } else {
            if (ctx == null || ctx.getPrincipal() == null) {
                throw new AuthenticationException("Couldn't extract Principal");
            }
            String message = String.format("Principal [%s] not allowed [%s] access for resource [%s]",
                    ctx.getPrincipal(), expectedLevel, resource);
            throw new AuthorizationException(message);
        }
    }

    public String checkAuthorization(String resource, AuthHandler.Permissions expectedLevel) {
        return checkAuthorization(resource, expectedLevel, AuthContext.current());
    }

    public String checkAuthorizationAndCreateToken(String resource, AuthHandler.Permissions expectedLevel) {
        if (isAuthEnabled) {
            try {
                checkAuthorization(resource, expectedLevel);
            } catch (RuntimeException e) {
                // An auth handler plugin loaded in the system may throw this exception.
                log.warn("Authorization failed", e);
                throw e;
            }
            return createDelegationToken(resource, expectedLevel, tokenSigningKey);
        }
        return "";
    }

    public String createDelegationToken(String resource, AuthHandler.Permissions expectedLevel) {
        return createDelegationToken(resource, expectedLevel, this.tokenSigningKey);
    }

    private String createDelegationToken(String resource, AuthHandler.Permissions expectedLevel, String tokenSigningKey) {
        if (isAuthEnabled) {
            Map<String, Object> claims = new HashMap<>();
            claims.put(resource, String.valueOf(expectedLevel));

            return new JsonWebToken("segmentstoreresource", "segmentstore", tokenSigningKey.getBytes(),
                            claims, this.accessTokenTTLInSeconds).toCompactString();
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
                customClaims, null).toCompactString();
    }

    public static void registerInterceptors(Map<String, AuthHandler> handlers, ServerBuilder<?> builder) {
        for (Map.Entry<String, AuthHandler> handler : handlers.entrySet()) {
            builder.intercept(new AuthInterceptor(handler.getValue()));
        }
    }
}

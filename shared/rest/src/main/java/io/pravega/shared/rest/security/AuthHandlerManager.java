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
import com.google.common.collect.ImmutableMap;
import io.pravega.auth.AuthException;
import io.pravega.auth.AuthHandler;
import io.pravega.auth.AuthenticationException;
import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import javax.annotation.concurrent.GuardedBy;

import io.pravega.auth.ServerConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * Manages instances of {@link AuthHandler} for the Controller's gRPC and REST interfaces.
 *
 * In case of grpc, the routing of the authenticate function to specific registered interceptor is taken care by grpc
 * interceptor mechanism. In case of REST calls, this class routes the call to specific AuthHandler.
 */
@Slf4j
public class AuthHandlerManager {

    @GuardedBy("this")
    private final Map<String, AuthHandler> handlerMap;

    /**
     * If the {@link ServiceLoader} fails to load a {@link AuthHandler} class, any future authorization/authentication
     * attempts that rely on that {@link AuthHandler} being registered will fail with an {@link AuthenticationException}.
     * This maintains the previous behavior where the initialization was done via an explicit method call outside of the constructor.
     *
     * @param serverConfig The {@link ServerConfig} config object.
     */
    public AuthHandlerManager(ServerConfig serverConfig) {
        this.handlerMap = new HashMap<>();
        if (serverConfig != null && serverConfig.isAuthorizationEnabled()) {
            try {
                ServiceLoader<AuthHandler> loader = ServiceLoader.load(AuthHandler.class);
                for (AuthHandler handler : loader) {
                    try {
                        handler.initialize(serverConfig);
                        if (handlerMap.putIfAbsent(handler.getHandlerName(), handler) != null) {
                            log.warn("Handler with name {} already exists. Not replacing it with the latest handler");
                            continue;
                        }
                    } catch (Exception e) {
                        log.warn("Exception while initializing auth handler {}", handler, e);
                    }
                }
            } catch (Throwable e) {
                log.warn("Exception while loading the auth handlers", e);
            }
        }
    }

    public Map<String, AuthHandler> getHandlerMap() {
        return ImmutableMap.copyOf(handlerMap);
    }

    private AuthHandler getHandler(String handlerName) throws AuthenticationException {
        AuthHandler retVal;
        synchronized (this) {
            retVal = handlerMap.get(handlerName);
        }
        if (retVal == null) {
            throw new AuthenticationException("Handler does not exist for method " + handlerName);
        }
        return retVal;
    }

    /**
     * API to authenticate and authorize access to a given resource.
     * @param resource The resource identifier for which the access needs to be controlled.
     * @param credentials  Credentials used for authentication.
     * @param level    Expected level of access.
     * @return         Returns true if the entity represented by the custom auth headers had given level of access to the resource.
     *                 Returns false if the entity does not have access. 
     * @throws AuthenticationException if an authentication failure occurred.
     */
    public boolean authenticateAndAuthorize(String resource, String credentials, AuthHandler.Permissions level) throws AuthenticationException {
        Preconditions.checkNotNull(credentials, "credentials");
        boolean retVal = false;
        try {
            String[] parts = extractMethodAndToken(credentials);
            String method = parts[0];
            String token = parts[1];
            AuthHandler handler = getHandler(method);
            Preconditions.checkNotNull( handler, "Can not find handler.");
            Principal principal;
            if ((principal = handler.authenticate(token)) == null) {
                throw new AuthenticationException("Authentication failure");
            }
            retVal = handler.authorize(resource, principal).ordinal() >= level.ordinal();
        } catch (AuthException e) {
            throw new AuthenticationException("Authentication failure");
        }
        return retVal;
    }

    /**
     *
     * API to authenticate a given credential.
     * @param credentials  Credentials used for authentication.
     *
     * @return Returns the Principal if the entity represented by credentials is authenticated.
     * @throws AuthException if an authentication failure occurred.
     */
    public Principal authenticate(String credentials) throws AuthException {
        Preconditions.checkNotNull(credentials, "credentials");
        String[] parts = extractMethodAndToken(credentials);
        String method = parts[0];
        String token = parts[1];
        AuthHandler handler = getHandler(method);
        Preconditions.checkNotNull( handler, "Can not find handler.");
        return handler.authenticate(token);
    }

    private String[] extractMethodAndToken(String credentials) throws AuthenticationException {
        String[] parts = credentials.split("\\s+", 2);
        if (parts.length != 2) {
            throw new AuthenticationException("Malformed request");
        }
        return parts;
    }

    /**
     *
     * API to authorize a given principal and credential.
     *
     * @param resource The resource identifier for which the access needs to be controlled.
     * @param credentials Credentials used for authentication.
     * @param level Expected level of access.
     * @param principal Principal associated with the credentials.
     *
     * @return Returns true if the entity represented by the credentials has given level of access to the resource.
     *      Returns false if the entity does not have access.
     * @throws AuthException if an authentication or authorization failure occurred.
     */
    public boolean authorize(String resource, Principal principal, String credentials, AuthHandler.Permissions level)
            throws AuthException {
        Preconditions.checkNotNull(credentials, "credentials");

        String method = extractMethodAndToken(credentials)[0];
        AuthHandler handler = getHandler(method);
        Preconditions.checkNotNull( handler, "Can not find handler.");

        return handler.authorize(resource, principal).ordinal() >= level.ordinal();
    }


    /**
     * This method is not only visible for testing, but also intended to be used solely for testing. It allows tests
     * to inject and register custom auth handlers. Also, this method is idempotent.
     *
     * @param authHandler the {@code AuthHandler} implementation to register
     * @throws NullPointerException {@code authHandler} is null
     */
    @VisibleForTesting
    public synchronized void registerHandler(AuthHandler authHandler) {
        Preconditions.checkNotNull(authHandler, "authHandler");
        this.handlerMap.put(authHandler.getHandlerName(), authHandler);
    }
}

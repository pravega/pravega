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
package io.pravega.auth;

import java.security.Principal;

/**
 * Custom authorization/authentication handlers implement this interface.
 * The implementations are loaded from the classpath using `ServiceLoader` (https://docs.oracle.com/javase/7/docs/api/java/util/ServiceLoader.html)
 * Pravega controller also implements this interface through {@code io.pravega.controller.server.rpc.auth.PasswordAuthHandler}.
 *
 * Each custom auth handler is registered with a unique name identifying a supported authentication scheme.
 *
 * The client supplies authentication credentials formatted as per HTTP 1.1 (RFC 7235):
 * <pre>
 *     Authentication: &lt;scheme&gt; &lt;token&gt;
 * </pre>
 * This is done by implementing `PravegaCredentials` interface and passing it to client calls.
 *
 * The credentials are passed via the {@code Authorization} header.  For gRPC, the header is passed via call metadata.
 * For REST, the credentials are passed as the value of the HTTP {@code Authorization} header.
 * For gRPC, the credentials are passed as the value of the {@code Authorization} header in call metadata.
 *
 */
public interface AuthHandler {

    enum Permissions {
        NONE,
        READ,
        READ_UPDATE
    }

    /**
     * Returns name of the handler. Only the first implementation with a unique name will be loaded.
     * @return The unique name assigned to the handler.
     */
    String getHandlerName();

    /**
     * Authenticates a given request. Pravega controller passes the HTTP headers associated with the call.
     * The custom implementation returns whether the user represented by these headers is authenticated.
     *
     * @param token the credentials token passed via the {@code Authorization} header.
     * @return Returns the Principal represented by the token.
     *
     * @throws AuthException Exception of type AuthException thrown if there is any error.
     */
    Principal authenticate(String token) throws AuthException;

    /**
     * Authorizes the access to a given resource. Pravega controller passes the HTTP headers associated with the call.
     * The implementations of this interface should return the maximum level of authorization possible for the user represented
     * by the headers.
     *
     * @param resource the resource that needs to be accessed.
     * @param principal the Principal which needs to be authorized. This is generally a Principal returned by an earlier
     *                  call to `authenticate` method.
     * @return The level of authorization.
     */
    Permissions authorize(String resource, Principal principal);

    /**
     * Sets the configuration. If the auth handler needs to access the server configuration, it can be accessed though this var.
     *
     * @param serverConfig The server configuration.
     */
    default void initialize(ServerConfig serverConfig) {
        // Do nothing if the handler does not need any configuration being injected.
    }
}

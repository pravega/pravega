/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.auth;

import java.util.Map;

/**
 * Custom authorization/authentication handlers implement this interface.
 * The implementations are loaded from the classpath using `ServiceLoader` (https://docs.oracle.com/javase/7/docs/api/java/util/ServiceLoader.html)
 * Pravega controller also implements this interface through {@link #PasswordAuthHandler}.
 *
 * Each custom auth handler is registered with a unique name.
 * A client selects its auth handler by setting a grpc header with a name "method". T
 * his is done by implementing `PravegaCredentials` interface and passing it to client calls.
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
     * @param headers the key-value pairs passed through grpc.
     * @return Returns true when the user is authenticated.
     */
    boolean authenticate(Map<String, String> headers);

    /**
     * Authorizes the access to a given resource. Pravega controller passes the HTTP headers associated with the call.
     * The implementations of this interface should return the maximum level of authorization possible for the user represented
     * by the headers.
     *
     * @param resource the resource that needs to be accessed.
     * @param headers the context for authorization.
     * @return The level of authorization.
     */
    Permissions authorize(String resource, Map<String, String> headers);

    /**
     * Sets the configuration. If the auth handler needs to access the server configuration, it can be accessed though this var.
     *
     * @param serverConfig The server configuration.
     */
    void initialize(ServerConfig serverConfig);
}

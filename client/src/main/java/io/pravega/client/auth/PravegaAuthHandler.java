/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.auth;

import java.util.Map;

/**
 * Custom authorization/authentication handlers implment this interface.
 * The implementations are loaded from the classpath using `ServiceLoader` (https://docs.oracle.com/javase/7/docs/api/java/util/ServiceLoader.html)
 * Pravega controller also implements this interface through `PravegaDefaultAuthHandler`.
 *
 * Each custom auth handler is registered with a unique name.
 * A client selects its auth handler by setting a grpc header with a name "method". T
 * his is done by implementing `PravegaCredentials` interface and passing it to client calls.
 *
 */
public interface PravegaAuthHandler {

    enum  PravegaAccessControlEnum {
        NONE,
        READ,
        READ_UPDATE
    }

    /**
     * Returns name of the handler.
     * @return The unique name assigned to the handler.
     */
    String getHandlerName();

    /**
     * Authenticates a given request.
     *
     * @param headers the key-value pairs passed through grpc.
     * @return Returns true when the user is authenticated.
     */
    boolean authenticate(Map<String, String> headers);

    /**
     * Authorizes the access to a given resources.
     * @param resource the resource that needs to be accessed.
     * @param headers the context for authorization.
     * @return The level of authorization. Throws exception if not authorized.
     */
    PravegaAccessControlEnum authorize(String resource, Map<String, String> headers);

    /**
     * Sets the configuration. The auth handler can extract its config from this.
     * @param serverConfig The server configuration.
     */
    void setServerConfig(Object serverConfig);
}

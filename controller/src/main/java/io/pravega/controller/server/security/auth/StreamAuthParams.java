/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.security.auth;

import io.pravega.auth.AuthHandler;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * A helper class used for processing auth params for streams.
 */
@Slf4j
public class StreamAuthParams {

    private static final AuthorizationResource AUTH_RESOURCE = new AuthorizationResourceImpl();

    private final String scope;
    private final String stream;
    private final boolean isInternalWritesWithReadPermEnabled;
    private final boolean isInternalStream;

    public StreamAuthParams(@NonNull String scope, @NonNull String stream, boolean isInternalWritesWithReadPermEnabled) {
        this.scope = scope;
        this.stream = stream;
        this.isInternalWritesWithReadPermEnabled = isInternalWritesWithReadPermEnabled;
        if (stream.startsWith("_")) {
            this.isInternalStream = true;
        } else {
            this.isInternalStream = false;
        }
    }

    public AuthHandler.Permissions requiredPermissionsForReads() {
        return AuthHandler.Permissions.READ;
    }

    public AuthHandler.Permissions requiredPermissionForWrites() {
        if (this.isInternalStream) {
            if (this.isInternalWritesWithReadPermEnabled) {
                return AuthHandler.Permissions.READ;
            } else {
                return AuthHandler.Permissions.READ_UPDATE;
            }
        } else {
            return AuthHandler.Permissions.READ_UPDATE;
        }
    }

    public static String toResourceString(String scope, String stream) {
        final String resource;
        if (stream.startsWith("_")) {
            resource = AUTH_RESOURCE.ofInternalStream(scope, stream);
        } else {
            resource = AUTH_RESOURCE.ofStreamInScope(scope, stream);
        }
        return resource;
    }
}

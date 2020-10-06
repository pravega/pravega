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

/**
 * A helper class used for process auth params for internal/state synchronizer streams
 * (like, `testScope/_RGtestReaderGroup`).
 */
public class InternalStreamAuthParams {

    private final String scope;
    private final String stream;
    private final boolean isInternalWritesWithReadPermEnabled;

    public InternalStreamAuthParams(@NonNull String scope, @NonNull String stream, boolean isInternalWritesWithReadPermEnabled) {
        if (!stream.startsWith("_")) {
            throw new IllegalArgumentException("Not an internal stream");
        }
        this.scope = scope;
        this.stream = stream;
        this.isInternalWritesWithReadPermEnabled = isInternalWritesWithReadPermEnabled;
    }

    public AuthHandler.Permissions requiredPermissionForWrites() {
        if (this.isInternalWritesWithReadPermEnabled) {
            return AuthHandler.Permissions.READ;
        } else {
            return AuthHandler.Permissions.READ_UPDATE;
        }
    }

    public String streamResource() {
        return null;
    }
}

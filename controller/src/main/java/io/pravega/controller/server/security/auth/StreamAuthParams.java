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

import com.google.common.annotations.VisibleForTesting;
import io.pravega.auth.AuthHandler;
import lombok.Getter;
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

    @VisibleForTesting
    @Getter
    private final boolean isInternalStream;

    @VisibleForTesting
    StreamAuthParams(@NonNull String scope, @NonNull String stream) {
        this(scope, stream, true);
    }

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

    public String resourceString() {
        return toResourceString(this.scope, this.stream);
    }

    public String streamResourceString() {
        return AUTH_RESOURCE.ofStreamInScope(scope, stream);
    }

    private static String toResourceString(String scope, String stream, boolean isStreamInternal) {
        return isStreamInternal ? AUTH_RESOURCE.ofInternalStream(scope, stream) :
                AUTH_RESOURCE.ofStreamInScope(scope, stream);
    }

    public static String toResourceString(String scope, String stream) {
        return toResourceString(scope, stream, stream.startsWith("_"));
    }

    public boolean isStreamUserDefined() {
        return !isInternalStream;
    }
}

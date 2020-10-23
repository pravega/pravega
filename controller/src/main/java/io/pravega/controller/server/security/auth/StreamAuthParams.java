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
import io.pravega.shared.NameUtils;
import io.pravega.shared.security.auth.PermissionsHelper;
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
    private final String requestedPermission;
    private final boolean isInternalWritesWithReadPermEnabled;

    @VisibleForTesting
    @Getter
    private final boolean isInternalStream;

    @VisibleForTesting
    StreamAuthParams(@NonNull String scope, @NonNull String stream) {
        this(scope, stream, "READ", true);
    }

    public StreamAuthParams(@NonNull String scope, @NonNull String stream, boolean isInternalWritesWithReadPermEnabled) {
        this(scope, stream, "", isInternalWritesWithReadPermEnabled);
    }

    public StreamAuthParams(@NonNull String scope, @NonNull String stream, @NonNull String requestedPermission,
                            boolean isInternalWritesWithReadPermEnabled) {
        this.scope = scope;
        this.stream = stream;
        this.isInternalWritesWithReadPermEnabled = isInternalWritesWithReadPermEnabled;
        this.requestedPermission = requestedPermission;
        this.isInternalStream = stream.startsWith(NameUtils.INTERNAL_NAME_PREFIX) ? true : false;
    }

    public AuthHandler.Permissions requestedPermission() {
        return PermissionsHelper.parse(requestedPermission, AuthHandler.Permissions.READ);
    }

    private AuthHandler.Permissions requestedPermission(AuthHandler.Permissions defaultValue) {
        return PermissionsHelper.parse(requestedPermission, defaultValue);
    }

    public AuthHandler.Permissions requiredPermissionForWrites() {
        if (this.isStreamUserDefined()) {
            return AuthHandler.Permissions.READ_UPDATE;
        } else {
            if (this.isInternalWritesWithReadPermEnabled) {
                return AuthHandler.Permissions.READ;
            } else {
                if (stream.startsWith(NameUtils.getMARK_PREFIX())) {
                    return this.requestedPermission(AuthHandler.Permissions.READ_UPDATE);
                }
                return AuthHandler.Permissions.READ_UPDATE;
            }
        }
    }

    public String resourceString() {
        return toResourceString(this.scope, this.stream);
    }

    public boolean isRequestedPermissionEmpty() {
        return this.requestedPermission.equals("");
    }

    public String streamResourceString() {
        return AUTH_RESOURCE.ofStreamInScope(scope, stream);
    }

    private static String toResourceString(String scope, String stream, boolean isStreamInternal) {
        return isStreamInternal ? AUTH_RESOURCE.ofInternalStream(scope, stream) :
                AUTH_RESOURCE.ofStreamInScope(scope, stream);
    }

    public static String toResourceString(String scope, String stream) {
        return toResourceString(scope, stream, stream.startsWith(NameUtils.INTERNAL_NAME_PREFIX));
    }

    public boolean isStreamUserDefined() {
        return !isInternalStream;
    }
}

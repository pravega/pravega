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
import io.pravega.auth.AuthHandler;
import io.pravega.shared.NameUtils;
import io.pravega.shared.security.auth.AccessOperation;
import io.pravega.shared.security.auth.AuthorizationResource;
import io.pravega.shared.security.auth.AuthorizationResourceImpl;
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

    @Getter
    private final String scope;
    @Getter
    private final String stream;
    private final AccessOperation accessOperation;
    private final boolean isRGWritesWithReadPermEnabled;
    @Getter
    private final boolean isMarkStream;

    @VisibleForTesting
    @Getter
    private final boolean isInternalStream;

    @VisibleForTesting
    StreamAuthParams(@NonNull String scope, @NonNull String stream) {
        this(scope, stream, AccessOperation.READ, true);
    }

    public StreamAuthParams(@NonNull String scope, @NonNull String stream, boolean isRGWritesWithReadPermEnabled) {
        this(scope, stream, AccessOperation.UNSPECIFIED, isRGWritesWithReadPermEnabled);
    }

    public StreamAuthParams(@NonNull String scope, @NonNull String stream, @NonNull AccessOperation accessOperation,
                            boolean isRGWritesWithReadPermEnabled) {
        this.scope = scope;
        this.stream = stream;
        this.isRGWritesWithReadPermEnabled = isRGWritesWithReadPermEnabled;
        this.accessOperation = accessOperation;
        this.isInternalStream = stream.startsWith(NameUtils.INTERNAL_NAME_PREFIX) ? true : false;
        this.isMarkStream = stream.startsWith(NameUtils.getMARK_PREFIX());
    }

    public AuthHandler.Permissions requestedPermission() {
        return PermissionsHelper.parse(accessOperation, AuthHandler.Permissions.READ);
    }

    public AuthHandler.Permissions requiredPermissionForWrites() {
        if (this.isStreamUserDefined()) {
            return AuthHandler.Permissions.READ_UPDATE;
        } else {
            if (this.isRGWritesWithReadPermEnabled && this.stream.startsWith(NameUtils.READER_GROUP_STREAM_PREFIX)) {
                return AuthHandler.Permissions.READ;
            } else {
                if (isMarkStream()) {
                    return AuthHandler.Permissions.READ;
                }
                return AuthHandler.Permissions.READ_UPDATE;
            }
        }
    }

    /**
     * For external streams, returns the stream resource representation (e.g., prn::/scope:testScope/stream:testStream.
     * For internal streams, returns a suitable resource representation for authorization.
     *    - E.g., for a scope/stream testScope/_RGmyapp - prn::/scope:testScope/reader-group:myApp
     *    - E.g., for a scope/stream testScope/_MARKtestStream - prn::/scope:testScope/stream:testStream
     *    - E.g., for a scope/stream testScope/_internalStream - prn::/scope:testScope/stream:_internalStream
     *
     * @return a resource string suitable for authorization
     */
    public String resourceString() {
        return toResourceString(this.scope, this.stream);
    }

    public boolean isAccessOperationUnspecified() {
        return this.accessOperation.equals(AccessOperation.UNSPECIFIED);
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

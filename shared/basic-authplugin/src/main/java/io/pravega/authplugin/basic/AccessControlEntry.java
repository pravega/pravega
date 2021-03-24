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
package io.pravega.authplugin.basic;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.pravega.auth.AuthHandler;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;

import java.util.regex.Pattern;

/**
 * An entry of an {@link AccessControlList}.
 */
class AccessControlEntry {
    private static final Pattern PATTERN_STAR_NOT_PRECEDED_BY_DOT = Pattern.compile("(?!=.)\\*");

    @Getter(AccessLevel.PACKAGE)
    private final String resourcePattern;

    @Getter(AccessLevel.PACKAGE)
    private final AuthHandler.Permissions permissions;

    @VisibleForTesting
    AccessControlEntry(@NonNull String aceResource, @NonNull AuthHandler.Permissions permissions) {
        this(aceResource, permissions, false);
    }

    @VisibleForTesting
    AccessControlEntry(@NonNull String aceResource, @NonNull AuthHandler.Permissions permissions, boolean isLegacyFormat) {
        // Replaces any `*` with `.*`, if it's not already preceded by `.`, for regex processing.
        // So, `prn::/*` becomes `prn::/.*` and `prn::/scope:*` becomes `prn::/scope:.*`
        this.resourcePattern = isLegacyFormat ?  aceResource :
                PATTERN_STAR_NOT_PRECEDED_BY_DOT.matcher(aceResource).replaceAll(".*");
        this.permissions = permissions;
    }

    static AccessControlEntry fromString(String ace) {
        String[] splits = null;
        if (Strings.isNullOrEmpty(ace) || (splits = ace.split(",")).length != 2) {
            return null;
        }
        String resource = null;
        if (Strings.isNullOrEmpty(splits[0]) || Strings.isNullOrEmpty(splits[1])) {
            return null;
        }
        resource = splits[0].trim();
        AuthHandler.Permissions permissions;
        try {
            permissions = AuthHandler.Permissions.valueOf(splits[1].trim());
        } catch (IllegalArgumentException e) {
            return null;
        }
        return new AccessControlEntry(resource, permissions, false);
    }

    boolean isResource(String resource) {
        return resourcePattern.equals(resource);
    }

    boolean resourceEndsWith(String resource) {
        return resourcePattern.endsWith(resource);
    }

    boolean resourceStartsWith(String resource) {
        return resourcePattern.startsWith(resource);
    }

    boolean hasHigherPermissionsThan(AuthHandler.Permissions input) {
        return this.permissions.ordinal() > input.ordinal();
    }
}

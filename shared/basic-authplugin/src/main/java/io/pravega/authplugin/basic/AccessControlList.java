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

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.List;

/**
 * Represents an access control list (ACL). An ACL specifies the permissions that a user has, for a set of resources.
 */
@RequiredArgsConstructor
class AccessControlList {
    @Getter(AccessLevel.PACKAGE)
    private final String encryptedPassword;

    @Getter(AccessLevel.PACKAGE)
    private final List<AccessControlEntry> entries;
}

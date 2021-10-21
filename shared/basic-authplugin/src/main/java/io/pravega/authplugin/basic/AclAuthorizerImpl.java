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

import io.pravega.auth.AuthHandler;
import lombok.NonNull;

class AclAuthorizerImpl extends AclAuthorizer {

    @Override
    public AuthHandler.Permissions authorize(@NonNull AccessControlList accessControlList, @NonNull String resource) {
        AuthHandler.Permissions result = AuthHandler.Permissions.NONE;
        String resourceDomain = resource.split("::")[0];
        for (AccessControlEntry accessControlEntry : accessControlList.getEntries()) {
            // You could have a null ACE in the ACL if you had a malformed entry such as `prn::/scope:readresource`
            // having no permissions set.
            if (accessControlEntry != null && accessControlEntry.resourceStartsWith(resourceDomain)) {
                if (resource.matches(accessControlEntry.getResourcePattern())) {
                    result = accessControlEntry.getPermissions();
                    break;
                }
            }
        }
        return result;
    }
}

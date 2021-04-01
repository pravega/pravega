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
package io.pravega.client.security.auth;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
@EqualsAndHashCode
class DelegationToken {

    /**
     * Represents the delegation token JWT compact string.
     *
     * The delegation token can take these values:
     *   1) null: the value was not set at construction time, so it'll be obtaining lazily here.
     *   2) ""  : an empty value used by callers when auth is disabled.
     *   3) a non-empty JWT compact string of the format
     *       <base64-encoded-header-json>.<base64-encoded-body-json>.<base64-encoded-signature>
     */
    private final String value;

    /**
     * Caches the expiration time of the current token, so that the token does not need to be parsed upon every retrieval.
     */
    private final Long expiryTime;

}

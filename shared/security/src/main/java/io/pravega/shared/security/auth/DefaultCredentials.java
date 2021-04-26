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
package io.pravega.shared.security.auth;

import io.pravega.auth.AuthConstants;
import lombok.EqualsAndHashCode;
import lombok.NonNull;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

@EqualsAndHashCode
public class DefaultCredentials implements Credentials {

    private static final long serialVersionUID = 1L;

    private final String token;

    /**
     * Creates a new object containing a token that is valid for Basic authentication scheme. The object encapsulates a
     * token value comprising of a Base64 encoded value of "<username>:<password>".
     *
     * @param password the password
     * @param userName the user name
     */
    public DefaultCredentials(@NonNull String password, @NonNull String userName) {
        String decoded = userName + ":" + password;
        this.token = Base64.getEncoder().encodeToString(decoded.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public String getAuthenticationType() {
        return AuthConstants.BASIC;
    }

    @Override
    public String getAuthenticationToken() {
        return token;
    }
}

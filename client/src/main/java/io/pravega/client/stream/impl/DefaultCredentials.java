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
package io.pravega.client.stream.impl;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.EqualsAndHashCode;

/**
 * Username/password credentials for basic authentication.
 *
 * @deprecated As of Pravega release 0.9, replaced by {@link io.pravega.shared.security.auth.DefaultCredentials}.
 */
@Deprecated
@SuppressFBWarnings(value = "NM_SAME_SIMPLE_NAME_AS_INTERFACE",
        justification = "Interface with same name retained for compatibility with older implementations")
@EqualsAndHashCode
public class DefaultCredentials implements Credentials {

    private static final long serialVersionUID = 1L;

    @EqualsAndHashCode.Exclude
    private final io.pravega.shared.security.auth.DefaultCredentials delegate;

    private final String token;

    public DefaultCredentials(String password, String userName) {
        delegate = new io.pravega.shared.security.auth.DefaultCredentials(password, userName);
        token = delegate.getAuthenticationToken();
    }

    @Override
    public String getAuthenticationType() {
        return delegate.getAuthenticationType();
    }

    @Override
    public String getAuthenticationToken() {
        return token;
    }
}

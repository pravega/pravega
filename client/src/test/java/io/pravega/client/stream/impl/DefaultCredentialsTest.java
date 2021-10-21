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

import io.pravega.auth.AuthConstants;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DefaultCredentialsTest {

    @SuppressWarnings("deprecation")
    @Test
    public void testObjectIsAssignableToBothInterfaces() {
        io.pravega.shared.security.auth.Credentials credentials = new DefaultCredentials("pwd", "user");
        Credentials legacyCredentials = new DefaultCredentials("pwd", "username");
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testLegacyObjectDelegatesToNewObject() {
        io.pravega.shared.security.auth.Credentials credentials =
                new io.pravega.client.stream.impl.DefaultCredentials("pwd", "user");
        assertEquals(new io.pravega.shared.security.auth.DefaultCredentials("pwd", "user").getAuthenticationToken(),
                credentials.getAuthenticationToken());
        assertEquals(AuthConstants.BASIC, credentials.getAuthenticationType());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testEqualsAndHashCode() {
        io.pravega.shared.security.auth.Credentials credentials1 = new DefaultCredentials("pwd", "user");
        io.pravega.shared.security.auth.Credentials credentials2 = new DefaultCredentials("pwd", "user");
        assertEquals(credentials1, credentials2);
        assertEquals(credentials1.hashCode(), credentials2.hashCode());
    }
}

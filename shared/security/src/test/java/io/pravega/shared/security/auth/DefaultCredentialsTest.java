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

import io.pravega.test.common.AssertExtensions;
import org.junit.Test;

import java.util.Base64;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DefaultCredentialsTest {

    @Test
    public void ctorThrowsExceptionIfInputIsNull() {
        AssertExtensions.assertThrows(NullPointerException.class, () -> new DefaultCredentials(null, "pwd"));
        AssertExtensions.assertThrows(NullPointerException.class, () -> new DefaultCredentials("username", null));
    }

    @Test
    public void ctorCreatesValidObjectForValidInput() {
        String userName = "username";
        String password = "pwd";
        Credentials credentials = new DefaultCredentials(password, userName);
        assertEquals("Basic", credentials.getAuthenticationType());

        String token = credentials.getAuthenticationToken();
        assertNotNull(token);

        String credentialsString = new String(Base64.getDecoder().decode(token));
        assertEquals(userName + ":" + password, credentialsString);
    }

    @Test
    public void testEqualsAndHashCode() {
        Credentials credentials1 = new DefaultCredentials("pwd", "user");
        Credentials credentials2 = new DefaultCredentials("pwd", "user");
        assertEquals(credentials1, credentials2);
        assertEquals(credentials1.hashCode(), credentials2.hashCode());
    }
}

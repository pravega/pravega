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
package io.pravega.auth;

import java.security.Principal;

public class TestAuthHandler implements AuthHandler {

    public static final String DUMMY_USER = "dummy";
    public static final String ADMIN_USER = "admin";

    @Override
    public String getHandlerName() {
        return "testHandler";
    }

    @Override
    public Principal authenticate(String token) {
        return new MockPrincipal(token);
    }

    @Override
    public Permissions authorize(String resource, Principal principal) {
        if (principal.getName().contains(DUMMY_USER)) {
            return Permissions.NONE;
        } else {
            return Permissions.READ_UPDATE;
        }
    }

    public static String testAuthToken(String userName) {
        return String.format("testHandler %s", userName);
    }
}

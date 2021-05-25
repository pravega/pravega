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
package io.pravega.test.integration.auth.customplugin;

import io.pravega.auth.AuthException;
import io.pravega.auth.AuthHandler;
import io.pravega.auth.AuthenticationException;
import lombok.extern.slf4j.Slf4j;

import java.security.Principal;

@Slf4j
public class TestAuthHandler implements AuthHandler {

    public static final String METHOD = "CustomMethod";

    public static final String TOKEN = "static-token";

    @Override
    public String getHandlerName() {
        return METHOD;
    }

    @Override
    public Principal authenticate(String token) throws AuthException {
        log.debug("Authenticating using token [{}]", token);
        if (token.equals(TOKEN)) {
            Principal result = new TestPrincipal(TOKEN);
            log.debug("Returning principal [{}] after successful authentication", result);
            return result;
        } else {
            throw new AuthenticationException("Specified token was invalid");
        }
    }

    @Override
    public Permissions authorize(String resource, Principal principal) {
        log.debug("Authorizing resource [{}] for principal [{}]", resource, principal);
        Permissions result;
        if (principal.getName().equals(TOKEN)) {
            result = Permissions.READ_UPDATE;
        } else {
            result = Permissions.NONE;
        }
        log.debug("Authorization result for resource [{}] for principal [{}] is permissions [{}]",
                resource, principal, result.name());
        return result;
    }
}

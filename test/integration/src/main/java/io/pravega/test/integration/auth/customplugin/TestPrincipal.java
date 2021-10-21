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

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.security.Principal;

@Slf4j
@ToString
public class TestPrincipal implements Principal {

    private String token;

    TestPrincipal(String token) {
        if (token == null || token.trim().isEmpty()) {
            log.warn("Token is blank");
            throw new IllegalArgumentException("token is blank");
        }
        this.token = token;
    }

    @Override
    public String getName() {
        return token;
    }
}
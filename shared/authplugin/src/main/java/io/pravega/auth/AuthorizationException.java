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

/**
 * Exception thrown when there is any error during authorization.
 */
public class AuthorizationException extends AuthException {
    private static final long serialVersionUID = 1L;
    public AuthorizationException(String message) {
        this(message, 401);
    }

    public AuthorizationException(String message, int responseCode) {
        super(message, responseCode);
    }

    public AuthorizationException(Exception e) {
        this(e, 403);
    }

    public AuthorizationException(Exception e, int responseCode) {
        super(e, responseCode);
    }
}

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
package io.pravega.segmentstore.server.host.delegationtoken;

import io.pravega.auth.AuthHandler;
import io.pravega.auth.InvalidClaimException;
import io.pravega.auth.InvalidTokenException;
import io.pravega.auth.TokenException;
import io.pravega.auth.TokenExpiredException;
import io.pravega.shared.security.token.JsonWebToken;

/**
 * This interface represents the code on segment store side that verifies the delegation token.
 */
public interface DelegationTokenVerifier {

    /**
     * Ensures that the given {@code token} represents specified {@code expectedLevel} of access on the
     * given {@code resource}. It returns normally if the {@code token} grants {@code expectedLevel} of access,
     * and throws an exception, otherwise.
     *
     * @param resource                the resource for which access is desired.
     * @param token                   the access/delegation token.
     * @param expectedLevel           maximum expected access to the given {@code resource}.
     * @throws TokenExpiredException  if the {@code token} has expired
     * @throws InvalidTokenException  if the {@code token} is invalid
     * @throws InvalidClaimException  if the {@code token} does not contain the claim representing
     *                                {@code expectedLevel} of access
     * @throws TokenException         if any other failure condition is encountered
     * @return JsonWebToken           a non-null value if token was parsed and verified successfully, otherwise null.
     */
    JsonWebToken verifyToken(String resource, String token, AuthHandler.Permissions expectedLevel)
            throws TokenExpiredException, InvalidTokenException, InvalidClaimException, TokenException;
}
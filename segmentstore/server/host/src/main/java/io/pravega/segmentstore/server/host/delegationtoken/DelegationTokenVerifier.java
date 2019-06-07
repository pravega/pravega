/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.delegationtoken;

import io.pravega.auth.AuthHandler;
import io.pravega.auth.InvalidClaimException;
import io.pravega.auth.InvalidTokenException;
import io.pravega.auth.TokenException;
import io.pravega.auth.TokenExpiredException;

/**
 * This interface represents the code on segment store side that verifies the delegation token.
 */
public interface DelegationTokenVerifier {

    /**
     * Determines whether the given {@code token} is valid for the specified {@code expectedLevel} of access on the
     * given {@code resource}.
     *
     * @param resource       the resource for which access is desired.
     * @param token          the access/delegation token.
     * @param expectedLevel  maximum expected access to the given {@code resource}.
     * @return               {@code true} if token is valid for accessing the resource, otherwise {@code false}.
     */
    boolean isTokenValid(String resource, String token, AuthHandler.Permissions expectedLevel);


    /**
     * Ensures that the given {@code token} represents specified {@code expectedLevel} of access on the
     * given {@code resource}. It returns normally if the token grants the access, and throws exceptions otherwise.
     *
     * @param resource                the resource for which access is desired.
     * @param token                   the access/delegation token.
     * @param expectedLevel           maximum expected access to the given {@code resource}.
     * @throws TokenExpiredException  if {@code token} has expired
     * @throws InvalidTokenException  if {@code token} is invalid
     * @throws InvalidClaimException  if {@code token} does not contain the claim representing
     *                                {@code expectedLevel} of access
     * @throws TokenException         in any other failure scenarios.
     */
    void verifyToken(String resource, String token, AuthHandler.Permissions expectedLevel)
            throws TokenExpiredException, InvalidTokenException, InvalidClaimException, TokenException;
}

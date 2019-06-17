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

import com.google.common.annotations.VisibleForTesting;
import io.pravega.auth.AuthHandler;
import io.pravega.auth.InvalidClaimException;
import io.pravega.auth.InvalidTokenException;
import io.pravega.auth.TokenException;
import io.pravega.auth.TokenExpiredException;
import io.pravega.common.Exceptions;
import io.pravega.segmentstore.server.host.stat.AutoScalerConfig;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.pravega.shared.security.token.JsonWebToken;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TokenVerifierImpl implements DelegationTokenVerifier {

    private final boolean isAuthEnabled;
    private final byte[] tokenSigningKey;

    public TokenVerifierImpl(AutoScalerConfig config) {
        this(config.isAuthEnabled(), config.getTokenSigningKey());
    }

    @VisibleForTesting
    public TokenVerifierImpl(boolean isAuthEnabled, String tokenSigningKeyBasis) {
        this.isAuthEnabled = isAuthEnabled;
        if (isAuthEnabled) {
            Exceptions.checkNotNullOrEmpty(tokenSigningKeyBasis, "tokenSigningKeyBasis");
            this.tokenSigningKey = tokenSigningKeyBasis.getBytes();
        } else {
            tokenSigningKey = null;
        }
    }

    @Override
    public void verifyToken(String resource, String token, AuthHandler.Permissions expectedLevel)
            throws TokenExpiredException, InvalidTokenException, InvalidClaimException, TokenException {
        if (isAuthEnabled) {

            // All key value pairs inside the payload are returned, including standard fields such as sub (for subject),
            // aud (for audience), iat, exp, as well as custom fields of the form "<resource> -> <permission>" set by
            // Pravega.
            Set<Map.Entry<String, Object>> claims = JsonWebToken.fetchClaims(token, tokenSigningKey);

            if (claims == null) {
                throw new InvalidTokenException("Token has no claims.");
            }

            Optional<Map.Entry<String, Object>> matchingClaim = claims.stream()
                    .filter(entry -> resourceMatchesClaimKey(entry.getKey(), resource) &&
                                     expectedLevel.compareTo(AuthHandler.Permissions.valueOf(entry.getValue().toString())) <= 0)
                    .findFirst();

            if (!matchingClaim.isPresent()) {
                log.trace(String.format("No matching claim found for resource [%s] and permission [%s] in token [%s].",
                        resource, expectedLevel, token));

                throw new InvalidClaimException(String.format(
                        "No matching claim found for resource: [%s] and permission: [%s] in the delegation token.",
                        resource, expectedLevel));

            }
        }
    }

    /**
     * Returns whether the specified resource} string matches the given claim key.
     *
     * @param claimKey
     * @param resource
     * @return
     */
    private boolean resourceMatchesClaimKey(String claimKey, String resource) {
        /*
         * Examples of the conditions when the claimKey (key of the key-value pair claim) matches the resource are:
         *      1) claimKey = "myscope", resource = "myscope"
         *      2) claimKey = "abc/", resource = "abc/xyx"
         *      3) claimKey = "_system/_requeststream", resource = "_system/_requeststream/0.#epoch.0"
         *      4) claimKey = "*" (the wildcard character)
         */
        return resource.equals(claimKey) // example 1
               || claimKey.endsWith("/") && resource.startsWith(claimKey) // example 2
               || resource.startsWith(claimKey + "/") // example 3
               || claimKey.equals("*"); // 4
    }
}

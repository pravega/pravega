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
import io.pravega.segmentstore.server.host.stat.AutoScalerConfig;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.pravega.shared.security.token.JsonWebToken;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TokenVerifierImpl implements DelegationTokenVerifier {
    private final AutoScalerConfig config;

    public TokenVerifierImpl(AutoScalerConfig config) {
        this.config = config;
    }

    @Override
    public boolean isTokenValid(String resource, String token, AuthHandler.Permissions expectedLevel) {
        try {
            verifyToken(resource, token, expectedLevel);
            return true;
        } catch (TokenException e) {
            log.warn(e.getMessage(), e);
            return false;
        }
    }

    @Override
    public void verifyToken(String resource, String token, AuthHandler.Permissions expectedLevel)
            throws TokenExpiredException, InvalidTokenException, InvalidClaimException, TokenException {
        if (config.isAuthEnabled()) {

            // All key value pairs inside the payload are returned, including standard fields such as sub (for subject),
            // aud (for audience), iat, exp, as well as custom fields of the form "<resource> -> <permission>" set by
            // Pravega.
            Set<Map.Entry<String, Object>> claims =
                    JsonWebToken.fetchClaims(token, config.getTokenSigningKey().getBytes());

            Optional<Map.Entry<String, Object>> matchingClaim = claims.stream()
                    .filter(entry -> entryMatchesResource(entry, resource) &&
                                     expectedLevel.compareTo(AuthHandler.Permissions.valueOf(entry.getValue().toString())) <= 0)
                    .findFirst();

            if (!matchingClaim.isPresent()) {
                log.trace(String.format("No matching claim found for resource [%s] and permission [%s] in token [%s].",
                        resource, expectedLevel.toString(), token));

                throw new InvalidClaimException(String.format(
                        "No matching claim found for resource: [%s] and permission: [%s] in the delegation token.",
                        resource, expectedLevel.toString()));

            }
        }
    }

    private boolean entryMatchesResource(Map.Entry<String, Object> entry, String resource) {
        return resource.equals(entry.getKey()) // exact match

               /*
                * Examples of matches:
                *   - Entry = "abc/, ...", resource = "abc/xyx"
                */
               || entry.getKey().endsWith("/") && resource.startsWith(entry.getKey())

               /*
                * Examples of matches:
                *    1) Entry = "_system/_requeststream, READ_UPDATE", resource = "_system/_requeststream/0.#epoch.0"
                */
               || resource.startsWith(entry.getKey() + "/")

                // The wildcard character matches in entry matches every possible resource.
               || entry.getKey().equals("*");
    }
}

/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.delegationtoken;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.pravega.auth.AuthHandler;
import io.pravega.segmentstore.server.host.stat.AutoScalerConfig;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TokenVerifierImpl implements DelegationTokenVerifier {
    private final AutoScalerConfig config;

    public TokenVerifierImpl(AutoScalerConfig config) {
        this.config = config;
    }

    @Override
    public boolean verifyToken(String resource, String token, AuthHandler.Permissions expectedLevel) {
        if (config.isAuthEnabled()) {
            try {
                Jws<Claims> claims = Jwts.parser()
                                         .setSigningKey(config.getTokenSigningKey().getBytes())
                                         .parseClaimsJws(token);
                Optional<Map.Entry<String, Object>> matchingClaim = claims.getBody().entrySet().stream().filter(entry ->
                        validateEntry(entry, resource)
                        && expectedLevel.compareTo(AuthHandler.Permissions.valueOf(entry.getValue().toString()))
                        <= 0).findFirst();
                if (matchingClaim.isPresent()) {
                    log.debug("Found a matching claim {} for resource {}", matchingClaim, resource);
                    return true;
                } else {
                    log.debug("Could not find a matching claim {} for resource {} in claims {}",
                            expectedLevel, resource, claims);
                    return false;
                }
            } catch (JwtException e) {
                log.warn("Claim verification failed for resource {} because {}", resource, e);
                return false;
            }
        } else {
            return true;
        }
    }

    private boolean validateEntry(Map.Entry<String, Object> entry, String resource) {
        return (entry.getKey().endsWith("/") && resource.startsWith(entry.getKey()))
                    ||  resource.startsWith(entry.getKey() + "/")
                || entry.getKey().equals("*");
    }
}

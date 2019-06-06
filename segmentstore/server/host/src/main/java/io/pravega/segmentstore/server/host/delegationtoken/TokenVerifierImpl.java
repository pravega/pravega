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

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.MalformedJwtException;
import io.jsonwebtoken.SignatureException;
import io.pravega.auth.AuthHandler;
import io.pravega.auth.InvalidClaimException;
import io.pravega.auth.InvalidTokenException;
import io.pravega.auth.TokenException;
import io.pravega.auth.TokenExpiredException;
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
            throws TokenExpiredException, InvalidTokenException, InvalidClaimException {
        if (config.isAuthEnabled()) {

            Jws<Claims> claims = parseClaims(token);

            Optional<Map.Entry<String, Object>> matchingClaim = claims.getBody().entrySet().stream().filter(entry ->
                    validateEntry(entry, resource)
                            && expectedLevel.compareTo(AuthHandler.Permissions.valueOf(entry.getValue().toString()))
                            <= 0).findFirst();

            if (!matchingClaim.isPresent()) {
                throw new InvalidClaimException("No matching claim found for resource " + resource);
            }
        }
    }

    private Jws<Claims> parseClaims(String token) throws TokenExpiredException, InvalidTokenException {
        Jws<Claims> claims = null;
        try {
            claims = Jwts.parser()
                    .setSigningKey(config.getTokenSigningKey().getBytes())
                    .parseClaimsJws(token);
            log.debug("Successfully parsed JWT token");
        } catch (ExpiredJwtException e) {
            throw new TokenExpiredException(e);
        } catch (MalformedJwtException | SignatureException | IllegalArgumentException e) {
            throw new InvalidTokenException(e);
        }
        return claims;
    }

    private boolean validateEntry(Map.Entry<String, Object> entry, String resource) {
        return (entry.getKey().endsWith("/") && resource.startsWith(entry.getKey()))
                    ||  resource.startsWith(entry.getKey() + "/")
                || entry.getKey().equals("*");
    }
}

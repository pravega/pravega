/**
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.security.token;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.MalformedJwtException;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.SignatureException;
import io.pravega.auth.InvalidTokenException;
import io.pravega.auth.TokenException;
import io.pravega.auth.TokenExpiredException;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * Creates Json Web Tokens (JWT). Also, parses a JWT.
 */
@ToString
@Slf4j
public class JsonWebToken {

    public final static String TOKEN_KEY_PREFIX = "pra-";

    /*
     * Using this variable for disambiguating the constructor invoked.
     */
    private static final Integer NULLINT = null;

    private final String subject;
    private final String audience;
    private final byte[] signingKey;

    private Date expirationTime;
    private Instant currentInstant;

    private Map<String, Object> permissionsByResource;
    private SignatureAlgorithm signatureAlgorithm = SignatureAlgorithm.HS512;

    public JsonWebToken(String subject, String audience, byte[] signingKey) {
        this(subject, audience, signingKey, NULLINT, null);
    }

    public JsonWebToken(@NonNull String subject, @NonNull String audience, @NonNull byte[] signingKey,
                        Integer timeToLiveInSeconds,
                        Map<String, Object> resourcePermissionClaims) {

        if (timeToLiveInSeconds != null) {
            // timetoLiveInSeconds = -1 implies that the token never expires.
            // timeToLiveInSeconds = 0 implies token immediately expires.
            Preconditions.checkArgument(timeToLiveInSeconds >= -1);
        }

        this.subject = subject;
        this.audience = audience;
        this.signingKey = signingKey.clone();

        this.currentInstant = Instant.now();

        if (timeToLiveInSeconds != null && timeToLiveInSeconds != -1) {
            this.expirationTime = Date.from(this.currentInstant.plusSeconds(timeToLiveInSeconds));
        }
        this.permissionsByResource = resourcePermissionClaims;
    }

    /**
     * Returns the 3 part JWT string representation.
     *
     *  Example JWT:
     *   - Compact representation:
     *       eyJhbGciOiJIUzUxMiJ9.eyJleHAiOjM3MDc2MjUyMjgsInMxIjoiUkVBRF9VUERBVEUifQ.j6xbFRIIZxv3GEedqKcZVy-49Y7U1710q-gjY43-UMgO_kwCH_9kJRuZ7Am589kg5TJewmGhGB9SPblES78pEg
     *   - Decoded parts:
     *       - header: {alg=HS512}
     *       - body/payload: {exp=3707625228, s1=READ_UPDATE},
     *       - signature: j6xbFRIIZxv3GEedqKcZVy-49Y7U1710q-gjY43-UMgO_kwCH_9kJRuZ7Am589kg5TJewmGhGB9SPblES78pEg
     *
     * @return compact representation of JWT
     */
    public String toCompactString() {
        JwtBuilder builder = Jwts.builder()
                .setSubject(subject)
                .setAudience(audience)
                .setIssuedAt(Date.from(currentInstant));

        if (this.permissionsByResource != null) {
            // Subject, audience and issued at fields are claims (in the JWT body) too. Invoking the setClaims()
            // will override the fields we set before. Therefore, we use the append method addClaims(..), instead.
            builder.addClaims(permissionsByResource);
        }
        if (this.expirationTime != null) {
            builder.setExpiration(expirationTime);
        }
        builder.signWith(signatureAlgorithm, signingKey);
        return builder.compact();
    }

    @VisibleForTesting
    static Claims parseClaims(String token, byte[] signingKey) throws TokenExpiredException,
            InvalidTokenException, TokenException {
        try {
            Jws<Claims> claimsJws = Jwts.parser()
                    .setSigningKey(signingKey)
                    .parseClaimsJws(token);
            log.debug("Successfully parsed JWT token.");
            return claimsJws.getBody();
        } catch (ExpiredJwtException e) {
            throw new TokenExpiredException(e);
        } catch (MalformedJwtException | SignatureException | IllegalArgumentException e) {
            throw new InvalidTokenException(e);
        } catch (JwtException e) {
            throw new TokenException(e);
        }
    }

    public static Set<Map.Entry<String, Object>> fetchClaims(String token, byte[] signingKey)
            throws TokenException {
        return parseClaims(token, signingKey).entrySet();
    }

    public static Map<String, Object> extractCustomClaims(String token, byte[] signingKey) throws TokenException {
        Claims claims = parseClaims(token, signingKey);

        Map<String, Object> result = new HashMap<>();
        claims.entrySet().forEach(entry -> result.put(entry.getKey(), entry.getValue()));
        return result;
    }
}

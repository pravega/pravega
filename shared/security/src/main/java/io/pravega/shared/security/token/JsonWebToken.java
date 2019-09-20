/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
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
import com.google.common.base.Strings;
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

    /**
     * The value of the JWT "sub" (Subject) claim (https://tools.ietf.org/html/rfc7519#section-4.1.2)
     */
    private final String subject;

    /**
     * The value of the JWT "aud" (Sudience) claim (https://tools.ietf.org/html/rfc7519#section-4.1.3)
     */
    private final String audience;

    /**
     * The key used for signing the JWT.
     */
    private final byte[] signingKey;

    /**
     * The value of the JWT "exp" (Expiration Time) claim (https://tools.ietf.org/html/rfc7519#section-4.1.4).
     */
    private final Date expirationTime;

    private final Instant currentInstant;
    private final Map<String, Object> permissionsByResource;
    private final SignatureAlgorithm signatureAlgorithm = SignatureAlgorithm.HS512;

    /**
     * Creates a new instance of this class.
     *
     * @param subject the subject of the token
     * @param audience the intended recipient of the token
     * @param signingKey the signing key to be used for generating the token signature
     */
    public JsonWebToken(String subject, String audience, byte[] signingKey) {
        this(subject, audience, signingKey, null, null);
    }

    /**
     * Creates a new instance of this class.
     *
     * @param subject the subject of the token
     * @param audience the intended recipient of the token
     * @param signingKey the signing key to be used for generating the token signature
     * @param timeToLiveInSeconds number of seconds relating to the current time after which the token should expire
     * @param resourcePermissionClaims a {@link java.util.Map} object with entries comprising of
     *                                 Pravega resource representation as key and permission as value
     */
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
        } else {
            // Token never expires.
            this.expirationTime = null;
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

        if (Strings.isNullOrEmpty(token)) {
            throw new InvalidTokenException("Token is null or empty");
        }
        // We don't need to validate signingKey as the code below will throw IllegalArgumentException if signingKey
        // is null.

        try {
            Jws<Claims> claimsJws = Jwts.parser()
                    .setSigningKey(signingKey)
                    .parseClaimsJws(token);
            log.debug("Successfully parsed JWT token.");
            return claimsJws.getBody();
        } catch (ExpiredJwtException e) {
            throw new TokenExpiredException(e);
        } catch (MalformedJwtException | SignatureException e) {
            throw new InvalidTokenException(e);
        } catch (JwtException e) {
            throw new TokenException(e);
        }
    }

    /**
     * Fetches claims from a given token.
     *
     * @param token the token to fetch the claims from
     * @param signingKey the key that was used for signing the token
     * @return a Set view of the mappings contained in this Claims map extracted from the token.
     *
     * @throws TokenException if any failure in parsing the token or extracting the claims occurs
     */
    public static Set<Map.Entry<String, Object>> fetchClaims(String token, byte[] signingKey)
            throws TokenException {
        return parseClaims(token, signingKey).entrySet();
    }
}

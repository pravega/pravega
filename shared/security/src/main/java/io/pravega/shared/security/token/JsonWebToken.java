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
package io.pravega.shared.security.token;

import com.google.common.base.Preconditions;
import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import lombok.Getter;
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
    @Getter
    private final String subject;

    /**
     * The value of the JWT "aud" (Sudience) claim (https://tools.ietf.org/html/rfc7519#section-4.1.3)
     */
    @Getter
    private final String audience;

    /**
     * The key used for signing the JWT.
     */
    private final byte[] signingKey;

    /**
     * The value of the JWT "exp" (Expiration Time) claim (https://tools.ietf.org/html/rfc7519#section-4.1.4).
     */
    @Getter
    private final Date expirationTime;

    private final Instant currentInstant = Instant.now();

    @Getter
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
        this(subject, audience, signingKey, new HashMap<>(), null);
    }

    /**
     * Creates a new instance of this class.
     *
     * @param subject the subject of the token
     * @param audience the intended recipient of the token
     * @param signingKey the signing key to be used for generating the token signature
     * @param resourcePermissionClaims a {@link java.util.Map} object with entries comprising of
     *                                 Pravega resource representation as key and permission as value
     * @param timeToLiveInSeconds number of seconds relating to the current time after which the token should expire
     */
    public JsonWebToken(@NonNull String subject, @NonNull String audience, @NonNull byte[] signingKey,
                        Map<String, Object> resourcePermissionClaims, Integer timeToLiveInSeconds) {

        if (timeToLiveInSeconds != null) {
            // timetoLiveInSeconds = -1 implies that the token never expires.
            // timeToLiveInSeconds = 0 implies token immediately expires.
            Preconditions.checkArgument(timeToLiveInSeconds >= -1);
        }

        this.subject = subject;
        this.audience = audience;
        this.signingKey = signingKey.clone();

        if (timeToLiveInSeconds != null && timeToLiveInSeconds != -1) {
            this.expirationTime = Date.from(this.currentInstant.plusSeconds(timeToLiveInSeconds));
        } else {
            // Token never expires.
            this.expirationTime = null;
        }
        this.permissionsByResource = resourcePermissionClaims;
    }

    public JsonWebToken(@NonNull String subject, @NonNull String audience, @NonNull byte[] signingKey,
                        Date expiry, Map<String, Object> resourcePermissionClaims) {
        this.subject = subject;
        this.audience = audience;
        this.signingKey = signingKey.clone();
        if (expiry != null) {
            this.expirationTime = Date.from(expiry.toInstant());
        } else {
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

    /**
     * Returns the duration relative to the current instant in which the specified JWT is to expire.
     *
     * @return the duration relative to the current instant. Returns null if
     *         a) the specified token is blank, or
     *         b) the specified token is of invalid format, or
     *         c) expiration time is missing from the token, or
     *         d) expiration time is not a number.
     */
    public Duration durationToExpiry() {
        if (this.expirationTime == null) {
            return null;
        } else {
            return Duration.between(this.currentInstant, expirationTime.toInstant());
        }
    }

    public static JsonWebToken emptyToken() {
        return new JsonWebToken("empty", "empty", "empty".getBytes());
    }
}

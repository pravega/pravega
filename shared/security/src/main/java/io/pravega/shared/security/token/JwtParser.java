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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.pravega.auth.InvalidTokenException;
import io.pravega.auth.TokenExpiredException;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class JwtParser {

    private final static List<String> CLAIMS_TO_FILTER = Arrays.asList("sub", "aud", "exp", "iat");

    /**
     * Fetches claims from a given token.
     *
     * @param token the token to fetch the claims from
     * @param signingKey the key that was used for signing the token
     * @return a Set view of the mappings contained in this Claims map extracted from the token.
     *
     * @throws TokenExpiredException if the token has expired
     * @throws InvalidTokenException if any failure in parsing the token, verifying the signature or
     *                               extracting the claims occurs
     */
    public static Set<Map.Entry<String, Object>> fetchClaims(String token, byte[] signingKey)
            throws TokenExpiredException, InvalidTokenException {
        return parseClaims(token, signingKey).entrySet();
    }

    public static JsonWebToken parse(String token, byte[] signingKey) {
        Claims claims = parseClaims(token, signingKey);
        if (claims == null) {
            throw new InvalidTokenException("Token has no claims.");
        }

        final Map<String, Object> permissionsByResource = new HashMap<>();
        claims.entrySet().forEach(entry -> {
            if (!CLAIMS_TO_FILTER.contains(entry.getKey())) {
                permissionsByResource.put(entry.getKey(), entry.getValue());
            }
        });
        return new JsonWebToken(claims.getSubject(), claims.getAudience(), signingKey,
                claims.getExpiration(), permissionsByResource);
    }

    @VisibleForTesting
    static Claims parseClaims(String token, byte[] signingKey) throws TokenExpiredException,
            InvalidTokenException {

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
        } catch (JwtException e) {
            throw new InvalidTokenException(e);
        }
    }
}

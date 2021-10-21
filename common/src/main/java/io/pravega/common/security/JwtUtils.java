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
package io.pravega.common.security;

import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility methods for JSON Web Tokens (JWT).
 */
@Slf4j
public class JwtUtils {

    /**
     * The regex pattern for extracting "exp" field from the JWT.
     *
     * Examples:
     *    Input:- {"sub":"subject","aud":"segmentstore","iat":1569837384,"exp":1569837434}, output:- "exp":1569837434
     *    Input:- {"sub": "subject","aud": "segmentstore","iat": 1569837384,"exp": 1569837434}, output:- "exp": 1569837434
     */
    private static final Pattern JWT_EXPIRATION_PATTERN = Pattern.compile("\"exp\":\\s?(\\d+)");

    /**
     * Extracts expiration time from the specified JWT token.
     *
     * @param jsonWebToken the JWT to extract the expiration time from
     * @return the the expiration time (in seconds). Returns null if
     *         a) the specified token is blank, or
     *         b) the specified token is of invalid format, or
     *         c) expiration time is missing from the token, or
     *         d) expiration time is not a number.
     */
    public static Long extractExpirationTime(String jsonWebToken) {
        if (jsonWebToken == null || jsonWebToken.trim().equals("")) {
            return null;
        }
        String[] tokenParts = jsonWebToken.split("\\.");

        // A JWT token has 3 parts: the header, the body and the signature.
        if (tokenParts == null || tokenParts.length != 3) {
            return null;
        }

        // The second part of the JWT token is the body, which contains the expiration time if present.
        String encodedBody = tokenParts[1];
        String decodedJsonBody = new String(Base64.getDecoder().decode(encodedBody));

        return parseExpirationTime(decodedJsonBody);
    }

    @VisibleForTesting
    static Long parseExpirationTime(String jwtBody) {
        Long result = null;
        if (jwtBody != null && !jwtBody.trim().equals("")) {
            Matcher matcher = JWT_EXPIRATION_PATTERN.matcher(jwtBody);
            if (matcher.find()) {
                // Should look like this, if a proper match is found: "exp": 1569837434
                String matchedString = matcher.group();

                String[] expiryTimeFieldParts = matchedString.split(":");
                if (expiryTimeFieldParts != null && expiryTimeFieldParts.length == 2) {
                    result = Long.parseLong(expiryTimeFieldParts[1].trim());
                }
            }
        }
        return result;
    }
}

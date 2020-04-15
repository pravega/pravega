/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.security;

import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

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

    public static Long extractExpirationTime(String token) {
        if (token == null || token.trim().equals("")) {
            return null;
        }
        String[] tokenParts = token.split("\\.");

        //A JWT token has 3 parts: the header, the body and the signature.
        if (tokenParts == null || tokenParts.length != 3) {
            return null;
        }

        // The second part of the JWT token is the body, which contains the expiration time if present.
        String encodedBody = tokenParts[1];
        String decodedJsonBody = new String(Base64.getDecoder().decode(encodedBody));

        return parseExpirationTime(decodedJsonBody);
    }

    public static Duration durationToExpiry(String token) {
        Long expirationTime = extractExpirationTime(token);
        if (expirationTime == null) {
            return null;
        } else {
            return Duration.between(Instant.now(), Instant.ofEpochSecond(expirationTime));
        }
    }

    public static Long parseExpirationTime(String jwtBody) {
        Long result = null;
        if (jwtBody != null && !jwtBody.trim().equals("")) {
            Matcher matcher = JWT_EXPIRATION_PATTERN.matcher(jwtBody);
            if (matcher.find()) {
                // Should look like this, if a proper match is found: "exp": 1569837434
                String matchedString = matcher.group();

                // JwtUtils

                String[] expiryTimeFieldParts = matchedString.split(":");
                if (expiryTimeFieldParts != null && expiryTimeFieldParts.length == 2) {
                    try {
                        result = Long.parseLong(expiryTimeFieldParts[1].trim());
                    } catch (NumberFormatException e) {
                        // ignore
                        log.warn("Encountered this exception when parsing JWT body for expiration time: {}", e.getMessage());
                    }
                }
            }
        }
        return result;
    }
}

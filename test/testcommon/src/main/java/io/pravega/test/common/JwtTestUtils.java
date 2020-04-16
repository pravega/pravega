/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.common;

import com.google.gson.Gson;

import java.util.Base64;

/**
 * JSON Web Tokens (JWT) related utility methods for aiding testing.
 */
public class JwtTestUtils {

    /**
     * Convert the specified {@code jwtBodyPart} to Compact JWT format.
     *
     * @param jwtBodyPart the JWT body
     * @return a Base64 encoded JSON representing the specified {@code jwtBodyPart}
     */
    public static String toCompact(JwtBody jwtBodyPart) {
        String json = new Gson().toJson(jwtBodyPart);
        return Base64.getEncoder().encodeToString(json.getBytes());
    }

    /**
     * Creates a JSON Web Token (JWT) in compact format containing dummy header and signature parts and the
     * specified {@code jwtBodyPart}.
     *
     * @param jwtBodyPart the JWT body to include in the JWT.
     * @return JWT in compact format containing dummy header and signature and the specified {@code jwtBodyPart}
     */
    public static String createTokenWithDummyMetadata(JwtBody jwtBodyPart) {
        return String.format("header.%s.signature", toCompact(jwtBodyPart));
    }

    /**
     * Creates a dummy JWT in compact format.
     * @return the dummy JWT token
     */
    public static String dummyToken() {
        return String.format("header.%s.signature",
                toCompact(JwtBody.builder().expirationTime(Long.MAX_VALUE).build()));
    }
}

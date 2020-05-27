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

<<<<<<< HEAD
<<<<<<< HEAD
import java.nio.charset.StandardCharsets;
=======
import com.google.gson.Gson;

>>>>>>> Issue 4691: Honor delegation token expiration during appends  (#4692)
=======
import java.nio.charset.StandardCharsets;
>>>>>>> Issue 4811: Replace JWT object-JSON mapping library (#4812)
import java.time.Instant;
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
<<<<<<< HEAD
<<<<<<< HEAD
        String json = jwtBodyPart.toString();
        return Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
=======
        String json = new Gson().toJson(jwtBodyPart);
        return Base64.getEncoder().encodeToString(json.getBytes());
>>>>>>> Issue 4691: Honor delegation token expiration during appends  (#4692)
=======
        String json = jwtBodyPart.toString();
        return Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
>>>>>>> Issue 4811: Replace JWT object-JSON mapping library (#4812)
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
     * @return a JWT token
     */
    public static String createDummyToken() {
        return createTokenWithDummyMetadata(JwtBody.builder()
                .subject("some-subject")
                .audience("some-audience")
                .issuedAtTime(Instant.now().getEpochSecond())
                .expirationTime(Instant.now().plusSeconds(1000).getEpochSecond())
                .build());
    }

    /**
     * Creates an empty dummy JWT token in compact format.
     * @return a JWT token
     */
    public static String createEmptyDummyToken() {
        return createTokenWithDummyMetadata(JwtBody.builder().build());

    }

    /**
     * Creates a dummy JWT token with no expiration set.
     *
     * @return a JWT token
     */
    public static String createDummyTokenWithNoExpiry() {
        return createTokenWithDummyMetadata(JwtBody.builder()
                .subject("some-subject")
                .audience("some-audience")
                .issuedAtTime(Instant.now().getEpochSecond())
                .build());
    }
}

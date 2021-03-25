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
package io.pravega.test.common;

import java.nio.charset.StandardCharsets;
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
        String json = jwtBodyPart.toString();
        return Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));
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

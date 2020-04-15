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

import io.pravega.test.common.JwtBody;
import io.pravega.test.common.JwtTestUtils;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class JwtUtilsTest {

    @Test
    public void testExtractExpirationTimeReturnsNullIfExpInBodyIsNotSet() {

        // See decoded parts at https://jwt.io/.
        //
        // The body decodes to:
        //     {
        //        "sub": "1234567890",
        //        "aud": "segmentstore",
        //        "iat": 1516239022
        //     }
        String token = String.format("%s.%s.%s", "base64-encoded-header",
                JwtBody.builder().subject("1234567890").audience("segmentstore").issuedAtTime(1516239022L).build(),
                "base64-encoded-signature");

        assertNull(JwtUtils.extractExpirationTime(token));
    }

    @Test
    public void testExtractExpirationTimeReturnsNullIfTokenIsNotInJwtFormat() {
        assertNull(JwtUtils.extractExpirationTime("abc"));
        assertNull(JwtUtils.extractExpirationTime("abc.def"));
        assertNull(JwtUtils.extractExpirationTime("abc.def.ghi.jkl"));
    }

    // Refresh behavior when expiration time is not set
    // public void testExpirationTimeIsNullIfExpInBodyIsNotSet

    @Test
    public void testExpirationTimeIsNotNullIfExpInBodyIsSet() {

        // See decoded parts at https://jwt.io/.
        //
        // The body decodes to:
        //     {
        //        "sub": "jdoe",
        //        "aud": "segmentstore",
        //         "iat": 1569324678,
        //         "exp": 1569324683
        //     }
        String token = String.format("%s.%s.%s",
                "eyJhbGciOiJIUzUxMiJ9", // header
                "eyJzdWIiOiJqZG9lIiwiYXVkIjoic2VnbWVudHN0b3JlIiwiaWF0IjoxNTY5MzI0Njc4LCJleHAiOjE1NjkzMjQ2ODN9", // body
                "EKvw5oVkIihOvSuKlxiX7q9_OAYz7m64wsFZjJTBkoqg4oidpFtdlsldXHToe30vrPnX45l8QAG4DoShSMdw"); // signature
        assertNotNull(JwtUtils.extractExpirationTime(token));
    }

    @Test
    public void testReturnsNullExpirationTimeForNullToken() {
          assertNull(JwtUtils.extractExpirationTime(null));
    }

    @Test
    public void testParseExpirationTimeExtractsExpiryTime() {
        // Contains a space before each field value
        String json1 = "{\"sub\": \"subject\",\"aud\": \"segmentstore\",\"iat\": 1569837384,\"exp\": 1569837434}";
        assertEquals(1569837434, JwtUtils.parseExpirationTime(json1).longValue());

        // Does not contain space before field values
        String json2 = "{\"sub\":\"subject\",\"aud\":\"segmentstore\",\"iat\":1569837384,\"exp\":1569837434}";
        assertEquals(1569837434, JwtUtils.parseExpirationTime(json2).longValue());
    }

    @Test
    public void testParseExpirationTimeReturnsNullWhenExpiryIsNotSet() {
        // Does not contain expiry time
        String json = "{\"sub\":\"subject\",\"aud\":\"segmentstore\",\"iat\":1569837384}";

        assertNull(JwtUtils.parseExpirationTime(json));
    }

    @Test
    public void testParseExpirationTimeReturnsNullWhenTokenIsNullOrEmpty() {
        assertNull(JwtUtils.parseExpirationTime(null));
        assertNull(JwtUtils.parseExpirationTime(""));
    }

    @Test
    public void testParseExpirationTimeReturnsNullWhenTokenIsNotInteger() {
        // Notice that the exp field value contains non-digits/alphabets
        String jwtBody = "{\"sub\":\"subject\",\"aud\":\"segmentstore\",\"iat\":1569837384,\"exp\":\"abc\"}";
        assertNull(JwtUtils.parseExpirationTime(jwtBody));
    }

    @Test
    public void durationToExpiryIsNullIfTokenIsBlank() {
        assertNull(JwtUtils.durationToExpiry(null));
        assertNull(JwtUtils.durationToExpiry(""));
    }

    @Test
    public void durationToExpiryIsNullIfTokenHasNoExpiration() {
        String token = JwtTestUtils.createTokenWithDummyMetadata(
                JwtBody.builder().subject("sub").audience("aud").build());
        assertNull(JwtUtils.durationToExpiry(token));
    }

    @Test
    public void durationToExpiryIsPositiveIfTokenExpiryIsInFuture() {
        String token = JwtTestUtils.createTokenWithDummyMetadata(
                JwtBody.builder().expirationTime(Instant.now().plusSeconds(100).getEpochSecond()).build());
        assertFalse(JwtUtils.durationToExpiry(token).isNegative());
    }

    @Test
    public void durationToExpiryIsNegativeIfTokenExpiryIsInThePast() {
        String token = JwtTestUtils.createTokenWithDummyMetadata(
                JwtBody.builder().expirationTime(Instant.now().minusSeconds(2).getEpochSecond()).build());
        Duration duration = JwtUtils.durationToExpiry(token);
        System.out.println(duration.getSeconds());
        assertTrue("Duration is not negative", duration.isNegative());
    }
}

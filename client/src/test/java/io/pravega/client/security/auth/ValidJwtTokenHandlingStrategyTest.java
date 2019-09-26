/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.security.auth;

import com.google.gson.Gson;
import io.pravega.client.stream.impl.Controller;
import org.junit.Test;

import java.time.Instant;
import java.util.Base64;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class ValidJwtTokenHandlingStrategyTest {

    @Test
    public void testIsWithinThresholdForRefresh() {
        ValidJwtTokenHandlingStrategy strategy = new ValidJwtTokenHandlingStrategy(
                dummyToken(), mock(Controller.class), "somescope", "somestream");

        assertFalse(strategy.isWithinRefreshThreshold(Instant.ofEpochSecond(10), Instant.ofEpochSecond(30)));
        assertFalse(strategy.isWithinRefreshThreshold(Instant.now(), Instant.now().plusSeconds(11)));
        assertTrue(strategy.isWithinRefreshThreshold(Instant.ofEpochSecond(50), Instant.ofEpochSecond(40)));
        assertTrue(strategy.isWithinRefreshThreshold(Instant.ofEpochSecond(50), Instant.ofEpochSecond(55)));
        assertTrue(strategy.isWithinRefreshThreshold(Instant.ofEpochSecond(50), Instant.ofEpochSecond(60)));
    }

    @Test
    public void testRetrievesSameTokenPassedDuringConstruction() {
        String token = String.format("header.%s.signature", createJwtBody(
                Jwt.builder().exp(Long.MAX_VALUE).build()));
        ValidJwtTokenHandlingStrategy objectUnderTest = new ValidJwtTokenHandlingStrategy(
                token, mock(Controller.class), "somescope", "somestream");
        assertEquals(token, objectUnderTest.retrieveToken());
    }

    @Test
    public void testExpirationTimeIsNullIfExpInBodyIsNotSet() {

        // See decoded parts at https://jwt.io/.
        //
        // The body decodes to:
        //     {
        //        "sub": "1234567890",
        //        "name": "John Doe",
        //        "iat": 1516239022
        //     }
        String token = String.format("%s.%s.%s",
                "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9", // header
                "eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ", // body
                "SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"); // signature

        ValidJwtTokenHandlingStrategy objectUnderTest = new ValidJwtTokenHandlingStrategy(
                token, mock(Controller.class), "somescope", "somestream");

        assertNull(objectUnderTest.extractExpirationTime(token));
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
        ValidJwtTokenHandlingStrategy objectUnderTest = new ValidJwtTokenHandlingStrategy(
                token, mock(Controller.class), "somescope", "somestream");
        assertNotNull(objectUnderTest.extractExpirationTime(token));
    }

    private String createJwtBody(Jwt jwt) {
        String json = new Gson().toJson(jwt);
        System.out.println(json);
        return Base64.getEncoder().encodeToString(json.getBytes());
    }

    private String dummyToken() {
        return String.format("header.%s.signature", createJwtBody(Jwt.builder().exp(Long.MAX_VALUE).build()));
    }
}

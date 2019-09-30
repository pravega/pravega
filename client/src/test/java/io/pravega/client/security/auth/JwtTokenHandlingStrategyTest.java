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

import io.pravega.client.stream.impl.Controller;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static io.pravega.client.security.auth.JwtTestUtils.createJwtBody;
import static io.pravega.client.security.auth.JwtTestUtils.dummyToken;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Slf4j
public class JwtTokenHandlingStrategyTest {

    private Controller dummyController = mock(Controller.class);

    @Test
    public void testIsWithinThresholdForRefresh() {
        JwtTokenHandlingStrategy strategy = new JwtTokenHandlingStrategy(
                dummyToken(), dummyController, "somescope", "somestream");

        assertFalse(strategy.isWithinRefreshThreshold(Instant.ofEpochSecond(10), Instant.ofEpochSecond(30)));
        assertFalse(strategy.isWithinRefreshThreshold(Instant.now(), Instant.now().plusSeconds(11)));
        assertTrue(strategy.isWithinRefreshThreshold(Instant.ofEpochSecond(50), Instant.ofEpochSecond(40)));
        assertTrue(strategy.isWithinRefreshThreshold(Instant.ofEpochSecond(50), Instant.ofEpochSecond(55)));
        assertTrue(strategy.isWithinRefreshThreshold(Instant.ofEpochSecond(50), Instant.ofEpochSecond(53)));
    }

    @Test
    public void testRetrievesSameTokenPassedDuringConstruction() {
        String token = String.format("header.%s.signature", createJwtBody(
                JwtBody.builder().expirationTime(Instant.now().plusSeconds(10000).getEpochSecond()).build()));
        JwtTokenHandlingStrategy objectUnderTest = new JwtTokenHandlingStrategy(
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

        JwtTokenHandlingStrategy objectUnderTest = new JwtTokenHandlingStrategy(
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
        JwtTokenHandlingStrategy objectUnderTest = new JwtTokenHandlingStrategy(
                token, dummyController, "somescope", "somestream");
        assertNotNull(objectUnderTest.extractExpirationTime(token));
    }

    @Test
    public void testRetrievesNewTokenIfTokenIsNearingExpiry() {
        String token = String.format("header.%s.signature", createJwtBody(
                JwtBody.builder().expirationTime(Instant.now().minusSeconds(1).getEpochSecond()).build()));
        log.debug("token: {}", token);

        // Setup mock
        Controller mockController = mock(Controller.class);
        CompletableFuture<String> future = CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                return String.format("newtokenheader.%s.signature", createJwtBody(
                        JwtBody.builder().expirationTime(Instant.now().plusSeconds(10000).getEpochSecond()).build()));
            }
        });
        when(mockController.getOrRefreshDelegationTokenFor("somescope", "somestream"))
                .thenReturn(future);

        // Setup the object under test
        JwtTokenHandlingStrategy objectUnderTest = new JwtTokenHandlingStrategy(
                token, mockController, "somescope", "somestream");

        // Act
        String newToken = objectUnderTest.retrieveToken();
        log.debug(newToken);

        assertTrue(newToken.startsWith("newtokenheader"));
    }

    @Test
    public void testReturnsNullExpirationTimeForNullToken() {
        JwtTokenHandlingStrategy objectUnderTest = new JwtTokenHandlingStrategy(
                dummyToken(), dummyController, "some-scope", "some-stream");
        assertNull(objectUnderTest.extractExpirationTime(null));
    }

    @Test
    public void testReturnsNullExpirationTimeForEmptyToken() {
        JwtTokenHandlingStrategy objectUnderTest = new JwtTokenHandlingStrategy(
                dummyToken(), dummyController, "some-scope", "some-stream");
        assertNull(objectUnderTest.extractExpirationTime(null));
    }
}

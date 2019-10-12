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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Slf4j
public class JwtTokenProviderImplTest {

    private Controller dummyController = mock(Controller.class);

    @Test
    public void testIsWithinThresholdForRefresh() {
        JwtTokenProviderImpl objectUnderTest = new JwtTokenProviderImpl(
                dummyToken(), dummyController, "somescope", "somestream");

        assertFalse(objectUnderTest.isWithinRefreshThreshold(Instant.ofEpochSecond(10), Instant.ofEpochSecond(30)));
        assertFalse(objectUnderTest.isWithinRefreshThreshold(Instant.now(), Instant.now().plusSeconds(11)));
        assertTrue(objectUnderTest.isWithinRefreshThreshold(Instant.ofEpochSecond(50), Instant.ofEpochSecond(40)));
        assertTrue(objectUnderTest.isWithinRefreshThreshold(Instant.ofEpochSecond(50), Instant.ofEpochSecond(55)));
        assertTrue(objectUnderTest.isWithinRefreshThreshold(Instant.ofEpochSecond(50), Instant.ofEpochSecond(53)));
    }

    @Test
    public void testRetrievesSameTokenPassedDuringConstruction() {
        String token = String.format("header.%s.signature", createJwtBody(
                JwtBody.builder().expirationTime(Instant.now().plusSeconds(10000).getEpochSecond()).build()));
        JwtTokenProviderImpl objectUnderTest = new JwtTokenProviderImpl(
                token, mock(Controller.class), "somescope", "somestream");
        assertEquals(token, objectUnderTest.retrieveToken());
    }

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

        JwtTokenProviderImpl objectUnderTest = new JwtTokenProviderImpl(
                token, mock(Controller.class), "somescope", "somestream");

        assertNull(objectUnderTest.extractExpirationTime(token));
    }

    @Test
    public void testExtractExpirationTimeReturnsNullIfTokenIsNotInJwtFormat() {
        JwtTokenProviderImpl objectUnderTest = new JwtTokenProviderImpl(
                mock(Controller.class), "somescope", "somestream");
        assertNull(objectUnderTest.extractExpirationTime("abc"));
        assertNull(objectUnderTest.extractExpirationTime("abc.def"));
        assertNull(objectUnderTest.extractExpirationTime("abc.def.ghi.jkl"));
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
        JwtTokenProviderImpl objectUnderTest = new JwtTokenProviderImpl(
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
        JwtTokenProviderImpl objectUnderTest = new JwtTokenProviderImpl(
                token, mockController, "somescope", "somestream");

        // Act
        String newToken = objectUnderTest.retrieveToken();
        log.debug("new token: {}", newToken);

        assertTrue(newToken.startsWith("newtokenheader"));
    }

    @Test
    public void testRetrievesSameTokenOutsideOfTokenRefreshThresholdWhenTokenIsNull() {
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
        DelegationTokenProvider objectUnderTest = new JwtTokenProviderImpl(mockController,
                "somescope", "somestream");

        // Act
        String token = objectUnderTest.retrieveToken();
        assertEquals(token, objectUnderTest.retrieveToken());
    }

    @Test
    public void testReturnsNullExpirationTimeForNullToken() {
        JwtTokenProviderImpl objectUnderTest = new JwtTokenProviderImpl(
                dummyToken(), dummyController, "some-scope", "some-stream");
        assertNull(objectUnderTest.extractExpirationTime(null));
    }

    @Test
    public void testReturnsNullExpirationTimeForEmptyToken() {
        JwtTokenProviderImpl objectUnderTest = new JwtTokenProviderImpl(
                dummyToken(), dummyController, "some-scope", "some-stream");
        assertNull(objectUnderTest.extractExpirationTime(null));
    }

    @Test
    public void testDefaultTokenRefreshThreshold() {
        JwtTokenProviderImpl objectUnderTest = new JwtTokenProviderImpl(
                dummyToken(), dummyController, "some-scope", "some-stream");
        assertSame(JwtTokenProviderImpl.DEFAULT_REFRESH_THRESHOLD_SECONDS,
                objectUnderTest.getRefreshThresholdInSeconds());
    }

    @Test
    public void testReturnsExistingTokenIfNotNearingExpiry() {
        String encodedJwtBody = createJwtBody(JwtBody.builder()
                .expirationTime(Instant.now().plusSeconds(10000).getEpochSecond())
                .build());
        String token = String.format("header.%s.signature", encodedJwtBody);

        JwtTokenProviderImpl objectUnderTest = new JwtTokenProviderImpl(token, dummyController, "testscope",
                "teststream");
        assertEquals(token, objectUnderTest.retrieveToken());
    }

    @Test
    public void testParseExpirationTimeExtractsExpiryTime() {
        // Contains a space before each field value
        String json1 = "{\"sub\": \"subject\",\"aud\": \"segmentstore\",\"iat\": 1569837384,\"exp\": 1569837434}";
        JwtTokenProviderImpl objectUnderTest = new JwtTokenProviderImpl(
                dummyToken(), dummyController, "some-scope", "some-stream");
        assertEquals(1569837434, objectUnderTest.parseExpirationTime(json1).longValue());

        // Does not contain space before field values
        String json2 = "{\"sub\":\"subject\",\"aud\":\"segmentstore\",\"iat\":1569837384,\"exp\":1569837434}";
        assertEquals(1569837434, objectUnderTest.parseExpirationTime(json2).longValue());
    }

    @Test
    public void testParseExpirationTimeReturnsNullWhenExpiryIsNotSet() {
        JwtTokenProviderImpl objectUnderTest = new JwtTokenProviderImpl(
                dummyToken(), dummyController, "some-scope", "some-stream");

        // Does not contain expiry time
        String json = "{\"sub\":\"subject\",\"aud\":\"segmentstore\",\"iat\":1569837384}";

        assertNull(objectUnderTest.parseExpirationTime(json));
    }

    @Test
    public void testParseExpirationTimeReturnsNullWhenTokenIsNullOrEmpty() {
        JwtTokenProviderImpl objectUnderTest = new JwtTokenProviderImpl(
                dummyToken(), dummyController, "some-scope", "some-stream");
        assertNull(objectUnderTest.parseExpirationTime(null));
        assertNull(objectUnderTest.parseExpirationTime(""));
    }

    @Test
    public void testParseExpirationTimeReturnsNullWhenTokenIsNotInteger() {
        // Notice that the exp field value contains non-digits/alphabets
        String jwtBody = "{\"sub\":\"subject\",\"aud\":\"segmentstore\",\"iat\":1569837384,\"exp\":\"abc\"}";

        JwtTokenProviderImpl objectUnderTest = new JwtTokenProviderImpl(
                dummyToken(), dummyController, "some-scope", "some-stream");
        assertNull(objectUnderTest.parseExpirationTime(jwtBody));
    }

    @Test
    public void testRetrievesNewTokenFirstTimeWhenInitialTokenIsNull() {
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
        DelegationTokenProvider objectUnderTest = new JwtTokenProviderImpl(mockController, "somescope", "somestream");

        // Act
        String token = objectUnderTest.retrieveToken();
        log.debug(token);

        assertTrue(token.startsWith("newtokenheader"));
    }

    @Test(expected = NullPointerException.class)
    public void testCtorRejectsNullControllerInput() {
        new JwtTokenProviderImpl(null, "somescope", "somestream");
    }

    @Test(expected = NullPointerException.class)
    public void testCtorRejectsNullScopeInput() {
        new JwtTokenProviderImpl(dummyController, null, "somestream");
    }

    @Test(expected = NullPointerException.class)
    public void testCtorRejectsNullStreamInput() {
        new JwtTokenProviderImpl(dummyController, "somescope", null);
    }

    @Test
    public void testPopulateTokenReturnsTrueWhenInputIsEmptyAndExistingTokenIsNull() {
        JwtTokenProviderImpl objectUnderTest = new JwtTokenProviderImpl(this.dummyController, "somescope", "somestream");
        assertTrue(objectUnderTest.populateToken(""));
    }

    @Test
    public void testPopulateTokenReturnsFalseWhenInputIsEmptyAndExistingTokenIsEmpty() {
        JwtTokenProviderImpl objectUnderTest = new JwtTokenProviderImpl(this.dummyController, "somescope", "somestream");
        objectUnderTest.populateToken("");
        assertFalse(objectUnderTest.populateToken(""));
    }

    @Test
    public void testPopulateTokenReturnsFalseWhenInputIsNull() {
        JwtTokenProviderImpl objectUnderTest = new JwtTokenProviderImpl(this.dummyController, "somescope", "somestream");
        assertFalse(objectUnderTest.populateToken(null));
    }

    @Test
    public void testPopulateTokenReturnsTrueWhenTokenIsNonEmpty() {
        String initialToken = String.format("%s.%s.%s", "base64-encoded-header",
                JwtTestUtils.createJwtBody(JwtBody.builder().expirationTime(Instant.now().getEpochSecond()).build()),
                "base64-encoded-signature");

        JwtTokenProviderImpl objectUnderTest = new JwtTokenProviderImpl(initialToken, this.dummyController,
                "somescope", "somestream");

        String newToken = String.format("%s.%s.%s", "base64-encoded-header",
                JwtTestUtils.createJwtBody(JwtBody.builder().expirationTime(Instant.now().getEpochSecond()).build()),
                "base64-encoded-signature");
        assertTrue(objectUnderTest.populateToken(newToken));
    }
}
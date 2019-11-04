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

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.impl.Controller;
import io.pravega.test.common.AssertExtensions;
import org.junit.Test;

import java.time.Instant;

import static io.pravega.client.security.auth.JwtTestUtils.createJwtBody;
import static io.pravega.client.security.auth.JwtTestUtils.dummyToken;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class DelegationTokenProviderFactoryTest {

    private Controller dummyController = mock(Controller.class);

    @Test
    public void testCreateWithEmptyToken() {
       DelegationTokenProvider tokenProvider = DelegationTokenProviderFactory.createWithEmptyToken();
       assertEquals("", tokenProvider.retrieveToken());
       assertFalse(tokenProvider.populateToken("new-token"));
    }

    @Test
    public void testCreateWithNullInputThrowsException() {
        AssertExtensions.assertThrows("Null controller argument wasn't rejected.",
                () -> DelegationTokenProviderFactory.create(null, "test-scope", "test-stream"),
                e -> e instanceof NullPointerException);

        AssertExtensions.assertThrows("Null Controller argument wasn't rejected.",
                () -> DelegationTokenProviderFactory.create(dummyToken(), null, mock(Segment.class)),
                e -> e instanceof NullPointerException);

        AssertExtensions.assertThrows("Null scopeName argument wasn't rejected.",
                () -> DelegationTokenProviderFactory.create(dummyController, null, "test-stream"),
                e -> e instanceof NullPointerException);

        AssertExtensions.assertThrows("Null streamName argument wasn't rejected.",
                () -> DelegationTokenProviderFactory.create(dummyController, "test-scope", null),
                e -> e instanceof NullPointerException);

        AssertExtensions.assertThrows("Null segment argument wasn't rejected.",
                () -> DelegationTokenProviderFactory.create(dummyController, null),
                e -> e instanceof NullPointerException);

        AssertExtensions.assertThrows("Null segment argument wasn't rejected.",
                () -> DelegationTokenProviderFactory.create(dummyToken(), dummyController, null),
                e -> e instanceof NullPointerException);
    }

    @Test
    public void testCreateWithEmptyInputThrowsException() {
        AssertExtensions.assertThrows("Empty scopeName argument wasn't rejected.",
                () -> DelegationTokenProviderFactory.create(dummyController, "", "test-stream"),
                e -> e instanceof IllegalArgumentException);

        AssertExtensions.assertThrows("Empty streamName argument wasn't rejected.",
                () -> DelegationTokenProviderFactory.create(dummyController, "test-scope", ""),
                e -> e instanceof IllegalArgumentException);
    }

    @Test
    public void testCreateWithNonJwtToken() {
        String nonJwtDelegationToken = "non-jwt-token";
        DelegationTokenProvider tokenProvider = DelegationTokenProviderFactory.create(nonJwtDelegationToken,
                dummyController, new Segment("test-scope", "test-stream", 1));
        assertEquals(nonJwtDelegationToken, tokenProvider.retrieveToken());

        String newNonJwtDelegationToken = "new-non-jwt-token";
        tokenProvider.populateToken(newNonJwtDelegationToken);
        assertEquals("new-non-jwt-token", tokenProvider.retrieveToken());
    }

    @Test
    public void testCreateWithJwtToken() {
        String jwtDelegationToken = String.format("base-64-encoded-header.%s.base-64-encoded-signature", createJwtBody(
                JwtBody.builder().expirationTime(Instant.now().plusSeconds(10000).getEpochSecond()).build()));
        DelegationTokenProvider tokenProvider = DelegationTokenProviderFactory.create(jwtDelegationToken,
                dummyController, new Segment("test-scope", "test-stream", 1));
        assertEquals(jwtDelegationToken, tokenProvider.retrieveToken());
    }

    @Test
    public void testCreateWithNullDelegationToken() {
        DelegationTokenProvider tokenProvider = DelegationTokenProviderFactory.create(null,
                dummyController, new Segment("test-scope", "test-stream", 1));
        assertTrue(tokenProvider instanceof JwtTokenProviderImpl);

        tokenProvider = DelegationTokenProviderFactory.create(dummyController, "test-scope", "test-stream");
        assertTrue(tokenProvider instanceof JwtTokenProviderImpl);
    }
}

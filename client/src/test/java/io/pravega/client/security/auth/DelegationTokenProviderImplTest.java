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
import java.time.Instant;
import org.junit.Test;

import static io.pravega.client.security.auth.JwtTestUtils.createJwtBody;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class DelegationTokenProviderImplTest {

    private Controller dummyController = mock(Controller.class);

    @Test
    public void testDefaultCtorReturnsEmptyToken() {
        assertEquals("", DelegationTokenProviderFactory.createWithEmptyToken().retrieveToken());
    }

    @Test
    public void testDefaultCtorReturnsEmptyTokenOnRefresh() {
        assertEquals("", DelegationTokenProviderFactory.createWithEmptyToken().refreshToken());
    }

    @Test
    public void testReturnsExistingTokenIfExpiryIsNotSet() {
        String token = String.format("%s.%s.%s",
                "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9", // header
                "eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ", // body
                "SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"); // signature
        DelegationTokenProviderImpl proxy = new DelegationTokenProviderImpl(token, dummyController,
                "testscope", "teststream");
        assertEquals(token, proxy.retrieveToken());
    }

    @Test
    public void testUsesEmptyTokenHandlingStrategyForEmptyToken() {
        DelegationTokenProviderImpl proxy = new DelegationTokenProviderImpl("", dummyController,
                "testscope", "teststream");
        assertTrue(proxy.getStrategy() instanceof EmptyTokenHandlingStrategy);
    }

    @Test
    public void testUsesNullTokenHandlingStrategyForNullToken() {
        DelegationTokenProviderImpl proxy = new DelegationTokenProviderImpl(null, dummyController,
                "testscope", "teststream");
        assertTrue(proxy.getStrategy() instanceof NullJwtTokenHandlingStrategy);
    }

    @Test
    public void testUsesValidTokenHandlingStrategyForValidTokenWithExpiry() {
        String token = String.format("%s.%s.%s",
                "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9", // header
                "eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ", // body
                "SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"); // signature

        DelegationTokenProviderImpl proxy = new DelegationTokenProviderImpl(token, dummyController,
                "testscope", "teststream");
        assertTrue(proxy.getStrategy() instanceof JwtTokenHandlingStrategy);
    }

    @Test
    public void testReturnsExistingTokenIfNotNearingExpiry() {
        String encodedJwtBody = createJwtBody(JwtBody.builder()
                .expirationTime(Instant.now().plusSeconds(10000).getEpochSecond())
                .build());
        String token = String.format("header.%s.signature", encodedJwtBody);

        DelegationTokenProviderImpl proxy = new DelegationTokenProviderImpl(token, dummyController, "testscope", "teststream");
        assertEquals(token, proxy.retrieveToken());
    }
}

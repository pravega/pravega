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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Slf4j
public class NullInitializedTokenProviderImplTest extends JwtTokenProviderImplTest {

    @Test
    public void testRetrievesNewTokenFirstTime() {
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
        NullInitializedTokenProviderImpl objectUnderTest = new NullInitializedTokenProviderImpl(mockController,
                "somescope", "somestream");

        // Act
        String token = objectUnderTest.retrieveToken();
        log.debug(token);

        assertTrue(token.startsWith("newtokenheader"));
    }
}

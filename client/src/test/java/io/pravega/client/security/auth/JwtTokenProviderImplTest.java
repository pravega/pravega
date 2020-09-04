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

import io.pravega.client.ClientConfig;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.common.util.RetriesExhaustedException;
import java.net.URI;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import io.pravega.test.common.JwtBody;
import io.pravega.test.common.JwtTestUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import static io.pravega.test.common.JwtTestUtils.createEmptyDummyToken;
import static io.pravega.test.common.JwtTestUtils.toCompact;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Slf4j
public class JwtTokenProviderImplTest {

    private Controller dummyController = mock(Controller.class);

    @Test
    public void testIsWithinThresholdForRefresh() {
        JwtTokenProviderImpl objectUnderTest = new JwtTokenProviderImpl(
                createEmptyDummyToken(), dummyController, "somescope", "somestream");

        assertFalse(objectUnderTest.isWithinRefreshThreshold(Instant.ofEpochSecond(10), Instant.ofEpochSecond(30)));
        assertFalse(objectUnderTest.isWithinRefreshThreshold(Instant.now(), Instant.now().plusSeconds(11)));
        assertTrue(objectUnderTest.isWithinRefreshThreshold(Instant.ofEpochSecond(50), Instant.ofEpochSecond(40)));
        assertTrue(objectUnderTest.isWithinRefreshThreshold(Instant.ofEpochSecond(50), Instant.ofEpochSecond(55)));
        assertTrue(objectUnderTest.isWithinRefreshThreshold(Instant.ofEpochSecond(50), Instant.ofEpochSecond(53)));
    }

    @Test
    public void testRetrievesSameTokenPassedDuringConstruction() {
        String token = String.format("header.%s.signature", toCompact(
                JwtBody.builder().expirationTime(Instant.now().plusSeconds(10000).getEpochSecond()).build()));
        JwtTokenProviderImpl objectUnderTest = new JwtTokenProviderImpl(
                token, mock(Controller.class), "somescope", "somestream");
        assertEquals(token, objectUnderTest.retrieveToken().join());
    }

    @Test
    public void testRetrievesNewTokenIfTokenIsNearingExpiry() {
        String token = String.format("header.%s.signature", toCompact(
                JwtBody.builder().expirationTime(Instant.now().minusSeconds(1).getEpochSecond()).build()));
        log.debug("token: {}", token);

        // Setup mock
        Controller mockController = mock(Controller.class);
        CompletableFuture<String> future = CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                return String.format("newtokenheader.%s.signature", toCompact(
                        JwtBody.builder().expirationTime(Instant.now().plusSeconds(10000).getEpochSecond()).build()));
            }
        });
        when(mockController.getOrRefreshDelegationTokenFor("somescope", "somestream"))
                .thenReturn(future);

        // Setup the object under test
        JwtTokenProviderImpl objectUnderTest = new JwtTokenProviderImpl(
                token, mockController, "somescope", "somestream");

        // Act
        String newToken = objectUnderTest.retrieveToken().join();
        log.debug("new token: {}", newToken);

        assertTrue(newToken.startsWith("newtokenheader"));
    }

    @Test
    public void testRetrievesNewTokenIfSignalledOfTokenExpiry() {
        final String token = String.format("newtokenheader.%s.signature", toCompact(
                JwtBody.builder().expirationTime(Instant.now().plusSeconds(100000).getEpochSecond()).build()));
        // Setup mock
        Controller mockController = mock(Controller.class);
        CompletableFuture<String> future = CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                return token + System.currentTimeMillis();
            }
        });
        when(mockController.getOrRefreshDelegationTokenFor("somescope", "somestream"))
                .thenReturn(future);

        // Setup the object under test
        DelegationTokenProvider objectUnderTest = new JwtTokenProviderImpl(mockController,
                "somescope", "somestream");

        String firstToken = objectUnderTest.retrieveToken().join();
        String secondToken = objectUnderTest.retrieveToken().join();
        assertEquals(firstToken, secondToken);

        objectUnderTest.signalTokenExpired();
        assertEquals(firstToken, objectUnderTest.retrieveToken().join());
    }

    @Test
    public void testRetrievesSameTokenOutsideOfTokenRefreshThresholdWhenTokenIsNull() {
        final String token = String.format("newtokenheader.%s.signature", toCompact(
                JwtBody.builder().expirationTime(Instant.now().plusSeconds(10000).getEpochSecond()).build()));
        // Setup mock
        Controller mockController = mock(Controller.class);
        CompletableFuture<String> future = CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                return token;
            }
        });
        when(mockController.getOrRefreshDelegationTokenFor("somescope", "somestream"))
                .thenReturn(future);

        // Setup the object under test
        DelegationTokenProvider objectUnderTest = new JwtTokenProviderImpl(mockController,
                "somescope", "somestream");

        assertEquals(token, objectUnderTest.retrieveToken().join());
    }

    @Test
    public void testDefaultTokenRefreshThreshold() {
        JwtTokenProviderImpl objectUnderTest = new JwtTokenProviderImpl(
                createEmptyDummyToken(), dummyController, "some-scope", "some-stream");
        assertSame(JwtTokenProviderImpl.DEFAULT_REFRESH_THRESHOLD_SECONDS,
                objectUnderTest.getRefreshThresholdInSeconds());
    }

    @Test
    public void testReturnsExistingTokenIfNotNearingExpiry() {
        String encodedJwtBody = toCompact(JwtBody.builder()
                .expirationTime(Instant.now().plusSeconds(10000).getEpochSecond())
                .build());
        String token = String.format("header.%s.signature", encodedJwtBody);

        JwtTokenProviderImpl objectUnderTest = new JwtTokenProviderImpl(token, dummyController, "testscope",
                "teststream");
        assertEquals(token, objectUnderTest.retrieveToken().join());
    }

    @Test
    public void testRetrievesNewTokenFirstTimeWhenInitialTokenIsNull() {
        // Setup mock
        Controller mockController = mock(Controller.class);
        CompletableFuture<String> future = CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                return String.format("newtokenheader.%s.signature", toCompact(
                        JwtBody.builder().expirationTime(Instant.now().plusSeconds(10000).getEpochSecond()).build()));
            }
        });
        when(mockController.getOrRefreshDelegationTokenFor("somescope", "somestream"))
                .thenReturn(future);

        // Setup the object under test
        DelegationTokenProvider objectUnderTest = new JwtTokenProviderImpl(mockController, "somescope", "somestream");

        // Act
        String token = objectUnderTest.retrieveToken().join();
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
                JwtTestUtils.toCompact(JwtBody.builder().expirationTime(Instant.now().getEpochSecond()).build()),
                "base64-encoded-signature");

        JwtTokenProviderImpl objectUnderTest = new JwtTokenProviderImpl(initialToken, this.dummyController,
                "somescope", "somestream");

        String newToken = String.format("%s.%s.%s", "base64-encoded-header",
                JwtTestUtils.toCompact(JwtBody.builder().expirationTime(Instant.now().getEpochSecond()).build()),
                "base64-encoded-signature");
        assertTrue(objectUnderTest.populateToken(newToken));
    }

    @Test(expected = CompletionException.class)
    public void testRetrieveTokenFailsWhenClientCallToControllerFails() {
        Controller mockController = mock(Controller.class);

        CompletableFuture<String> tokenFuture = new CompletableFuture<>();
        tokenFuture.completeExceptionally(new CompletionException(new RuntimeException("Failed to connect to server")));

        when(mockController.getOrRefreshDelegationTokenFor("test-scope", "test-stream"))
                .thenReturn(tokenFuture);
        DelegationTokenProvider tokenProvider = DelegationTokenProviderFactory.create(mockController, "test-scope", "test-stream");
        try {
            tokenProvider.retrieveToken().join();
        } catch (CompletionException e) {
            assertEquals(RuntimeException.class.getName(), e.getCause().getClass().getName());
            throw e;
        }
    }

    @Test(expected = CompletionException.class)
    public void testRefreshTokenCompletesUponFailure() {
        ClientConfig config = ClientConfig.builder().controllerURI(URI.create("tcp://non-existent-cluster:9090")).build();
        Controller controllerClient = new ControllerImpl(
                ControllerImplConfig.builder().clientConfig(config).retryAttempts(1).build(),
                Executors.newScheduledThreadPool(1));

        DelegationTokenProvider tokenProvider = DelegationTokenProviderFactory.create(controllerClient, "bob-0", "bob-0");
        try {
            tokenProvider.retrieveToken().join();
        } catch (CompletionException e) {
            assertEquals(RetriesExhaustedException.class.getName(), e.getCause().getClass().getName());
            throw e;
        }
    }

    @Test
    public void testTokenRefreshFutureIsClearedUponFailure() throws InterruptedException {
        ClientConfig config = ClientConfig.builder().controllerURI(
                URI.create("tcp://non-existent-cluster:9090")).build();

        Controller controllerClient = new ControllerImpl(
                ControllerImplConfig.builder().clientConfig(config).retryAttempts(1).build(),
                Executors.newScheduledThreadPool(1));

        JwtTokenProviderImpl tokenProvider = (JwtTokenProviderImpl) DelegationTokenProviderFactory.create(controllerClient,
                "bob-0", "bob-0");

        try {
            String token = tokenProvider.retrieveToken().join();
            fail("Didn't expect the control to come here");
        } catch (CompletionException e) {
            log.info("Encountered CompletionException as expected");
            assertNull("Expected a null tokenRefreshFuture", tokenProvider.getTokenRefreshFuture().get());
        }
        try {
            tokenProvider.retrieveToken().join();
        } catch (CompletionException e) {
            log.info("Encountered CompletionException as expected");
            assertNull("Expected a null tokenRefreshFuture", tokenProvider.getTokenRefreshFuture().get());
        }
    }
}
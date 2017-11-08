/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Stream;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
@Slf4j
public class TxnKeepAliveTest {

    private EventWriterConfig config;
    private Stream stream;
    @Mock
    private Controller controller;
    @Spy
    private ScheduledExecutorService executor;
    @Mock
    private ScheduledFuture<Void> future;

    @Before
    public void setUp() throws Exception {
        config = EventWriterConfig.builder().build();
        stream = new StreamImpl("testScope", "testStream");

        when(controller.pingTransaction(eq(stream), any(UUID.class), anyLong())).thenReturn(CompletableFuture
                .completedFuture(null));
        when(executor.scheduleAtFixedRate(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)))
                .thenAnswer(invocation -> {
                    Runnable runnable = (Runnable) invocation.getArgument(0);
                    runnable.run();
                    return future;
                });
    }

    @After
    public void tearDown() throws Exception {
        executor.shutdownNow();
    }

    @Test
    public void startTxnKeepAlive() throws Exception {
        final UUID txnID = UUID.randomUUID();
        @Cleanup
        TxnKeepAlive txnKeepAlive = new TxnKeepAlive(config, stream, controller, executor);

        txnKeepAlive.startTxnKeepAlive(txnID);
        long expectedKeepAliveInterval = (long) (0.7 * config.getTransactionTimeoutTime());
        verify(executor, times(1)).scheduleAtFixedRate(any(Runnable.class), anyLong(),
                eq(expectedKeepAliveInterval), eq(TimeUnit.MILLISECONDS));
        verify(controller, times(1)).pingTransaction(eq(stream), eq(txnID), eq(config.getTransactionTimeoutTime()));
    }

    @Test
    public void startTxnKeepAliveWithLowLeaseValue() {
        final UUID txnID = UUID.randomUUID();
        final EventWriterConfig smallTxnLeaseTime = EventWriterConfig.builder()
                                                                     .transactionTimeoutTime(SECONDS.toMillis(10))
                                                                     .build();

        TxnKeepAlive txnKeepAlive = new TxnKeepAlive(smallTxnLeaseTime, stream, controller, executor);
        txnKeepAlive.startTxnKeepAlive(txnID);

        verify(executor, times(1)).scheduleAtFixedRate(any(Runnable.class), anyLong(),
                eq(SECONDS.toMillis(10)), eq(TimeUnit.MILLISECONDS));
        verify(controller, times(1)).pingTransaction(eq(stream), eq(txnID),
                eq(smallTxnLeaseTime.getTransactionTimeoutTime()));
    }

    @Test
    public void startTxnKeepAliveError() throws Exception {
        final UUID txnID = UUID.randomUUID();

        CompletableFuture<Void> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Error"));
        when(controller.pingTransaction(eq(stream), eq(txnID), anyLong())).thenReturn(failedFuture);

        @Cleanup
        TxnKeepAlive txnKeepAlive = new TxnKeepAlive(config, stream, controller, executor);
        txnKeepAlive.startTxnKeepAlive(txnID);

        long expectedKeepAliveInterval = (long) (0.7 * config.getTransactionTimeoutTime());
        verify(executor, times(1)).scheduleAtFixedRate(any(Runnable.class), anyLong(),
                eq(expectedKeepAliveInterval), eq(TimeUnit.MILLISECONDS));
        verify(controller, times(1)).pingTransaction(eq(stream), eq(txnID), eq(config.getTransactionTimeoutTime()));
    }

    @Test
    public void startTxnKeepAliveMultiple() throws Exception {
        final UUID txnID1 = UUID.randomUUID();
        final UUID txnID2 = UUID.randomUUID();
        @Cleanup
        TxnKeepAlive txnKeepAlive = new TxnKeepAlive(config, stream, controller, executor);

        txnKeepAlive.startTxnKeepAlive(txnID1);
        txnKeepAlive.startTxnKeepAlive(txnID2);
        long expectedKeepAliveInterval = (long) (0.7 * config.getTransactionTimeoutTime());
        verify(executor, times(1)).scheduleAtFixedRate(any(Runnable.class), anyLong(),
                eq(expectedKeepAliveInterval), eq(TimeUnit.MILLISECONDS));
    }
}

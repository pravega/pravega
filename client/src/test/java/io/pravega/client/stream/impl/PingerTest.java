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
package io.pravega.client.stream.impl;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.Transaction;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.test.common.InlineExecutor;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.longThat;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
@Slf4j
public class PingerTest {

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
    public void tearDown() {
        ExecutorServiceHelpers.shutdown(executor);
    }

    @Test
    public void startTxnKeepAlive() throws Exception {
        final UUID txnID = UUID.randomUUID();
        @Cleanup
        Pinger pinger = new Pinger(config.getTransactionTimeoutTime(), stream, controller, executor);

        pinger.startPing(txnID);
        verify(executor, times(1)).scheduleAtFixedRate(any(Runnable.class), anyLong(),
                longThat(i -> i <= config.getTransactionTimeoutTime()), eq(TimeUnit.MILLISECONDS));
        verify(controller, times(1)).pingTransaction(eq(stream), eq(txnID), eq(config.getTransactionTimeoutTime()));
    }

    @Test
    public void startTxnKeepAliveWithLowLeaseValue() {
        final UUID txnID = UUID.randomUUID();
        final EventWriterConfig smallTxnLeaseTime = EventWriterConfig.builder()
                                                                     .transactionTimeoutTime(SECONDS.toMillis(10))
                                                                     .build();
        @Cleanup
        Pinger pinger = new Pinger(smallTxnLeaseTime.getTransactionTimeoutTime(), stream, controller, executor);
        pinger.startPing(txnID);

        verify(executor, times(1)).scheduleAtFixedRate(any(Runnable.class), anyLong(),
                longThat(l -> l > 0 && l <= 10000), eq(TimeUnit.MILLISECONDS));
        verify(controller, times(1)).pingTransaction(eq(stream), eq(txnID),
                eq(smallTxnLeaseTime.getTransactionTimeoutTime()));
    }

    @Test
    public void startTxnKeepAliveError() throws Exception {
        final UUID txnID = UUID.randomUUID();

        CompletableFuture<Transaction.PingStatus> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Error"));
        when(controller.pingTransaction(eq(stream), eq(txnID), anyLong())).thenReturn(failedFuture);

        @Cleanup
        Pinger pinger = new Pinger(config.getTransactionTimeoutTime(), stream, controller, executor);
        pinger.startPing(txnID);

        verify(executor, times(1)).scheduleAtFixedRate(any(Runnable.class), anyLong(),
                                                       longThat(l -> l > 0 && l <= 50000), eq(TimeUnit.MILLISECONDS));
        verify(controller, times(1)).pingTransaction(eq(stream), eq(txnID), eq(config.getTransactionTimeoutTime()));
    }

    @Test
    public void startTxnKeepAliveMultiple() throws Exception {
        final UUID txnID1 = UUID.randomUUID();
        final UUID txnID2 = UUID.randomUUID();
        @Cleanup
        Pinger pinger = new Pinger(config.getTransactionTimeoutTime(), stream, controller, executor);

        pinger.startPing(txnID1);
        pinger.startPing(txnID2);
        verify(executor, times(1)).scheduleAtFixedRate(any(Runnable.class), anyLong(),
                                                       longThat(l -> l > 0 && l <= 50000), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testPingWithStatus() {

        long transactionTimeoutTime = 500;
        final UUID txnID1 = UUID.randomUUID();
        final UUID txnID2 = UUID.randomUUID();
        final UUID txnID3 = UUID.randomUUID();
        final UUID txnID4 = UUID.randomUUID();
        final UUID txnID5 = UUID.randomUUID();

        @Cleanup("shutdown")
        InlineExecutor pingExecutor = new InlineExecutor();

        //Setup mock to return different
        when(controller.pingTransaction(any(Stream.class), eq(txnID1), anyLong()))
                .thenReturn(CompletableFuture.<Transaction.PingStatus>completedFuture(Transaction.PingStatus.ABORTED));
        when(controller.pingTransaction(any(Stream.class), eq(txnID2), anyLong()))
                .thenReturn(CompletableFuture.<Transaction.PingStatus>completedFuture(Transaction.PingStatus.COMMITTED));
        when(controller.pingTransaction(any(Stream.class), eq(txnID3), anyLong()))
                .thenReturn(CompletableFuture.<Transaction.PingStatus>completedFuture(Transaction.PingStatus.OPEN));
        CompletableFuture<Transaction.PingStatus> failedPingFuture = new CompletableFuture<>();
        failedPingFuture.completeExceptionally(new RuntimeException("error"));
        when(controller.pingTransaction(any(Stream.class), eq(txnID4), anyLong()))
                .thenReturn(failedPingFuture);
        when(controller.pingTransaction(eq(stream), eq(txnID5), anyLong()))
                .thenReturn(Futures.failedFuture(new StatusRuntimeException(Status.NOT_FOUND)));

        @Cleanup
        Pinger pinger = new Pinger(transactionTimeoutTime, stream, controller, pingExecutor);

        pinger.startPing(txnID1);
        pinger.startPing(txnID2);
        pinger.startPing(txnID3);
        pinger.startPing(txnID4);
        pinger.startPing(txnID5);

        verify(controller, timeout(1000)).pingTransaction(eq(stream), eq(txnID1), eq(transactionTimeoutTime));
        verify(controller, timeout(1000)).pingTransaction(eq(stream), eq(txnID2), eq(transactionTimeoutTime));
        verify(controller, timeout(1000)).pingTransaction(eq(stream), eq(txnID3), eq(transactionTimeoutTime));
        verify(controller, timeout(1000)).pingTransaction(eq(stream), eq(txnID4), eq(transactionTimeoutTime));
        verify(controller, timeout(1000)).pingTransaction(eq(stream), eq(txnID5), eq(transactionTimeoutTime));
        assertEquals(3, pinger.getCompletedTxns().size());
    }
}

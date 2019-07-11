/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.handler;

import io.netty.buffer.Unpooled;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ReusableLatch;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.host.stat.SegmentStatsRecorder;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.ByteBufWrapper;
import io.pravega.shared.protocol.netty.FailingRequestProcessor;
import io.pravega.shared.protocol.netty.WireCommands.AppendSetup;
import io.pravega.shared.protocol.netty.WireCommands.ConditionalCheckFailed;
import io.pravega.shared.protocol.netty.WireCommands.DataAppended;
import io.pravega.shared.protocol.netty.WireCommands.OperationUnsupported;
import io.pravega.shared.protocol.netty.WireCommands.SetupAppend;
import io.pravega.test.common.IntentionalException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import static io.pravega.segmentstore.contracts.Attributes.EVENT_COUNT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AppendProcessorTest {
    private final long requestId = 1L;

    @Test
    public void testAppend() throws Exception {
        String streamSegmentName = "scope/stream/0.#epoch.0";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        val mockedRecorder = Mockito.mock(SegmentStatsRecorder.class);
        AppendProcessor processor = new AppendProcessor(store, connection, new FailingRequestProcessor(), mockedRecorder, null, false);

        setupGetAttributes(streamSegmentName, clientId, store);
        val ac = interceptAppend(store, streamSegmentName, updateEventNumber(clientId, data.length), CompletableFuture.completedFuture(null));

        processor.setupAppend(new SetupAppend(1, clientId, streamSegmentName, ""));
        processor.append(new Append(streamSegmentName, clientId, data.length, 1, Unpooled.wrappedBuffer(data), null, requestId));
        verify(store).getAttributes(anyString(), eq(Collections.singleton(clientId)), eq(true), eq(AppendProcessor.TIMEOUT));
        verifyStoreAppend(ac, data);
        verify(connection).send(new AppendSetup(1, streamSegmentName, clientId, 0));
        verify(connection, atLeast(0)).resumeReading();
        verify(connection).send(new DataAppended(requestId, clientId, data.length, 0L));
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);

        verify(mockedRecorder).recordAppend(eq(streamSegmentName), eq(8L), eq(1), any());
    }

    @Test
    public void testTransactionAppend() throws Exception {
        String streamSegmentName = "scope/stream/transactionSegment#transaction.01234567890123456789012345678901";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        val mockedRecorder = Mockito.mock(SegmentStatsRecorder.class);
        AppendProcessor processor = new AppendProcessor(store, connection, new FailingRequestProcessor(), mockedRecorder, null, false);

        setupGetAttributes(streamSegmentName, clientId, store);
        val ac = interceptAppend(store, streamSegmentName, updateEventNumber(clientId, data.length), CompletableFuture.completedFuture(null));
        processor.setupAppend(new SetupAppend(requestId, clientId, streamSegmentName, ""));
        processor.append(new Append(streamSegmentName, clientId, data.length, 1, Unpooled.wrappedBuffer(data), null, requestId));
        verify(store).getAttributes(anyString(), eq(Collections.singleton(clientId)), eq(true), eq(AppendProcessor.TIMEOUT));
        verifyStoreAppend(ac, data);
        verify(connection).send(new AppendSetup(requestId, streamSegmentName, clientId, 0));
        verify(connection, atLeast(0)).resumeReading();
        verify(connection).send(new DataAppended(requestId, clientId, data.length, 0L));
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);

        verify(mockedRecorder).recordAppend(eq(streamSegmentName), eq(8L), eq(1), any());
    }

    @Test
    public void testSwitchingSegment() {
        String streamSegmentName1 = "scope/stream/testAppendSegment1";
        String streamSegmentName2 = "scope/stream/testAppendSegment2";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        InOrder verifier = Mockito.inOrder(store);
        ServerConnection connection = mock(ServerConnection.class);
        AppendProcessor processor = new AppendProcessor(store, connection, new FailingRequestProcessor(), null);

        setupGetAttributes(streamSegmentName1, clientId, store);
        processor.setupAppend(new SetupAppend(1, clientId, streamSegmentName1, ""));
        verifier.verify(store).getAttributes(anyString(), eq(Collections.singleton(clientId)), eq(true), eq(AppendProcessor.TIMEOUT));

        val ac1 = interceptAppend(store, streamSegmentName1, updateEventNumber(clientId, 10), CompletableFuture.completedFuture(null));
        processor.append(new Append(streamSegmentName1, clientId, 10, 1, Unpooled.wrappedBuffer(data), null, requestId));
        verifyStoreAppend(verifier, ac1, data);

        setupGetAttributes(streamSegmentName2, clientId, store);
        processor.setupAppend(new SetupAppend(1, clientId, streamSegmentName2, ""));
        verifier.verify(store).getAttributes(anyString(), eq(Collections.singleton(clientId)), eq(true), eq(AppendProcessor.TIMEOUT));

        val ac2 = interceptAppend(store, streamSegmentName2, updateEventNumber(clientId, 2000), CompletableFuture.completedFuture(null));
        processor.append(new Append(streamSegmentName2, clientId, 2000, 1, Unpooled.wrappedBuffer(data), null, requestId));
        verifyStoreAppend(verifier, ac2, data);

        val ac3 = interceptAppend(store, streamSegmentName1, updateEventNumber(clientId, 20, 10, 1), CompletableFuture.completedFuture(null));
        processor.append(new Append(streamSegmentName1, clientId, 20, 1, Unpooled.wrappedBuffer(data), null, requestId));
        verifyStoreAppend(verifier, ac3, data);

        verifyNoMoreInteractions(store);
    }

    @Test
    public void testConditionalAppendSuccess() throws Exception {
        String streamSegmentName = "scope/stream/testConditionalAppendSuccess";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        val mockedRecorder = Mockito.mock(SegmentStatsRecorder.class);
        AppendProcessor processor = new AppendProcessor(store, connection, new FailingRequestProcessor(), mockedRecorder, null, false);

        setupGetAttributes(streamSegmentName, clientId, store);
        val ac1 = interceptAppend(store, streamSegmentName, updateEventNumber(clientId, 1), CompletableFuture.completedFuture(null));
        processor.setupAppend(new SetupAppend(1, clientId, streamSegmentName, ""));
        processor.append(new Append(streamSegmentName, clientId, 1, 1, Unpooled.wrappedBuffer(data), null, requestId));

        val ac2 = interceptAppend(store, streamSegmentName, data.length, updateEventNumber(clientId, 2, 1, 1), CompletableFuture.completedFuture(null));
        processor.append(new Append(streamSegmentName, clientId, 2, 1, Unpooled.wrappedBuffer(data), (long) data.length, requestId));
        verify(store).getAttributes(anyString(), eq(Collections.singleton(clientId)), eq(true), eq(AppendProcessor.TIMEOUT));
        verifyStoreAppend(ac1, data);
        verifyStoreAppend(ac2, data);
        verify(connection).send(new AppendSetup(1, streamSegmentName, clientId, 0));
        verify(connection, atLeast(0)).resumeReading();
        verify(connection).send(new DataAppended(requestId, clientId, 1, 0));
        verify(connection).send(new DataAppended(requestId, clientId, 2, 1));
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
        verify(mockedRecorder, times(2)).recordAppend(eq(streamSegmentName), eq(8L), eq(1), any());
    }

    @Test
    public void testConditionalAppendFailure() throws Exception {
        String streamSegmentName = "scope/stream/testConditionalAppendFailure";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        val mockedRecorder = Mockito.mock(SegmentStatsRecorder.class);
        AppendProcessor processor = new AppendProcessor(store, connection, new FailingRequestProcessor(), mockedRecorder, null, false);

        setupGetAttributes(streamSegmentName, clientId, store);
        val ac1 = interceptAppend(store, streamSegmentName, updateEventNumber(clientId, 1), CompletableFuture.completedFuture(null));

        processor.setupAppend(new SetupAppend(1, clientId, streamSegmentName, ""));
        processor.append(new Append(streamSegmentName, clientId, 1, 1, Unpooled.wrappedBuffer(data), null, requestId));

        val ac2 = interceptAppend(store, streamSegmentName, 0, updateEventNumber(clientId, 2, 1, 1),
                Futures.failedFuture(new BadOffsetException(streamSegmentName, data.length, 0)));

        processor.append(new Append(streamSegmentName, clientId, 2, 1, Unpooled.wrappedBuffer(data), 0L, requestId));
        verify(store).getAttributes(anyString(), eq(Collections.singleton(clientId)), eq(true), eq(AppendProcessor.TIMEOUT));
        verifyStoreAppend(ac1, data);
        verifyStoreAppend(ac2, data);
        verify(connection).send(new AppendSetup(1, streamSegmentName, clientId, 0));
        verify(connection, atLeast(0)).resumeReading();
        verify(connection).send(new DataAppended(requestId, clientId, 1, 0));
        verify(connection).send(new ConditionalCheckFailed(clientId, 2, requestId));
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
        verify(mockedRecorder).recordAppend(eq(streamSegmentName), eq(8L), eq(1), any());
    }

    @Test
    public void testInvalidOffset() {
        String streamSegmentName = "scope/stream/testAppendSegment";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        AppendProcessor processor = new AppendProcessor(store, connection, new FailingRequestProcessor(), null);

        setupGetAttributes(streamSegmentName, clientId, 100, store);
        processor.setupAppend(new SetupAppend(1, clientId, streamSegmentName, ""));
        try {
            processor.append(new Append(streamSegmentName, clientId, data.length, 1, Unpooled.wrappedBuffer(data), null, requestId));
            fail();
        } catch (RuntimeException e) {
            //expected
        }
        verify(store).getAttributes(anyString(), eq(Collections.singleton(clientId)), eq(true), eq(AppendProcessor.TIMEOUT));
        verify(connection).send(new AppendSetup(1, streamSegmentName, clientId, 100));
        verify(connection, atLeast(0)).resumeReading();
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
    }

    @Test
    public void testSetupSkipped() {
        String streamSegmentName = "scope/stream/testAppendSegment";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        AppendProcessor processor = new AppendProcessor(store, connection, new FailingRequestProcessor(), null);
        try {
            processor.append(new Append(streamSegmentName, clientId, data.length, 1, Unpooled.wrappedBuffer(data), null, requestId));
            fail();
        } catch (RuntimeException e) {
            //expected
        }
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
    }

    @Test
    public void testSwitchingStream() {
        String segment1 = "scope/stream/segment1";
        String segment2 = "scope/stream/segment2";
        UUID clientId1 = UUID.randomUUID();
        UUID clientId2 = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        AppendProcessor processor = new AppendProcessor(store, connection, new FailingRequestProcessor(), null);

        setupGetAttributes(segment1, clientId1, store);
        val ac1 = interceptAppend(store, segment1, updateEventNumber(clientId1, data.length), CompletableFuture.completedFuture(null));

        setupGetAttributes(segment2, clientId2, store);
        val ac2 = interceptAppend(store, segment2, updateEventNumber(clientId2, data.length), CompletableFuture.completedFuture(null));

        processor.setupAppend(new SetupAppend(requestId, clientId1, segment1, ""));
        processor.append(new Append(segment1, clientId1, data.length, 1, Unpooled.wrappedBuffer(data), null, requestId));
        processor.setupAppend(new SetupAppend(requestId, clientId2, segment2, ""));
        processor.append(new Append(segment2, clientId2, data.length, 1, Unpooled.wrappedBuffer(data), null, requestId));

        verify(store).getAttributes(eq(segment1), eq(Collections.singleton(clientId1)), eq(true), eq(AppendProcessor.TIMEOUT));
        verifyStoreAppend(ac1, data);

        verify(store).getAttributes(eq(segment2), eq(Collections.singleton(clientId2)), eq(true), eq(AppendProcessor.TIMEOUT));
        verifyStoreAppend(ac2, data);
        verify(connection, atLeast(0)).resumeReading();
        verify(connection).send(new AppendSetup(requestId, segment1, clientId1, 0));
        verify(connection).send(new DataAppended(requestId, clientId1, data.length, 0));
        verify(connection).send(new AppendSetup(requestId, segment2, clientId2, 0));
        verify(connection).send(new DataAppended(requestId, clientId2, data.length, 0));
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
    }

    @Test
    public void testAppendFails() throws Exception {
        String streamSegmentName = "scope/stream/testAppendSegment";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        val mockedRecorder = mock(SegmentStatsRecorder.class);
        AppendProcessor processor = new AppendProcessor(store, connection, new FailingRequestProcessor(), mockedRecorder, null, false);

        setupGetAttributes(streamSegmentName, clientId, store);
        interceptAppend(store, streamSegmentName, updateEventNumber(clientId, data.length), Futures.failedFuture(new IntentionalException()));

        processor.setupAppend(new SetupAppend(1, clientId, streamSegmentName, ""));
        processor.append(new Append(streamSegmentName, clientId, data.length, 1, Unpooled.wrappedBuffer(data), null, requestId));
        try {
            processor.append(new Append(streamSegmentName, clientId, data.length * 2, 1, Unpooled.wrappedBuffer(data), null, requestId));
            fail();
        } catch (IllegalStateException e) {
            // Expected
        }
        verify(connection).send(new AppendSetup(1, streamSegmentName, clientId, 0));
        verify(connection, atLeast(0)).resumeReading();
        verify(connection).close();
        verify(store, atMost(1)).append(any(), any(), any(), any());
        verifyNoMoreInteractions(connection);

        verify(mockedRecorder, never()).recordAppend(eq(streamSegmentName), eq(8L), eq(1), any());
    }

    @Test
    public void testEventNumbers() {
        String streamSegmentName = "scope/stream/testAppendSegment";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        AppendProcessor processor = new AppendProcessor(store, connection, new FailingRequestProcessor(), null);

        when(store.getAttributes(streamSegmentName, Collections.singleton(clientId), true, AppendProcessor.TIMEOUT))
                .thenReturn(CompletableFuture.completedFuture(Collections.emptyMap()));
        processor.setupAppend(new SetupAppend(1, clientId, streamSegmentName, ""));
        verify(store).getAttributes(streamSegmentName, Collections.singleton(clientId), true, AppendProcessor.TIMEOUT);

        int eventCount = 100;
        val ac1 = interceptAppend(store, streamSegmentName,
                updateEventNumber(clientId, 100, Attributes.NULL_ATTRIBUTE_VALUE, eventCount), CompletableFuture.completedFuture(null));
        processor.append(new Append(streamSegmentName, clientId, 100, eventCount, Unpooled.wrappedBuffer(data), null, requestId));
        verifyStoreAppend(ac1, data);

        val ac2 = interceptAppend(store, streamSegmentName, updateEventNumber(clientId, 200, 100, eventCount),
                CompletableFuture.completedFuture(null));
        processor.append(new Append(streamSegmentName, clientId, 200, eventCount, Unpooled.wrappedBuffer(data), null, requestId));
        verifyStoreAppend(ac2, data);

        verifyNoMoreInteractions(store);
    }

    /**
     * Test to ensure newer appends are processed only after successfully sending the DataAppended acknowledgement
     * back to client. This test tests the following:
     * - If sending first DataAppended is blocked, ensure future appends are not written to store.
     * - Once the first DataAppended is sent ensure the remaining appends are written to store and DataAppended ack'ed
     * back.
     */
    @Test(timeout = 15 * 1000)
    public void testDelayedDataAppended() throws Exception {
        ReusableLatch firstStoreAppendInvoked = new ReusableLatch();
        ReusableLatch completeFirstDataAppendedAck = new ReusableLatch();
        ReusableLatch secondStoreAppendInvoked = new ReusableLatch();
        @Cleanup("shutdownNow")
        ScheduledExecutorService nettyExecutor = ExecutorServiceHelpers.newScheduledThreadPool(1, "Netty-threadPool");

        String streamSegmentName = "scope/stream/testDelayedAppend";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[]{1, 2, 3, 4, 6, 7, 8, 9};
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);

        //Ensure the first DataAppended is hung/delayed.
        doAnswer(invocation -> {
            firstStoreAppendInvoked.release();
            completeFirstDataAppendedAck.await(); // wait, simulating a hung/delayed dataAppended acknowledgement.
            return null;
        }).doAnswer( invocation -> {
            secondStoreAppendInvoked.release();
            return null;
        }).when(connection).send(any(DataAppended.class));

        AppendProcessor processor = new AppendProcessor(store, connection, new FailingRequestProcessor(), null);

        when(store.getAttributes(streamSegmentName, Collections.singleton(clientId), true, AppendProcessor.TIMEOUT))
                .thenReturn(CompletableFuture.completedFuture(Collections.emptyMap()));

        processor.setupAppend(new SetupAppend(1, clientId, streamSegmentName, ""));
        verify(store).getAttributes(streamSegmentName, Collections.singleton(clientId), true, AppendProcessor.TIMEOUT);

        int eventCount = 100;
        val ac1 = interceptAppend(store, streamSegmentName,
                updateEventNumber(clientId, 100, Attributes.NULL_ATTRIBUTE_VALUE, eventCount),
                CompletableFuture.completedFuture(null));

        //Trigger the first append, here the sending of DataAppended ack will be delayed/hung.
        nettyExecutor.submit(() -> processor.append(new Append(streamSegmentName, clientId, 100, eventCount, Unpooled
                .wrappedBuffer(data), null, requestId)));
        firstStoreAppendInvoked.await();
        verifyStoreAppend(ac1, data);

        /* Trigger the next append. This should be completed immediately and should not cause a store.append to be
        invoked as the previous DataAppended ack is still not sent. */
        processor.append(new Append(streamSegmentName, clientId, 200, eventCount, Unpooled.wrappedBuffer(data), null, requestId));

        //Since the first Ack was never sent the next append should not be written to the store.
        verifyNoMoreInteractions(store);

        //Setup mock for check behaviour after the delayed/hung dataAppended completes.
        val ac2 = interceptAppend(store, streamSegmentName, updateEventNumber(clientId, 200, 100, eventCount),
                CompletableFuture.completedFuture(null));
        completeFirstDataAppendedAck.release(); //Now ensure the dataAppended sent
        secondStoreAppendInvoked.await(); // wait until the next store append is invoked.

        //Verify that the next store append invoked.
        verifyStoreAppend(ac2, data);
        //Verify two DataAppended acks are sent out.
        verify(connection, times(2)).send(any(DataAppended.class));
        verify(connection).send(new DataAppended(requestId, clientId, 100, Long.MIN_VALUE));
        verify(connection).send(new DataAppended(requestId, clientId, 200, 100));
    }

    @Test
    public void testEventNumbersOldClient() {
        String streamSegmentName = "scope/stream/testAppendSegment";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        AppendProcessor processor = new AppendProcessor(store, connection, new FailingRequestProcessor(), null);

        when(store.getAttributes(streamSegmentName, Collections.singleton(clientId), true, AppendProcessor.TIMEOUT))
                .thenReturn(CompletableFuture.completedFuture(Collections.singletonMap(clientId, 100L)));
        processor.setupAppend(new SetupAppend(1, clientId, streamSegmentName, ""));
        verify(store).getAttributes(streamSegmentName, Collections.singleton(clientId), true, AppendProcessor.TIMEOUT);

        int eventCount = 10;
        val ac1 = interceptAppend(store, streamSegmentName, updateEventNumber(clientId, 200, 100, eventCount),
                CompletableFuture.completedFuture(null));
        processor.append(new Append(streamSegmentName, clientId, 200, eventCount, Unpooled.wrappedBuffer(data), null, requestId));
        verifyStoreAppend(ac1, data);

        val ac2 = interceptAppend(store, streamSegmentName, updateEventNumber(clientId, 300, 200, eventCount),
                CompletableFuture.completedFuture(null));
        processor.append(new Append(streamSegmentName, clientId, 300, eventCount, Unpooled.wrappedBuffer(data), null, requestId));
        verifyStoreAppend(ac2, data);

        verifyNoMoreInteractions(store);
    }


    @Test
    public void testUnsupportedOperation() {
        String streamSegmentName = "scope/stream/testAppendSegment";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        AppendProcessor processor = new AppendProcessor(store, connection, new FailingRequestProcessor(), null);

        setupGetAttributes(streamSegmentName, clientId, store);
        val ac = interceptAppend(store, streamSegmentName, updateEventNumber(clientId, data.length), Futures.failedFuture(new UnsupportedOperationException()));

        processor.setupAppend(new SetupAppend(1, clientId, streamSegmentName, ""));
        processor.append(new Append(streamSegmentName, clientId, data.length, 1, Unpooled.wrappedBuffer(data), null, requestId));
        verify(store).getAttributes(anyString(), eq(Collections.singleton(clientId)), eq(true), eq(AppendProcessor.TIMEOUT));
        verifyStoreAppend(ac, data);

        verify(connection).send(new AppendSetup(1, streamSegmentName, clientId, 0));
        verify(connection, atLeast(0)).resumeReading();
        verify(connection).send(new OperationUnsupported(requestId, "appending data", ""));
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
    }

    @Test
    public void testCancellationException() {
        String streamSegmentName = "scope/stream/testAppendSegment";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        AppendProcessor processor = new AppendProcessor(store, connection, new FailingRequestProcessor(), null);

        setupGetAttributes(streamSegmentName, clientId, store);
        val ac = interceptAppend(store, streamSegmentName, updateEventNumber(clientId, data.length), Futures.failedFuture(new CancellationException()));

        processor.setupAppend(new SetupAppend(1, clientId, streamSegmentName, ""));
        processor.append(new Append(streamSegmentName, clientId, data.length, 1, Unpooled.wrappedBuffer(data), null, requestId));
        verify(store).getAttributes(anyString(), eq(Collections.singleton(clientId)), eq(true), eq(AppendProcessor.TIMEOUT));
        verifyStoreAppend(ac, data);

        verify(connection).send(new AppendSetup(1, streamSegmentName, clientId, 0));
        verify(connection, atLeast(0)).resumeReading();
        verify(connection).close();
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
    }

    private Collection<AttributeUpdate> updateEventNumber(UUID clientId, long eventNum) {
        return updateEventNumber(clientId, eventNum, 0, 1);
    }

    private Collection<AttributeUpdate> updateEventNumber(UUID clientId, long eventNum, long previousValue, long eventCount) {
        return Arrays.asList(new AttributeUpdate(clientId, AttributeUpdateType.ReplaceIfEquals, eventNum,
                                                 previousValue),
                             new AttributeUpdate(EVENT_COUNT, AttributeUpdateType.Accumulate, eventCount));
    }

    private void setupGetAttributes(String streamSegmentName, UUID clientId, StreamSegmentStore store) {
        setupGetAttributes(streamSegmentName, clientId, 0, store);
    }

    private void setupGetAttributes(String streamSegmentName, UUID clientId, long eventNumber, StreamSegmentStore store) {
        when(store.getAttributes(streamSegmentName, Collections.singleton(clientId), true, AppendProcessor.TIMEOUT))
                .thenReturn(CompletableFuture.completedFuture(Collections.singletonMap(clientId, eventNumber)));
    }

    private AppendContext interceptAppend(StreamSegmentStore store, String streamSegmentName, Collection<AttributeUpdate> attributeUpdates,
                                          CompletableFuture<Void> response) {
        val result = new AppendContext(store, streamSegmentName, attributeUpdates);
        when(store.append(eq(streamSegmentName), any(), eq(attributeUpdates), eq(AppendProcessor.TIMEOUT)))
                .thenAnswer(invocation -> {
                    result.appendedData.set(((ByteBufWrapper) invocation.getArgument(1)).getCopy());
                    return response;
                });
        return result;
    }

    private AppendContext interceptAppend(StreamSegmentStore store, String streamSegmentName, long offset, Collection<AttributeUpdate> attributeUpdates,
                                          CompletableFuture<Void> response) {
        val result = new AppendContext(store, streamSegmentName, offset, attributeUpdates);
        when(store.append(eq(streamSegmentName), eq(offset), any(), eq(attributeUpdates), eq(AppendProcessor.TIMEOUT)))
                .thenAnswer(invocation -> {
                    result.appendedData.set(((ByteBufWrapper) invocation.getArgument(2)).getCopy());
                    return response;
                });
        return result;
    }

    private void verifyStoreAppend(AppendContext appendContext, byte[] expectedData) {
        verifyStoreAppend(Mockito::verify, appendContext, expectedData);
    }

    private void verifyStoreAppend(InOrder verifier, AppendContext appendContext, byte[] expectedData) {
        verifyStoreAppend(verifier::verify, appendContext, expectedData);
    }

    private void verifyStoreAppend(Function<StreamSegmentStore, StreamSegmentStore> verifier, AppendContext appendContext, byte[] expectedData) {
        if (appendContext.offset == null) {
            verifier.apply(appendContext.store).append(eq(appendContext.segmentName),
                    any(),
                    eq(appendContext.attributeUpdates),
                    eq(AppendProcessor.TIMEOUT));
        } else {
            verifier.apply(appendContext.store).append(eq(appendContext.segmentName),
                    eq(appendContext.offset),
                    any(),
                    eq(appendContext.attributeUpdates),
                    eq(AppendProcessor.TIMEOUT));
        }
        assertArrayEquals(expectedData, appendContext.appendedData.get());
    }

    @RequiredArgsConstructor
    private static class AppendContext {
        final StreamSegmentStore store;
        final String segmentName;
        final Long offset;
        final Collection<AttributeUpdate> attributeUpdates;
        final AtomicReference<byte[]> appendedData = new AtomicReference<>();

        AppendContext(StreamSegmentStore store, String segmentName, Collection<AttributeUpdate> attributeUpdates) {
            this(store, segmentName, null, attributeUpdates);
        }
    }

    private BufferView wrap(byte[] appendData) {
        return new ByteBufWrapper(Unpooled.wrappedBuffer(appendData));
    }
}

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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.BadAttributeUpdateException;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.stat.SegmentStatsRecorder;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.AppendDecoder;
import io.pravega.shared.protocol.netty.ByteBufWrapper;
import io.pravega.shared.protocol.netty.CommandDecoder;
import io.pravega.shared.protocol.netty.CommandEncoder;
import io.pravega.shared.protocol.netty.ExceptionLoggingHandler;
import io.pravega.shared.protocol.netty.FailingRequestProcessor;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.Request;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommandType;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import static io.pravega.segmentstore.contracts.Attributes.EVENT_COUNT;
import static io.pravega.shared.protocol.netty.WireCommands.MAX_WIRECOMMAND_SIZE;
import static io.pravega.test.common.AssertExtensions.assertEventuallyEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AppendProcessorTest {
    private final long requestId = 1L;

    @Test
    public void testAppend() {
        String streamSegmentName = "scope/stream/0.#epoch.0";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        val mockedRecorder = Mockito.mock(SegmentStatsRecorder.class);
        AppendProcessor processor = new AppendProcessor(store, connection, new FailingRequestProcessor(), mockedRecorder, null, false);

        setupGetAttributes(streamSegmentName, clientId, store);
        val ac = interceptAppend(store, streamSegmentName, updateEventNumber(clientId, data.length), CompletableFuture.completedFuture((long) data.length));

        processor.setupAppend(new SetupAppend(1, clientId, streamSegmentName, ""));
        processor.append(new Append(streamSegmentName, clientId, data.length, 1, Unpooled.wrappedBuffer(data), null, requestId));
        verify(store).getAttributes(anyString(), eq(Collections.singleton(clientId)), eq(true), eq(AppendProcessor.TIMEOUT));
        verifyStoreAppend(ac, data);
        verify(connection).send(new AppendSetup(1, streamSegmentName, clientId, 0));
        verify(connection).adjustOutstandingBytes(data.length);
        verify(connection).send(new DataAppended(requestId, clientId, data.length, 0L, data.length));
        verify(connection).adjustOutstandingBytes(-data.length);
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);

        verify(mockedRecorder).recordAppend(eq(streamSegmentName), eq(8L), eq(1), any());
    }

    @Test
    public void testTransactionAppend() {
        String streamSegmentName = "scope/stream/transactionSegment#transaction.01234567890123456789012345678901";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        val mockedRecorder = Mockito.mock(SegmentStatsRecorder.class);
        AppendProcessor processor = new AppendProcessor(store, connection, new FailingRequestProcessor(), mockedRecorder, null, false);

        setupGetAttributes(streamSegmentName, clientId, store);
        val ac = interceptAppend(store, streamSegmentName, updateEventNumber(clientId, data.length), CompletableFuture.completedFuture(21L));
        processor.setupAppend(new SetupAppend(requestId, clientId, streamSegmentName, ""));
        processor.append(new Append(streamSegmentName, clientId, data.length, 1, Unpooled.wrappedBuffer(data), null, requestId));
        verify(store).getAttributes(anyString(), eq(Collections.singleton(clientId)), eq(true), eq(AppendProcessor.TIMEOUT));
        verifyStoreAppend(ac, data);
        verify(connection).send(new AppendSetup(requestId, streamSegmentName, clientId, 0));
        verify(connection).adjustOutstandingBytes(data.length);
        verify(connection).send(new DataAppended(requestId, clientId, data.length, 0L, 21L));
        verify(connection).adjustOutstandingBytes(-data.length);
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

        val ac1 = interceptAppend(store, streamSegmentName1, updateEventNumber(clientId, 10), CompletableFuture.completedFuture(1L));
        processor.append(new Append(streamSegmentName1, clientId, 10, 1, Unpooled.wrappedBuffer(data), null, requestId));
        verifyStoreAppend(verifier, ac1, data);

        setupGetAttributes(streamSegmentName2, clientId, store);
        processor.setupAppend(new SetupAppend(1, clientId, streamSegmentName2, ""));
        verifier.verify(store).getAttributes(anyString(), eq(Collections.singleton(clientId)), eq(true), eq(AppendProcessor.TIMEOUT));

        val ac2 = interceptAppend(store, streamSegmentName2, updateEventNumber(clientId, 2000), CompletableFuture.completedFuture(2L));
        processor.append(new Append(streamSegmentName2, clientId, 2000, 1, Unpooled.wrappedBuffer(data), null, requestId));
        verifyStoreAppend(verifier, ac2, data);

        val ac3 = interceptAppend(store, streamSegmentName1, updateEventNumber(clientId, 20, 10, 1), CompletableFuture.completedFuture(3L));
        processor.append(new Append(streamSegmentName1, clientId, 20, 1, Unpooled.wrappedBuffer(data), null, requestId));
        verifyStoreAppend(verifier, ac3, data);

        verifyNoMoreInteractions(store);
    }

    @Test
    public void testConditionalAppendSuccess() {
        String streamSegmentName = "scope/stream/testConditionalAppendSuccess";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        val mockedRecorder = Mockito.mock(SegmentStatsRecorder.class);
        AppendProcessor processor = new AppendProcessor(store, connection, new FailingRequestProcessor(), mockedRecorder, null, false);

        setupGetAttributes(streamSegmentName, clientId, store);
        val ac1 = interceptAppend(store, streamSegmentName, updateEventNumber(clientId, 1), CompletableFuture.completedFuture((long) data.length));
        processor.setupAppend(new SetupAppend(1, clientId, streamSegmentName, ""));
        processor.append(new Append(streamSegmentName, clientId, 1, 1, Unpooled.wrappedBuffer(data), null, requestId));

        val ac2 = interceptAppend(store, streamSegmentName, data.length, updateEventNumber(clientId, 2, 1, 1), CompletableFuture.completedFuture(Long.valueOf(2 * data.length)));
        processor.append(new Append(streamSegmentName, clientId, 2, 1, Unpooled.wrappedBuffer(data), (long) data.length, requestId));
        verify(store).getAttributes(anyString(), eq(Collections.singleton(clientId)), eq(true), eq(AppendProcessor.TIMEOUT));
        verifyStoreAppend(ac1, data);
        verifyStoreAppend(ac2, data);
        verify(connection).send(new AppendSetup(1, streamSegmentName, clientId, 0));
        verify(connection, times(2)).adjustOutstandingBytes(data.length);
        verify(connection).send(new DataAppended(requestId, clientId, 1, 0, data.length));
        verify(connection).send(new DataAppended(requestId, clientId, 2, 1, 2 * data.length));
        verify(connection, times(2)).adjustOutstandingBytes(-data.length);
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
        verify(mockedRecorder, times(2)).recordAppend(eq(streamSegmentName), eq(8L), eq(1), any());
    }

    @Test
    public void testConditionalAppendFailure() {
        String streamSegmentName = "scope/stream/testConditionalAppendFailure";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        val mockedRecorder = Mockito.mock(SegmentStatsRecorder.class);
        AppendProcessor processor = new AppendProcessor(store, connection, new FailingRequestProcessor(), mockedRecorder, null, false);

        setupGetAttributes(streamSegmentName, clientId, store);
        val ac1 = interceptAppend(store, streamSegmentName, updateEventNumber(clientId, 1), CompletableFuture.completedFuture((long) data.length));

        processor.setupAppend(new SetupAppend(1, clientId, streamSegmentName, ""));
        processor.append(new Append(streamSegmentName, clientId, 1, 1, Unpooled.wrappedBuffer(data), null, requestId));

        val ac2 = interceptAppend(store, streamSegmentName, 0, updateEventNumber(clientId, 2, 1, 1),
                Futures.failedFuture(new BadOffsetException(streamSegmentName, data.length, 0)));

        processor.append(new Append(streamSegmentName, clientId, 2, 1, Unpooled.wrappedBuffer(data), 0L, requestId));
        verify(store).getAttributes(anyString(), eq(Collections.singleton(clientId)), eq(true), eq(AppendProcessor.TIMEOUT));
        verifyStoreAppend(ac1, data);
        verifyStoreAppend(ac2, data);
        verify(connection).send(new AppendSetup(1, streamSegmentName, clientId, 0));
        verify(connection, times(2)).adjustOutstandingBytes(data.length);
        verify(connection).send(new DataAppended(requestId, clientId, 1, 0, data.length));
        verify(connection).send(new ConditionalCheckFailed(clientId, 2, requestId));
        verify(connection, times(2)).adjustOutstandingBytes(-data.length);
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
        } catch (IllegalStateException e) {
            //expected
        }
        verify(store).getAttributes(anyString(), eq(Collections.singleton(clientId)), eq(true), eq(AppendProcessor.TIMEOUT));
        verify(connection).send(new AppendSetup(1, streamSegmentName, clientId, 100));
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
        } catch (IllegalStateException e) {
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
        val ac1 = interceptAppend(store, segment1, updateEventNumber(clientId1, data.length), CompletableFuture.completedFuture((long) data.length));

        setupGetAttributes(segment2, clientId2, store);
        val ac2 = interceptAppend(store, segment2, updateEventNumber(clientId2, data.length), CompletableFuture.completedFuture((long) data.length));

        processor.setupAppend(new SetupAppend(1, clientId1, segment1, ""));
        processor.append(new Append(segment1, clientId1, data.length, 1, Unpooled.wrappedBuffer(data), null, 2));
        processor.setupAppend(new SetupAppend(3, clientId2, segment2, ""));
        processor.append(new Append(segment2, clientId2, data.length, 1, Unpooled.wrappedBuffer(data), null, 4));

        verify(store).getAttributes(eq(segment1), eq(Collections.singleton(clientId1)), eq(true), eq(AppendProcessor.TIMEOUT));
        verifyStoreAppend(ac1, data);

        verify(store).getAttributes(eq(segment2), eq(Collections.singleton(clientId2)), eq(true), eq(AppendProcessor.TIMEOUT));
        verifyStoreAppend(ac2, data);
        verify(connection, times(2)).adjustOutstandingBytes(data.length);
        verify(connection).send(new AppendSetup(1, segment1, clientId1, 0));
        verify(connection).send(new DataAppended(2, clientId1, data.length, 0, data.length));
        verify(connection).send(new AppendSetup(3, segment2, clientId2, 0));
        verify(connection).send(new DataAppended(4, clientId2, data.length, 0, data.length));
        verify(connection, times(2)).adjustOutstandingBytes(-data.length);
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
    }

    @Test
    public void testAppendFails() {
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
        verify(connection).adjustOutstandingBytes(data.length);
        verify(connection).close();
        verify(connection).adjustOutstandingBytes(-data.length);

        verify(store, atMost(1)).append(any(), any(), any(), any());
        verifyNoMoreInteractions(connection);

        verify(mockedRecorder, never()).recordAppend(eq(streamSegmentName), eq(8L), eq(1), any());
    }

    @Test(timeout = 5000)
    public void testAppendFailChannelClose() throws Exception {
        String streamSegmentName = "scope/stream/testAppendSegment";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[]{1, 2, 3, 4, 6, 7, 8, 9};

        // Setup mocks.
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        setupGetAttributes(streamSegmentName, clientId, store);

        interceptAppend(store, streamSegmentName, updateEventNumber(clientId, data.length), Futures.failedFuture(new IntentionalException()));

        @Cleanup
        EmbeddedChannel channel = createChannel(store);

        // Send a setup append WireCommand.
        Reply reply = sendRequest(channel, new SetupAppend(requestId, clientId, streamSegmentName, ""));
        assertEquals(new AppendSetup(1, streamSegmentName, clientId, 0), reply);

        // Send an append which will cause a RuntimeException to be thrown by the store.
        reply = sendRequest(channel, new Append(streamSegmentName, clientId, data.length, 1, Unpooled.wrappedBuffer(data), null,
                                                requestId));
        assertNull("No WireCommand reply is expected", reply);
        // Verify that the channel is closed by the AppendProcessor.
        assertEventuallyEquals(false, channel::isOpen, 3000);
    }

    @Test(timeout = 5000)
    public void testBadAttributeException() throws Exception {
        String streamSegmentName = "scope/stream/testAppendSegment";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[]{1, 2, 3, 4, 6, 7, 8, 9};

        // Setup mocks.
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        setupGetAttributes(streamSegmentName, clientId, store);
        val ex = new BadAttributeUpdateException(streamSegmentName,
                new AttributeUpdate(UUID.randomUUID(), AttributeUpdateType.ReplaceIfEquals, 100, 101),
                false, "error");
        interceptAppend(store, streamSegmentName, updateEventNumber(clientId, data.length), Futures.failedFuture(ex));

        @Cleanup
        EmbeddedChannel channel = createChannel(store);

        // Send a setup append WireCommand.
        Reply reply = sendRequest(channel, new SetupAppend(requestId, clientId, streamSegmentName, ""));
        assertEquals(new AppendSetup(1, streamSegmentName, clientId, 0), reply);

        // Send an append which will cause a RuntimeException to be thrown by the store.
        reply = sendRequest(channel, new Append(streamSegmentName, clientId, data.length, 1, Unpooled.wrappedBuffer(data), null,
                                                requestId));
        // validate InvalidEventNumber Wirecommand is sent before closing the Channel.
        assertNotNull("Invalid Event WireCommand is expected", reply);
        assertEquals(WireCommandType.INVALID_EVENT_NUMBER.getCode(), ((WireCommand) reply).getType().getCode());

        // Verify that the channel is closed by the AppendProcessor.
        assertEventuallyEquals(false, channel::isOpen, 3000);
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
                updateEventNumber(clientId, 100, Attributes.NULL_ATTRIBUTE_VALUE, eventCount), CompletableFuture.completedFuture((long) data.length));
        processor.append(new Append(streamSegmentName, clientId, 100, eventCount, Unpooled.wrappedBuffer(data), null, requestId));
        verifyStoreAppend(ac1, data);

        val ac2 = interceptAppend(store, streamSegmentName, updateEventNumber(clientId, 200, 100, eventCount),
                CompletableFuture.completedFuture(null));
        processor.append(new Append(streamSegmentName, clientId, 200, eventCount, Unpooled.wrappedBuffer(data), null, requestId));
        verifyStoreAppend(ac2, data);

        verifyNoMoreInteractions(store);
    }

    /**
     * Verifies that appends are "pipelined" into the underlying store and that acks are sent as appropriate via the
     * connection when they complete.
     */
    @Test(timeout = 15 * 1000)
    public void testAppendPipelining() {
        String streamSegmentName = "scope/stream/testAppendSegment";
        UUID clientId = UUID.randomUUID();
        byte[] data1 = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        byte[] data2 = new byte[]{1, 2, 3, 4, 5};
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        AppendProcessor processor = new AppendProcessor(store, connection, new FailingRequestProcessor(), null);

        setupGetAttributes(streamSegmentName, clientId, store);
        processor.setupAppend(new SetupAppend(1, clientId, streamSegmentName, ""));
        verify(store).getAttributes(anyString(), eq(Collections.singleton(clientId)), eq(true), eq(AppendProcessor.TIMEOUT));
        verify(connection).send(new AppendSetup(1, streamSegmentName, clientId, 0));

        // Initiate two appends in short sequence, and simulate the Store blocking on both of them.
        val store1 = new CompletableFuture<Long>();
        val ac1 = interceptAppend(store, streamSegmentName, updateEventNumber(clientId, 1, 0, 1), store1);
        processor.append(new Append(streamSegmentName, clientId, 1, 1, Unpooled.wrappedBuffer(data1), null, requestId));
        val store2 = new CompletableFuture<Long>();
        val ac2 = interceptAppend(store, streamSegmentName, updateEventNumber(clientId, 2, 1, 1), store2);
        processor.append(new Append(streamSegmentName, clientId, 2, 1, Unpooled.wrappedBuffer(data2), null, requestId));
        verifyStoreAppend(ac1, data1);
        verifyStoreAppend(ac2, data2);
        verify(connection).adjustOutstandingBytes(data1.length);
        verify(connection).adjustOutstandingBytes(data2.length);

        // Complete the second one (this simulates acks arriving out of order from the store).
        store2.complete(100L);

        // Verify an ack is sent for both appends (because of pipelining guarantees), but only the second append's length
        // is subtracted from the outstanding bytes.
        verify(connection).send(new DataAppended(requestId, clientId, 2, 0L, 100L));
        verify(connection).adjustOutstandingBytes(-data2.length);
        verifyNoMoreInteractions(connection);

        // Complete the first one, and verify that no additional acks are sent via the connection, but the first append's
        // length is subtracted from the outstanding bytes.
        store1.complete(50L);
        verify(connection).adjustOutstandingBytes(-data1.length);
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
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
                CompletableFuture.completedFuture((long) data.length));
        processor.append(new Append(streamSegmentName, clientId, 200, eventCount, Unpooled.wrappedBuffer(data), null, requestId));
        verifyStoreAppend(ac1, data);

        val ac2 = interceptAppend(store, streamSegmentName, updateEventNumber(clientId, 300, 200, eventCount),
                CompletableFuture.completedFuture((long) (2 * data.length)));
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
        verify(connection).adjustOutstandingBytes(data.length);
        verify(connection).send(new OperationUnsupported(requestId, "appending data", ""));
        verify(connection).adjustOutstandingBytes(-data.length);
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
        verify(connection).adjustOutstandingBytes(data.length);
        verify(connection).close();
        verify(connection).adjustOutstandingBytes(-data.length);
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

    private EmbeddedChannel createChannel(StreamSegmentStore store) {
        ServerConnectionInboundHandler lsh = new ServerConnectionInboundHandler(new ConnectionTracker()); // TODO
        EmbeddedChannel channel = new EmbeddedChannel(new ExceptionLoggingHandler(""),
                new CommandEncoder(null),
                new LengthFieldBasedFrameDecoder(MAX_WIRECOMMAND_SIZE, 4, 4),
                new CommandDecoder(),
                new AppendDecoder(),
                lsh);
        lsh.setRequestProcessor(new AppendProcessor(store, lsh, new PravegaRequestProcessor(store, mock(TableStore.class), lsh), null));
        return channel;
    }

    private Reply sendRequest(EmbeddedChannel channel, Request request) throws Exception {
        channel.writeInbound(request);
        Object encodedReply = channel.readOutbound();
        for (int i = 0; encodedReply == null && i < 50; i++) {
            channel.runPendingTasks();
            Thread.sleep(10);
            encodedReply = channel.readOutbound();
        }
        if (encodedReply == null) {
            return null;
        }
        WireCommand decoded = CommandDecoder.parseCommand((ByteBuf) encodedReply);
        ((ByteBuf) encodedReply).release();
        assertNotNull(decoded);
        return (Reply) decoded;
    }

    private AppendContext interceptAppend(StreamSegmentStore store, String streamSegmentName, Collection<AttributeUpdate> attributeUpdates,
                                          CompletableFuture<Long> response) {
        val result = new AppendContext(store, streamSegmentName, attributeUpdates);
        when(store.append(eq(streamSegmentName), any(), eq(attributeUpdates), eq(AppendProcessor.TIMEOUT)))
                .thenAnswer(invocation -> {
                    result.appendedData.set(((ByteBufWrapper) invocation.getArgument(1)).getCopy());
                    return response;
                });
        return result;
    }

    private AppendContext interceptAppend(StreamSegmentStore store, String streamSegmentName, long offset, Collection<AttributeUpdate> attributeUpdates,
                                          CompletableFuture<Long> response) {
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
}

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

import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.FailingRequestProcessor;
import io.pravega.shared.protocol.netty.WireCommands.AppendSetup;
import io.pravega.shared.protocol.netty.WireCommands.ConditionalCheckFailed;
import io.pravega.shared.protocol.netty.WireCommands.DataAppended;
import io.pravega.shared.protocol.netty.WireCommands.SetupAppend;
import io.pravega.common.util.ImmutableDate;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.netty.buffer.Unpooled;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.junit.Ignore;
import org.junit.Test;

import static io.pravega.segmentstore.contracts.Attributes.EVENT_COUNT;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AppendProcessorTest {

    @Test
    public void testAppend() {
        String streamSegmentName = "testAppendSegment";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        AppendProcessor processor = new AppendProcessor(store, connection, new FailingRequestProcessor());

        setupGetStreamSegmentInfo(streamSegmentName, clientId, store);
        CompletableFuture<Void> result = CompletableFuture.completedFuture(null);
        when(store.append(streamSegmentName, data, updateEventNumber(clientId, data.length), AppendProcessor.TIMEOUT))
            .thenReturn(result);

        processor.setupAppend(new SetupAppend(1, clientId, streamSegmentName));
        processor.append(new Append(streamSegmentName, clientId, data.length, Unpooled.wrappedBuffer(data), null));
        verify(store).getStreamSegmentInfo(anyString(), eq(true), eq(AppendProcessor.TIMEOUT));
        verify(store).append(streamSegmentName,
                             data,
                             updateEventNumber(clientId, data.length),
                             AppendProcessor.TIMEOUT);
        verify(connection).send(new AppendSetup(1, streamSegmentName, clientId, 0));
        verify(connection, atLeast(0)).resumeReading();
        verify(connection).send(new DataAppended(clientId, data.length));
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
    }

    @Test
    public void testConditionalAppendSuccess() {
        String streamSegmentName = "testConditionalAppendSuccess";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        AppendProcessor processor = new AppendProcessor(store, connection, new FailingRequestProcessor());

        setupGetStreamSegmentInfo(streamSegmentName, clientId, store);
        CompletableFuture<Void> result = CompletableFuture.completedFuture(null);
        when(store.append(streamSegmentName, data, updateEventNumber(clientId, 1), AppendProcessor.TIMEOUT))
            .thenReturn(result);
        processor.setupAppend(new SetupAppend(1, clientId, streamSegmentName));
        processor.append(new Append(streamSegmentName, clientId, 1, Unpooled.wrappedBuffer(data), null));

        result = CompletableFuture.completedFuture(null);
        when(store.append(streamSegmentName,
                          data.length,
                          data,
                          updateEventNumber(clientId, 2),
                          AppendProcessor.TIMEOUT)).thenReturn(result);

        processor.append(new Append(streamSegmentName, clientId, 2, Unpooled.wrappedBuffer(data), (long) data.length));
        verify(store).getStreamSegmentInfo(anyString(), eq(true), eq(AppendProcessor.TIMEOUT));
        verify(store).append(streamSegmentName,
                             data,
                             updateEventNumber(clientId, 1),
                             AppendProcessor.TIMEOUT);
        verify(store).append(streamSegmentName,
                             data.length,
                             data,
                             updateEventNumber(clientId, 2),
                             AppendProcessor.TIMEOUT);
        verify(connection).send(new AppendSetup(1, streamSegmentName, clientId, 0));
        verify(connection, atLeast(0)).resumeReading();
        verify(connection).send(new DataAppended(clientId, 1));
        verify(connection).send(new DataAppended(clientId, 2));
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
    }

    @Test
    public void testConditionalAppendFailure() {
        String streamSegmentName = "testConditionalAppendFailure";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        AppendProcessor processor = new AppendProcessor(store, connection, new FailingRequestProcessor());

        setupGetStreamSegmentInfo(streamSegmentName, clientId, store);
        CompletableFuture<Void> result = CompletableFuture.completedFuture(null);
        when(store.append(streamSegmentName, data, updateEventNumber(clientId, 1), AppendProcessor.TIMEOUT))
            .thenReturn(result);
        processor.setupAppend(new SetupAppend(1, clientId, streamSegmentName));
        processor.append(new Append(streamSegmentName, clientId, 1, Unpooled.wrappedBuffer(data), null));

        result = FutureHelpers.failedFuture(new BadOffsetException(streamSegmentName, data.length, 0));
        when(store.append(streamSegmentName,
                          0,
                          data,
                          updateEventNumber(clientId, 2),
                          AppendProcessor.TIMEOUT)).thenReturn(result);

        processor.append(new Append(streamSegmentName, clientId, 2, Unpooled.wrappedBuffer(data), 0L));
        verify(store).getStreamSegmentInfo(anyString(), eq(true), eq(AppendProcessor.TIMEOUT));
        verify(store).append(streamSegmentName,
                             data,
                             updateEventNumber(clientId, 1),
                             AppendProcessor.TIMEOUT);
        verify(store).append(streamSegmentName,
                             0L,
                             data,
                             updateEventNumber(clientId, 2),
                             AppendProcessor.TIMEOUT);
        verify(connection).send(new AppendSetup(1, streamSegmentName, clientId, 0));
        verify(connection, atLeast(0)).resumeReading();
        verify(connection).send(new DataAppended(clientId, 1));
        verify(connection).send(new ConditionalCheckFailed(clientId, 2));
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
    }

    @Test
    public void testInvalidOffset() {
        String streamSegmentName = "testAppendSegment";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        AppendProcessor processor = new AppendProcessor(store, connection, new FailingRequestProcessor());

        setupGetStreamSegmentInfo(streamSegmentName, clientId, 100, store);
        processor.setupAppend(new SetupAppend(1, clientId, streamSegmentName));
        try {
            processor.append(new Append(streamSegmentName, clientId, data.length, Unpooled.wrappedBuffer(data), null));
            fail();
        } catch (RuntimeException e) {
            //expected
        }
        verify(store).getStreamSegmentInfo(anyString(), eq(true), eq(AppendProcessor.TIMEOUT));
        verify(connection).send(new AppendSetup(1, streamSegmentName, clientId, 100));
        verify(connection, atLeast(0)).resumeReading();
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
    }

    @Test
    public void testSetupSkipped() {
        String streamSegmentName = "testAppendSegment";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        AppendProcessor processor = new AppendProcessor(store, connection, new FailingRequestProcessor());
        try {
            processor.append(new Append(streamSegmentName, clientId, data.length, Unpooled.wrappedBuffer(data), null));
            fail();
        } catch (RuntimeException e) {
            //expected
        }
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
    }

    @Test
    public void testSwitchingStream() {
        String segment1 = "segment1";
        String segment2 = "segment2";
        UUID clientId1 = UUID.randomUUID();
        UUID clientId2 = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        AppendProcessor processor = new AppendProcessor(store, connection, new FailingRequestProcessor());

        setupGetStreamSegmentInfo(segment1, clientId1, store);
        CompletableFuture<Void> result = CompletableFuture.completedFuture(null);
        when(store.append(segment1, data, updateEventNumber(clientId1, data.length), AppendProcessor.TIMEOUT))
            .thenReturn(result);

        setupGetStreamSegmentInfo(segment2, clientId2, store);
        result = CompletableFuture.completedFuture(null);
        when(store.append(segment2, data, updateEventNumber(clientId2, data.length), AppendProcessor.TIMEOUT))
            .thenReturn(result);

        processor.setupAppend(new SetupAppend(1, clientId1, segment1));
        processor.append(new Append(segment1, clientId1, data.length, Unpooled.wrappedBuffer(data), null));
        processor.setupAppend(new SetupAppend(2, clientId2, segment2));
        processor.append(new Append(segment2, clientId2, data.length, Unpooled.wrappedBuffer(data), null));

        verify(store).getStreamSegmentInfo(eq(segment1), eq(true), eq(AppendProcessor.TIMEOUT));
        verify(store).append(segment1,
                             data,
                             updateEventNumber(clientId1, data.length),
                             AppendProcessor.TIMEOUT);
        verify(store).getStreamSegmentInfo(eq(segment2), eq(true), eq(AppendProcessor.TIMEOUT));
        verify(store).append(segment2,
                             data,
                             updateEventNumber(clientId2, data.length),
                             AppendProcessor.TIMEOUT);
        verify(connection, atLeast(0)).resumeReading();
        verify(connection).send(new AppendSetup(1, segment1, clientId1, 0));
        verify(connection).send(new DataAppended(clientId1, data.length));
        verify(connection).send(new AppendSetup(2, segment2, clientId2, 0));
        verify(connection).send(new DataAppended(clientId2, data.length));
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(store);
    }

    @Test
    public void testAppendFails() {
        String streamSegmentName = "testAppendSegment";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        AppendProcessor processor = new AppendProcessor(store, connection, new FailingRequestProcessor());

        setupGetStreamSegmentInfo(streamSegmentName, clientId, store);
        CompletableFuture<Void> result = new CompletableFuture<>();
        result.completeExceptionally(new RuntimeException("Fake exception for testing"));
        when(store.append(streamSegmentName, data, updateEventNumber(clientId, data.length), AppendProcessor.TIMEOUT))
            .thenReturn(result);

        processor.setupAppend(new SetupAppend(1, clientId, streamSegmentName));
        processor.append(new Append(streamSegmentName, clientId, data.length, Unpooled.wrappedBuffer(data), null));
        try {
            processor.append(new Append(streamSegmentName, clientId, data.length * 2, Unpooled.wrappedBuffer(data), null));
            fail();
        } catch (IllegalStateException e) {
            // Expected
        }
        verify(connection).send(new AppendSetup(1, streamSegmentName, clientId, 0));
        verify(connection, atLeast(0)).resumeReading();
        verify(connection).close();
        verify(store, atMost(1)).append(any(), any(), any(), any());
        verifyNoMoreInteractions(connection);
    }

    @Test
    public void testAppendEventCount() {
        String streamSegmentName = "testAppendSegment";
        UUID clientId = UUID.randomUUID();
        byte[] data = new byte[] { 1, 2, 3, 4, 6, 7, 8, 9 };
        StreamSegmentStore store = mock(StreamSegmentStore.class);
        ServerConnection connection = mock(ServerConnection.class);
        AppendProcessor processor = new AppendProcessor(store, connection, new FailingRequestProcessor());

        CompletableFuture<SegmentProperties> propsFuture = CompletableFuture.completedFuture(
                new StreamSegmentInformation(streamSegmentName, 0, false, false,
                        Collections.singletonMap(clientId, 0L), new ImmutableDate()));

        when(store.getStreamSegmentInfo(streamSegmentName, true, AppendProcessor.TIMEOUT))
                .thenReturn(propsFuture);
        CompletableFuture<Void> result = CompletableFuture.completedFuture(null);
        int eventCount = 100;
        when(store.append(streamSegmentName, data, updateEventNumber(clientId, 100, eventCount), AppendProcessor.TIMEOUT))
                .thenReturn(result);

        processor.setupAppend(new SetupAppend(1, clientId, streamSegmentName));
        processor.append(new Append(streamSegmentName, clientId, 100, eventCount, Unpooled.wrappedBuffer(data), null));
        verify(store).append(streamSegmentName,
                data,
                updateEventNumber(clientId, 100, eventCount),
                AppendProcessor.TIMEOUT);

        Map<UUID, Long> map = new HashMap<>();
        map.put(clientId, 100L);
        map.put(EVENT_COUNT, 100L);
        propsFuture = CompletableFuture.completedFuture(
                new StreamSegmentInformation(streamSegmentName, 0, false, false,
                        map, new ImmutableDate()));

        when(store.getStreamSegmentInfo(streamSegmentName, true, AppendProcessor.TIMEOUT))
                .thenReturn(propsFuture);
        when(store.append(streamSegmentName, data, updateEventNumber(clientId, 200, eventCount), AppendProcessor.TIMEOUT))
                .thenReturn(result);

        processor.append(new Append(streamSegmentName, clientId, 200, eventCount, Unpooled.wrappedBuffer(data), null));
        verify(store).append(streamSegmentName,
                data,
                updateEventNumber(clientId, 200, eventCount),
                AppendProcessor.TIMEOUT);
    }

    @Test
    @Ignore
    public void testRecoveryFromFailure() {
        fail();
    }

    private Collection<AttributeUpdate> updateEventNumber(UUID clientId, long newValue) {
        return updateEventNumber(clientId, newValue, 1);
    }

    private Collection<AttributeUpdate> updateEventNumber(UUID clientId, long newValue, long eventCount) {
        return Arrays.asList(
                new AttributeUpdate(clientId, AttributeUpdateType.ReplaceIfGreater, newValue),
                new AttributeUpdate(EVENT_COUNT, AttributeUpdateType.Accumulate, eventCount));
    }

    private void setupGetStreamSegmentInfo(String streamSegmentName, UUID clientId, StreamSegmentStore store) {
        setupGetStreamSegmentInfo(streamSegmentName, clientId, 0, store);
    }

    private void setupGetStreamSegmentInfo(String streamSegmentName, UUID clientId, long eventNumber, StreamSegmentStore store) {
        CompletableFuture<SegmentProperties> propsFuture = CompletableFuture.completedFuture(
                new StreamSegmentInformation(streamSegmentName, 0, false, false,
                        Collections.singletonMap(clientId, eventNumber), new ImmutableDate()));

        when(store.getStreamSegmentInfo(streamSegmentName, true, AppendProcessor.TIMEOUT))
                .thenReturn(propsFuture);
    }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.service.server.host.handler;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.netty.Append;
import com.emc.pravega.common.netty.FailingRequestProcessor;
import com.emc.pravega.common.netty.WireCommands.AppendSetup;
import com.emc.pravega.common.netty.WireCommands.ConditionalCheckFailed;
import com.emc.pravega.common.netty.WireCommands.DataAppended;
import com.emc.pravega.common.netty.WireCommands.SetupAppend;
import com.emc.pravega.service.contracts.Attribute;
import com.emc.pravega.service.contracts.AttributeUpdate;
import com.emc.pravega.service.contracts.BadOffsetException;
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.contracts.StreamSegmentInformation;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import io.netty.buffer.Unpooled;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.Ignore;
import org.junit.Test;

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

        processor.setupAppend(new SetupAppend(clientId, streamSegmentName));
        processor.append(new Append(streamSegmentName, clientId, data.length, Unpooled.wrappedBuffer(data), null));
        verify(store).getStreamSegmentInfo(anyString(), eq(true), eq(AppendProcessor.TIMEOUT));
        verify(store).append(streamSegmentName,
                             data,
                             updateEventNumber(clientId, data.length),
                             AppendProcessor.TIMEOUT);
        verify(connection).send(new AppendSetup(streamSegmentName, clientId, 0));
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
        processor.setupAppend(new SetupAppend(clientId, streamSegmentName));
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
        verify(connection).send(new AppendSetup(streamSegmentName, clientId, 0));
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
        processor.setupAppend(new SetupAppend(clientId, streamSegmentName));
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
        verify(connection).send(new AppendSetup(streamSegmentName, clientId, 0));
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
        processor.setupAppend(new SetupAppend(clientId, streamSegmentName));
        try {
            processor.append(new Append(streamSegmentName, clientId, data.length, Unpooled.wrappedBuffer(data), null));
            fail();
        } catch (RuntimeException e) {
            //expected
        }
        verify(store).getStreamSegmentInfo(anyString(), eq(true), eq(AppendProcessor.TIMEOUT));
        verify(connection).send(new AppendSetup(streamSegmentName, clientId, 100));
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

        processor.setupAppend(new SetupAppend(clientId1, segment1));
        processor.append(new Append(segment1, clientId1, data.length, Unpooled.wrappedBuffer(data), null));
        processor.setupAppend(new SetupAppend(clientId2, segment2));
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
        verify(connection).send(new AppendSetup(segment1, clientId1, 0));
        verify(connection).send(new DataAppended(clientId1, data.length));
        verify(connection).send(new AppendSetup(segment2, clientId2, 0));
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

        processor.setupAppend(new SetupAppend(clientId, streamSegmentName));
        processor.append(new Append(streamSegmentName, clientId, data.length, Unpooled.wrappedBuffer(data), null));
        try {
            processor.append(new Append(streamSegmentName, clientId, data.length * 2, Unpooled.wrappedBuffer(data), null));
            fail();
        } catch (IllegalStateException e) {
            // Expected
        }
        verify(connection).send(new AppendSetup(streamSegmentName, clientId, 0));
        verify(connection, atLeast(0)).resumeReading();
        verify(connection).close();
        verify(store, atMost(1)).append(any(), any(), any(), any());
        verifyNoMoreInteractions(connection);
    }

    @Test
    @Ignore
    public void testRecoveryFromFailure() {
        fail();
    }

    private Collection<AttributeUpdate> updateEventNumber(UUID clientId, long newValue) {
        return Collections.singleton(
                new AttributeUpdate(Attribute.dynamic(clientId, Attribute.UpdateType.ReplaceIfGreater), newValue));
    }

    private void setupGetStreamSegmentInfo(String streamSegmentName, UUID clientId, StreamSegmentStore store) {
        setupGetStreamSegmentInfo(streamSegmentName, clientId, 0, store);
    }

    private void setupGetStreamSegmentInfo(String streamSegmentName, UUID clientId, long eventNumber, StreamSegmentStore store) {
        CompletableFuture<SegmentProperties> propsFuture = CompletableFuture.completedFuture(
                new StreamSegmentInformation(streamSegmentName, 0, false, false,
                        Collections.singletonMap(clientId, eventNumber), new Date()));

        when(store.getStreamSegmentInfo(streamSegmentName, true, AppendProcessor.TIMEOUT))
                .thenReturn(propsFuture);
    }
}

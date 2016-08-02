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

import com.emc.pravega.service.contracts.AppendContext;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.nautilus.common.netty.FailingRequestProcessor;
import com.emc.nautilus.common.netty.ServerConnection;
import com.emc.nautilus.common.netty.WireCommands.Append;
import com.emc.nautilus.common.netty.WireCommands.AppendSetup;
import com.emc.nautilus.common.netty.WireCommands.DataAppended;
import com.emc.nautilus.common.netty.WireCommands.SetupAppend;
import io.netty.buffer.Unpooled;

import org.junit.Ignore;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.eq;
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

        CompletableFuture<AppendContext> contextFuture = new CompletableFuture<>();
        contextFuture.complete(new AppendContext(clientId, 0));
        when(store.getLastAppendContext(streamSegmentName, clientId, AppendProcessor.TIMEOUT)).thenReturn(contextFuture);
        CompletableFuture<Long> result = new CompletableFuture<>();
        result.complete((long) data.length);
        when(store.append(streamSegmentName, data, new AppendContext(clientId, data.length), AppendProcessor.TIMEOUT))
            .thenReturn(result);

        processor.setupAppend(new SetupAppend(clientId, streamSegmentName));
        processor.append(new Append(streamSegmentName, clientId, data.length, Unpooled.wrappedBuffer(data)));
        verify(store).getLastAppendContext(anyString(), any(), eq(AppendProcessor.TIMEOUT));
        verify(store).append(streamSegmentName,
                             data,
                             new AppendContext(clientId, data.length),
                             AppendProcessor.TIMEOUT);
        verify(connection).send(new AppendSetup(streamSegmentName, clientId, 0));
        verify(connection, atLeast(0)).resumeReading();
        verify(connection).send(new DataAppended(clientId, data.length));
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

        CompletableFuture<AppendContext> contextFuture = new CompletableFuture<>();
        contextFuture.complete(new AppendContext(clientId, 100));
        when(store.getLastAppendContext(streamSegmentName, clientId, AppendProcessor.TIMEOUT)).thenReturn(contextFuture);

        processor.setupAppend(new SetupAppend(clientId, streamSegmentName));
        try {
            processor.append(new Append(streamSegmentName, clientId, data.length, Unpooled.wrappedBuffer(data)));
            fail();
        } catch (RuntimeException e) {
            //expected
        }
        verify(store).getLastAppendContext(anyString(), any(), eq(AppendProcessor.TIMEOUT));
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
            processor.append(new Append(streamSegmentName, clientId, data.length, Unpooled.wrappedBuffer(data)));
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

        CompletableFuture<AppendContext> contextFuture = new CompletableFuture<>();
        contextFuture.complete(new AppendContext(clientId1, 0));
        when(store.getLastAppendContext(segment1, clientId1, AppendProcessor.TIMEOUT)).thenReturn(contextFuture);
        CompletableFuture<Long> result = new CompletableFuture<>();
        result.complete((long) data.length);
        when(store.append(segment1, data, new AppendContext(clientId1, data.length), AppendProcessor.TIMEOUT))
            .thenReturn(result);
        
        contextFuture = new CompletableFuture<>();
        contextFuture.complete(new AppendContext(clientId2, 0));
        when(store.getLastAppendContext(segment2, clientId2, AppendProcessor.TIMEOUT)).thenReturn(contextFuture);
        result = new CompletableFuture<>();
        result.complete((long) data.length);
        when(store.append(segment2, data, new AppendContext(clientId2, data.length), AppendProcessor.TIMEOUT))
            .thenReturn(result);

        processor.setupAppend(new SetupAppend(clientId1, segment1));
        processor.append(new Append(segment1, clientId1, data.length, Unpooled.wrappedBuffer(data)));
        processor.setupAppend(new SetupAppend(clientId2, segment2));
        processor.append(new Append(segment2, clientId2, data.length, Unpooled.wrappedBuffer(data)));
        
        verify(store).getLastAppendContext(eq(segment1), any(), eq(AppendProcessor.TIMEOUT));
        verify(store).append(segment1,
                             data,
                             new AppendContext(clientId1, data.length),
                             AppendProcessor.TIMEOUT);
        verify(store).getLastAppendContext(eq(segment2), any(), eq(AppendProcessor.TIMEOUT));
        verify(store).append(segment2,
                             data,
                             new AppendContext(clientId2, data.length),
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

        CompletableFuture<AppendContext> contextFuture = new CompletableFuture<>();
        contextFuture.complete(new AppendContext(clientId, 0));
        when(store.getLastAppendContext(streamSegmentName, clientId, AppendProcessor.TIMEOUT)).thenReturn(contextFuture);
        CompletableFuture<Long> result = new CompletableFuture<>();
        result.completeExceptionally(new RuntimeException("Fake exception for testing"));
        when(store.append(streamSegmentName, data, new AppendContext(clientId, data.length), AppendProcessor.TIMEOUT))
            .thenReturn(result);

        processor.setupAppend(new SetupAppend(clientId, streamSegmentName));
        processor.append(new Append(streamSegmentName, clientId, data.length, Unpooled.wrappedBuffer(data)));
        try {
            processor.append(new Append(streamSegmentName, clientId, data.length * 2, Unpooled.wrappedBuffer(data)));
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
}

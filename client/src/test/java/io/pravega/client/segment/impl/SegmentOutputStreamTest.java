/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.client.segment.impl;


import com.google.common.collect.ImmutableList;
import io.netty.buffer.Unpooled;
import io.pravega.client.netty.impl.ClientConnection;
import io.pravega.client.netty.impl.ClientConnection.CompletedCallback;
import io.pravega.client.stream.impl.PendingEvent;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.Async;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import lombok.Cleanup;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;


public class SegmentOutputStreamTest {

    private static final String SEGMENT = "segment";
    private static final int SERVICE_PORT = 12345;

    private static ByteBuffer getBuffer(String s) {
        return ByteBuffer.wrap(s.getBytes());
    }

    @Test(timeout = 10000)
    public void testConnectAndSend() throws SegmentSealedException, ConnectionFailedException {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl(uri);
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, controller, cf, cid);
        output.setupConnection();
        verify(connection).send(new WireCommands.SetupAppend(1, cid, SEGMENT));
        cf.getProcessor(uri).appendSetup(new WireCommands.AppendSetup(1, SEGMENT, cid, 0));

        sendAndVerifyEvent(cid, connection, output, getBuffer("test"), 1, null);
        verifyNoMoreInteractions(connection);
    }

    @Test(timeout = 10000)
    public void testConditionalSend() throws SegmentSealedException, ConnectionFailedException {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl(uri);
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, controller, cf, cid);
        output.setupConnection();
        verify(connection).send(new WireCommands.SetupAppend(1, cid, SEGMENT));
        cf.getProcessor(uri).appendSetup(new WireCommands.AppendSetup(1, SEGMENT, cid, 0));

        sendAndVerifyEvent(cid, connection, output, getBuffer("test"), 1, 0L);
        verifyNoMoreInteractions(connection);
    }

    @Test(timeout = 20000)
    public void testNewEventsGoAfterInflight() throws ConnectionFailedException, SegmentSealedException {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl(uri);
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf);
        ClientConnection connection = mock(ClientConnection.class);
        InOrder inOrder = inOrder(connection);
        cf.provideConnection(uri, connection);
        @SuppressWarnings("resource")
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, controller, cf, cid);
        
        output.setupConnection();
        cf.getProcessor(uri).appendSetup(new WireCommands.AppendSetup(1, SEGMENT, cid, 0));
        output.write(new PendingEvent(null, 1, getBuffer("test1"), new CompletableFuture<>()));
        output.write(new PendingEvent(null, 2, getBuffer("test2"), new CompletableFuture<>()));
        answerSuccess(connection);
        cf.getProcessor(uri).connectionDropped();
        Async.testBlocking(() -> output.write(new PendingEvent(null, 3, getBuffer("test3"), new CompletableFuture<>())),
                           () -> cf.getProcessor(uri).appendSetup(new WireCommands.AppendSetup(1, SEGMENT, cid, 0)));
        output.write(new PendingEvent(null, 4, getBuffer("test4"), new CompletableFuture<>()));
        
        Append append1 = new Append(SEGMENT, cid, 1, Unpooled.wrappedBuffer(getBuffer("test1")), null);
        Append append2 = new Append(SEGMENT, cid, 2, Unpooled.wrappedBuffer(getBuffer("test2")), null);
        Append append3 = new Append(SEGMENT, cid, 3, Unpooled.wrappedBuffer(getBuffer("test3")), null);
        Append append4 = new Append(SEGMENT, cid, 4, Unpooled.wrappedBuffer(getBuffer("test4")), null);
        inOrder.verify(connection).send(new WireCommands.SetupAppend(1, cid, SEGMENT));
        inOrder.verify(connection).send(append1);
        inOrder.verify(connection).send(append2);
        inOrder.verify(connection).close();
        inOrder.verify(connection).send(new WireCommands.SetupAppend(2, cid, SEGMENT));
        inOrder.verify(connection).sendAsync(eq(ImmutableList.of(append1, append2)), any());
        inOrder.verify(connection).send(append3);
        inOrder.verify(connection).send(append4);
        
        verifyNoMoreInteractions(connection);
    }

    private void answerSuccess(ClientConnection connection) {
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                CompletedCallback callback = (CompletedCallback) invocation.getArgument(1);
                callback.complete(null);
                return null;
            }
        }).when(connection).sendAsync(Mockito.any(), Mockito.any());
    }

    private void sendAndVerifyEvent(UUID cid, ClientConnection connection, SegmentOutputStreamImpl output,
            ByteBuffer data, int num, Long expectedLength) throws SegmentSealedException, ConnectionFailedException {
        CompletableFuture<Boolean> acked = new CompletableFuture<>();
        output.write(new PendingEvent(null, num, data, acked, expectedLength));
        verify(connection).send(new Append(SEGMENT, cid, num, Unpooled.wrappedBuffer(data), expectedLength));
        assertEquals(false, acked.isDone());
    }

    @Test(timeout = 10000)
    public void testClose() throws ConnectionFailedException, SegmentSealedException, InterruptedException {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl(uri);
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);

        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, controller, cf, cid);
        output.setupConnection();
        verify(connection).send(new WireCommands.SetupAppend(1, cid, SEGMENT));
        cf.getProcessor(uri).appendSetup(new WireCommands.AppendSetup(1, SEGMENT, cid, 0));
        ByteBuffer data = getBuffer("test");

        CompletableFuture<Boolean> acked = new CompletableFuture<>();
        output.write(new PendingEvent(null, 1, data, acked));
        verify(connection).send(new Append(SEGMENT, cid, 1, Unpooled.wrappedBuffer(data), null));
        assertEquals(false, acked.isDone());
        Async.testBlocking(() -> output.close(), () -> cf.getProcessor(uri).dataAppended(new WireCommands.DataAppended(cid, 1)));
        assertEquals(false, acked.isCompletedExceptionally());
        assertEquals(true, acked.isDone());
        verify(connection).send(new WireCommands.KeepAlive());
        verify(connection).close();
        verifyNoMoreInteractions(connection);
    }

    @Test
    @Ignore
    public void testConnectionFailure() {
        fail();
    }

    @Test(timeout = 10000)
    public void testFlush() throws ConnectionFailedException, SegmentSealedException {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl(uri);
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        InOrder order = Mockito.inOrder(connection);
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, controller, cf, cid);
        output.setupConnection();
        order.verify(connection).send(new WireCommands.SetupAppend(1, cid, SEGMENT));
        cf.getProcessor(uri).appendSetup(new WireCommands.AppendSetup(1, SEGMENT, cid, 0));
        ByteBuffer data = getBuffer("test");

        CompletableFuture<Boolean> acked1 = new CompletableFuture<>();
        output.write(new PendingEvent(null, 1, data, acked1));
        order.verify(connection).send(new Append(SEGMENT, cid, 1, Unpooled.wrappedBuffer(data), null));
        assertEquals(false, acked1.isDone());
        Async.testBlocking(() -> output.flush(), () -> cf.getProcessor(uri).dataAppended(new WireCommands.DataAppended(cid, 1)));
        assertEquals(false, acked1.isCompletedExceptionally());
        assertEquals(true, acked1.isDone());
        order.verify(connection).send(new WireCommands.KeepAlive());
        
        CompletableFuture<Boolean> acked2 = new CompletableFuture<>();
        output.write(new PendingEvent(null, 2, data, acked2));
        order.verify(connection).send(new Append(SEGMENT, cid, 2, Unpooled.wrappedBuffer(data), null));
        assertEquals(false, acked2.isDone());
        Async.testBlocking(() -> output.flush(), () -> cf.getProcessor(uri).dataAppended(new WireCommands.DataAppended(cid, 2)));
        assertEquals(false, acked2.isCompletedExceptionally());
        assertEquals(true, acked2.isDone());
        order.verify(connection).send(new WireCommands.KeepAlive());
        order.verifyNoMoreInteractions();
    }

    @Test
    @Ignore
    public void testAutoClose() {
        fail();
    }

    @Test
    @Ignore
    public void testFailOnAutoClose() {
        fail();
    }

    @Test
    @Ignore
    public void testOutOfOrderAcks() {
        fail();
    }

    @Test
    public void testOverSizedWriteFails() throws ConnectionFailedException, SegmentSealedException {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl(uri);
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        @Cleanup
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, controller, cf, cid);
        output.setupConnection();
        verify(connection).send(new WireCommands.SetupAppend(1, cid, SEGMENT));
        cf.getProcessor(uri).appendSetup(new WireCommands.AppendSetup(1, SEGMENT, cid, 0));

        ByteBuffer data = ByteBuffer.allocate(PendingEvent.MAX_WRITE_SIZE + 1);
        CompletableFuture<Boolean> acked = new CompletableFuture<>();
        try {
            output.write(new PendingEvent("routingKey", 1, data, acked));
            fail("Did not throw");
        } catch (IllegalArgumentException e) {
            // expected
        }
        assertEquals(false, acked.isDone());
        verifyNoMoreInteractions(connection);
    }
    
    @Test
    public void testThrowsOnNegativeSequence() throws ConnectionFailedException, SegmentSealedException, InterruptedException, ExecutionException {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl(uri);
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, controller, cf, cid);
        output.setupConnection();
        verify(connection).send(new WireCommands.SetupAppend(1, cid, SEGMENT));
        cf.getProcessor(uri).appendSetup(new WireCommands.AppendSetup(1, SEGMENT, cid, -1));
        CompletableFuture<Boolean> acked = new CompletableFuture<>();
        AssertExtensions.assertThrows(IllegalArgumentException.class,
                                      () -> output.write(new PendingEvent(null, -1, getBuffer("test"), acked, null)));
        output.write(new PendingEvent(null, 0, getBuffer("test"), acked, null));
        assertEquals(false, acked.isDone());
        
    }
    
    @Test
    public void testConnectCanAckFutureWrites() throws ConnectionFailedException, SegmentSealedException, InterruptedException, ExecutionException {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", SERVICE_PORT);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl(uri);
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, controller, cf, cid);
        output.setupConnection();
        verify(connection).send(new WireCommands.SetupAppend(1, cid, SEGMENT));
        cf.getProcessor(uri).appendSetup(new WireCommands.AppendSetup(1, SEGMENT, cid, 100));

        CompletableFuture<Boolean> acked = new CompletableFuture<>();
        output.write(new PendingEvent(null, 1, getBuffer("test"), acked, null));
        assertEquals(true, acked.isDone());
        assertEquals(false, acked.get());
        acked = new CompletableFuture<>();
        output.write(new PendingEvent(null, 2, getBuffer("test"), acked, null));
        assertEquals(true, acked.isDone());
        assertEquals(false, acked.get());
        acked = new CompletableFuture<>();
        output.write(new PendingEvent(null, 3, getBuffer("test"), acked, null));
        assertEquals(true, acked.isDone());
        assertEquals(false, acked.get());
        verifyNoMoreInteractions(connection);
    }
}

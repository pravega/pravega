/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.emc.pravega.stream.impl.segment;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.junit.Ignore;
import org.junit.Test;
import org.mockito.InOrder;

import com.emc.pravega.common.netty.Append;
import com.emc.pravega.common.netty.ConnectionFailedException;
import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.common.netty.WireCommands.AppendSetup;
import com.emc.pravega.common.netty.WireCommands.DataAppended;
import com.emc.pravega.common.netty.WireCommands.KeepAlive;
import com.emc.pravega.common.netty.WireCommands.SetupAppend;
import com.emc.pravega.stream.impl.netty.ClientConnection;
import com.emc.pravega.stream.mock.MockConnectionFactoryImpl;
import com.emc.pravega.stream.mock.MockController;
import com.emc.pravega.testcommon.Async;

import io.netty.buffer.Unpooled;
import lombok.Cleanup;

public class SegmentOutputStreamTest {

    private static final String SEGMENT = "segment";

    private static ByteBuffer getBuffer(String s) {
        return ByteBuffer.wrap(s.getBytes());
    }

    @Test
    public void testConnectAndSend() throws SegmentSealedException, ConnectionFailedException {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", 1234);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl(uri);
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, controller, cf, cid);
        output.setupConnection();
        verify(connection).send(new SetupAppend(cid, SEGMENT));
        cf.getProcessor(uri).appendSetup(new AppendSetup(SEGMENT, cid, 0));

        sendAndVerifyEvent(cid, connection, output, getBuffer("test"), 1, null);
        verifyNoMoreInteractions(connection);
    }
    
    @Test
    public void testConditionalSend() throws SegmentSealedException, ConnectionFailedException {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", 1234);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl(uri);
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, controller, cf, cid);
        output.setupConnection();
        verify(connection).send(new SetupAppend(cid, SEGMENT));
        cf.getProcessor(uri).appendSetup(new AppendSetup(SEGMENT, cid, 0));

        sendAndVerifyEvent(cid, connection, output, getBuffer("test"), 1, 0L);
        verifyNoMoreInteractions(connection);
    }

    @Test(timeout = 20000)
    public void testNewEventsGoAfterInflight() throws ConnectionFailedException, SegmentSealedException {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", 1234);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl(uri);
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf);
        ClientConnection connection = mock(ClientConnection.class);
        InOrder inOrder = inOrder(connection);
        cf.provideConnection(uri, connection);
        @SuppressWarnings("resource")
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, controller, cf, cid);
        
        output.setupConnection();
        cf.getProcessor(uri).appendSetup(new AppendSetup(SEGMENT, cid, 0));
        output.write(getBuffer("test1"), new CompletableFuture<>());
        output.write(getBuffer("test2"), new CompletableFuture<>());
        cf.getProcessor(uri).connectionDropped();
        Async.testBlocking(() -> output.write(getBuffer("test3"), new CompletableFuture<>()),
                           () -> cf.getProcessor(uri).appendSetup(new AppendSetup(SEGMENT, cid, 0)));
        output.write(getBuffer("test4"), new CompletableFuture<>());
        
        inOrder.verify(connection).send(new SetupAppend(cid, SEGMENT));
        inOrder.verify(connection).send(new Append(SEGMENT, cid, 1, Unpooled.wrappedBuffer(getBuffer("test1")), null));
        inOrder.verify(connection).send(new Append(SEGMENT, cid, 2, Unpooled.wrappedBuffer(getBuffer("test2")), null));
        inOrder.verify(connection).close();
        inOrder.verify(connection).send(new SetupAppend(cid, SEGMENT));        
        inOrder.verify(connection).send(new Append(SEGMENT, cid, 1, Unpooled.wrappedBuffer(getBuffer("test1")), null));
        inOrder.verify(connection).send(new Append(SEGMENT, cid, 2, Unpooled.wrappedBuffer(getBuffer("test2")), null));
        inOrder.verify(connection).send(new Append(SEGMENT, cid, 3, Unpooled.wrappedBuffer(getBuffer("test3")), null));
        inOrder.verify(connection).send(new Append(SEGMENT, cid, 4, Unpooled.wrappedBuffer(getBuffer("test4")), null));
        
        verifyNoMoreInteractions(connection);
    }

    private void sendAndVerifyEvent(UUID cid, ClientConnection connection, SegmentOutputStreamImpl output,
            ByteBuffer data, int num, Long expectedLength) throws SegmentSealedException, ConnectionFailedException {
        CompletableFuture<Boolean> acked = new CompletableFuture<>();
        if (expectedLength == null) {
            output.write(data, acked);
        } else {
            output.conditionalWrite(expectedLength, data, acked);
        }
        verify(connection).send(new Append(SEGMENT, cid, num, Unpooled.wrappedBuffer(data), expectedLength));
        assertEquals(false, acked.isDone());
    }

    @Test
    public void testClose() throws ConnectionFailedException, SegmentSealedException, InterruptedException {
        UUID cid = UUID.randomUUID();
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", 1234);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl(uri);
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);

        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, controller, cf, cid);
        output.setupConnection();
        verify(connection).send(new SetupAppend(cid, SEGMENT));
        cf.getProcessor(uri).appendSetup(new AppendSetup(SEGMENT, cid, 0));
        ByteBuffer data = getBuffer("test");

        CompletableFuture<Boolean> acked = new CompletableFuture<>();
        output.write(data, acked);
        verify(connection).send(new Append(SEGMENT, cid, 1, Unpooled.wrappedBuffer(data), null));
        assertEquals(false, acked.isDone());
        Async.testBlocking(() -> output.close(), () -> cf.getProcessor(uri).dataAppended(new DataAppended(cid, 1)));
        assertEquals(false, acked.isCompletedExceptionally());
        assertEquals(true, acked.isDone());
        verify(connection).send(new KeepAlive());
        verify(connection).close();
        verifyNoMoreInteractions(connection);
    }

    @Test
    @Ignore
    public void testConnectionFailure() {
        fail();
    }

    @Test
    @Ignore
    public void testFlush() {
        fail();
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
        PravegaNodeUri uri = new PravegaNodeUri("endpoint", 1234);
        MockConnectionFactoryImpl cf = new MockConnectionFactoryImpl(uri);
        MockController controller = new MockController(uri.getEndpoint(), uri.getPort(), cf);
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection(uri, connection);
        @Cleanup
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, controller, cf, cid);
        output.setupConnection();
        verify(connection).send(new SetupAppend(cid, SEGMENT));
        cf.getProcessor(uri).appendSetup(new AppendSetup(SEGMENT, cid, 0));

        ByteBuffer data = ByteBuffer.allocate(SegmentOutputStream.MAX_WRITE_SIZE + 1);
        CompletableFuture<Boolean> acked = new CompletableFuture<>();
        try {
            output.write(data, acked);
            fail("Did not throw");
        } catch (IllegalArgumentException e) {
            // expected
        }
        assertEquals(false, acked.isDone());
        verifyNoMoreInteractions(connection);
    }
}

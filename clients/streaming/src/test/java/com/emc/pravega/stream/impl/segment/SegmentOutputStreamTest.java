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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Ignore;
import org.junit.Test;

import com.emc.pravega.common.netty.ClientConnection;
import com.emc.pravega.common.netty.ConnectionFailedException;
import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.common.netty.WireCommands.Append;
import com.emc.pravega.common.netty.WireCommands.AppendSetup;
import com.emc.pravega.common.netty.WireCommands.DataAppended;
import com.emc.pravega.common.netty.WireCommands.KeepAlive;
import com.emc.pravega.common.netty.WireCommands.SetupAppend;
import com.emc.pravega.stream.mock.MockController;
import com.emc.pravega.stream.mock.MockConnectionFactoryImpl;

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

        sendEvent(cid, connection, output, getBuffer("test"), 1);
        verifyNoMoreInteractions(connection);
    }

    @Test
    public void testNewEventsGoAfterInflight() throws ConnectionFailedException, SegmentSealedException {
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

        sendEvent(cid, connection, output, getBuffer("test"), 1);
        verifyNoMoreInteractions(connection);
    }

    private void sendEvent(UUID cid, ClientConnection connection, SegmentOutputStreamImpl output, ByteBuffer data,
                           int num) throws SegmentSealedException, ConnectionFailedException {
        CompletableFuture<Void> acked = new CompletableFuture<>();
        output.write(data, acked);
        verify(connection).send(new Append(SEGMENT, cid, num, Unpooled.wrappedBuffer(data)));
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

        CompletableFuture<Void> acked = new CompletableFuture<>();
        output.write(data, acked);
        verify(connection).send(new Append(SEGMENT, cid, 1, Unpooled.wrappedBuffer(data)));
        assertEquals(false, acked.isDone());
        final AtomicReference<Exception> ex = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                output.close(); // should block
            } catch (Exception e) {
                ex.set(e);
            }
        });
        t.start();
        t.join(1000);
        assertNull(ex.get());
        assertEquals(true, t.isAlive());
        assertEquals(false, acked.isDone());
        cf.getProcessor(uri).dataAppended(new DataAppended(cid, 1));
        t.join(1000);
        assertNull(ex.get());
        assertEquals(false, t.isAlive());
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
        @Cleanup SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(SEGMENT, controller, cf, cid);
        output.setupConnection();
        verify(connection).send(new SetupAppend(cid, SEGMENT));
        cf.getProcessor(uri).appendSetup(new AppendSetup(SEGMENT, cid, 0));

        ByteBuffer data = ByteBuffer.allocate(SegmentOutputStream.MAX_WRITE_SIZE + 1);
        CompletableFuture<Void> acked = new CompletableFuture<>();
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

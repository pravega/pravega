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
package com.emc.pravega.stream.segment.impl;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Ignore;
import org.junit.Test;

import com.emc.pravega.common.netty.ClientConnection;
import com.emc.pravega.common.netty.ConnectionFactory;
import com.emc.pravega.common.netty.ConnectionFailedException;
import com.emc.pravega.common.netty.ReplyProcessor;
import com.emc.pravega.common.netty.WireCommands.Append;
import com.emc.pravega.common.netty.WireCommands.AppendSetup;
import com.emc.pravega.common.netty.WireCommands.DataAppended;
import com.emc.pravega.common.netty.WireCommands.KeepAlive;
import com.emc.pravega.common.netty.WireCommands.SetupAppend;
import com.emc.pravega.stream.segment.SegmentOutputStream;
import com.emc.pravega.stream.segment.SegmentSealedExcepetion;
import com.emc.pravega.stream.segment.impl.SegmentOutputStreamImpl;

import static org.junit.Assert.*;

import static org.mockito.Mockito.*;

import io.netty.buffer.Unpooled;
import lombok.Cleanup;
import lombok.Synchronized;

public class SegmentOutputStreamTest {

    private static final String SEGMENT = "segment";

    private static class TestConnectionFactoryImpl implements ConnectionFactory {
        Map<String, ClientConnection> connections = new HashMap<>();
        Map<String, ReplyProcessor> processors = new HashMap<>();

        @Override
        @Synchronized
        public ClientConnection establishConnection(String endpoint, ReplyProcessor rp) {
            ClientConnection connection = connections.get(endpoint);
            if (connection == null) {
                throw new IllegalStateException("Unexpected Endpoint");
            }
            processors.put(endpoint, rp);
            return connection;
        }

        @Synchronized
        void provideConnection(String endpoint, ClientConnection c) {
            connections.put(endpoint, c);
        }

        @Synchronized
        ReplyProcessor getProcessor(String endpoint) {
            return processors.get(endpoint);
        }

        @Override
        public void close() {
        }
    }

    private static ByteBuffer getBuffer(String s) {
        return ByteBuffer.wrap(s.getBytes());
    }

    @Test
    public void testConnectAndSend() throws SegmentSealedExcepetion, ConnectionFailedException {
        UUID cid = UUID.randomUUID();
        TestConnectionFactoryImpl cf = new TestConnectionFactoryImpl();
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection("endpoint", connection);
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(cf, "endpoint", cid, SEGMENT);
        output.connect();
        verify(connection).send(new SetupAppend(cid, SEGMENT));
        cf.getProcessor("endpoint").appendSetup(new AppendSetup(SEGMENT, cid, 0));

        sendEvent(cid, connection, output, getBuffer("test"), 1);
        verifyNoMoreInteractions(connection);
    }

    @Test
    public void testNewEventsGoAfterInflight() throws ConnectionFailedException, SegmentSealedExcepetion {
        UUID cid = UUID.randomUUID();
        TestConnectionFactoryImpl cf = new TestConnectionFactoryImpl();
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection("endpoint", connection);
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(cf, "endpoint", cid, SEGMENT);
        output.connect();
        verify(connection).send(new SetupAppend(cid, SEGMENT));
        cf.getProcessor("endpoint").appendSetup(new AppendSetup(SEGMENT, cid, 0));

        sendEvent(cid, connection, output, getBuffer("test"), 1);
        verifyNoMoreInteractions(connection);
    }

    private void sendEvent(UUID cid, ClientConnection connection, SegmentOutputStreamImpl output, ByteBuffer data,
            int num) throws SegmentSealedExcepetion, ConnectionFailedException {
        CompletableFuture<Void> acked = new CompletableFuture<>();
        output.write(data, acked);
        verify(connection).send(new Append(SEGMENT, cid, num, Unpooled.wrappedBuffer(data)));
        assertEquals(false, acked.isDone());
    }

    @Test
    public void testClose() throws ConnectionFailedException, SegmentSealedExcepetion, InterruptedException {
        UUID cid = UUID.randomUUID();
        TestConnectionFactoryImpl cf = new TestConnectionFactoryImpl();
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection("endpoint", connection);

        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(cf, "endpoint", cid, SEGMENT);
        output.connect();
        verify(connection).send(new SetupAppend(cid, SEGMENT));
        cf.getProcessor("endpoint").appendSetup(new AppendSetup(SEGMENT, cid, 0));
        ByteBuffer data = getBuffer("test");

        CompletableFuture<Void> acked = new CompletableFuture<>();
        output.write(data, acked);
        verify(connection).send(new Append(SEGMENT, cid, 1, Unpooled.wrappedBuffer(data)));
        assertEquals(false, acked.isDone());
        final AtomicReference<Exception> ex = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                output.close(); //should block
            } catch (Exception e) {
                ex.set(e);
            } 
        });
        t.start();
        t.join(1000);
        assertNull(ex.get());
        assertEquals(true, t.isAlive());
        assertEquals(false, acked.isDone());
        cf.getProcessor("endpoint").dataAppended(new DataAppended(cid, 1));
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
    public void testOverSizedWriteFails() throws ConnectionFailedException, SegmentSealedExcepetion {
        UUID cid = UUID.randomUUID();
        TestConnectionFactoryImpl cf = new TestConnectionFactoryImpl();
        ClientConnection connection = mock(ClientConnection.class);
        cf.provideConnection("endpoint", connection);
        @Cleanup
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(cf, "endpoint", cid, SEGMENT);
        output.connect();
        verify(connection).send(new SetupAppend(cid, SEGMENT));
        cf.getProcessor("endpoint").appendSetup(new AppendSetup(SEGMENT, cid, 0));

        ByteBuffer data = ByteBuffer.allocate(SegmentOutputStream.MAX_WRITE_SIZE + 1);
        CompletableFuture<Void> acked = new CompletableFuture<>();
        try {            
            output.write(data, acked);
            fail("Did not throw");
        } catch (IllegalArgumentException e){
            //expected
        }
        assertEquals(false, acked.isDone());
        verifyNoMoreInteractions(connection);
    }
}

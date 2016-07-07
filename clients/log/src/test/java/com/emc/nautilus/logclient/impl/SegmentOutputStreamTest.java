package com.emc.nautilus.logclient.impl;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.junit.Test;

import com.emc.nautilus.common.netty.ClientConnection;
import com.emc.nautilus.common.netty.ConnectionFactory;
import com.emc.nautilus.common.netty.ConnectionFailedException;
import com.emc.nautilus.common.netty.ReplyProcessor;
import com.emc.nautilus.common.netty.WireCommands.AppendData;
import com.emc.nautilus.common.netty.WireCommands.AppendSetup;
import com.emc.nautilus.common.netty.WireCommands.SetupAppend;
import com.emc.nautilus.logclient.SegmentSealedExcepetion;
import com.emc.nautilus.logclient.impl.SegmentOutputStreamImpl;

import static org.junit.Assert.*;

import static org.mockito.Mockito.*;

import io.netty.buffer.Unpooled;
import lombok.Synchronized;

public class SegmentOutputStreamTest {

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
        public void shutdown() {
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
        SegmentOutputStreamImpl output = new SegmentOutputStreamImpl(cf, "endpoint", cid, "segment");
        output.connect();
        verify(connection).send(new SetupAppend(cid, "segment"));
        cf.getProcessor("endpoint").appendSetup(new AppendSetup("segment", cid, 0));
        
        CompletableFuture<Void> acked = new CompletableFuture<>();
        ByteBuffer data = getBuffer("test");
        output.write(data, acked);
        verify(connection).send(new AppendData(cid, data.array().length, Unpooled.wrappedBuffer(data)));
        verifyNoMoreInteractions(connection);
        assertEquals(false, acked.isDone());
    }

    @Test
    public void testNewEventsGoAfterInflight() {
        fail();
    }

    @Test
    public void testClose() {
        fail();
    }

    @Test
    public void testFlush() {
        fail();
    }

    @Test
    public void testAutoClose() {
        fail();
    }

    @Test
    public void testFailOnAutoClose() {
        fail();
    }

    @Test
    public void testOutOfOrderAcks() {
        fail();
    }

    @Test
    public void testLargeWrite() {
        fail();
    }
}

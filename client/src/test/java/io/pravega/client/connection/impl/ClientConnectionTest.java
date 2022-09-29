/**
 * Copyright Pravega Authors.
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
package io.pravega.client.connection.impl;

import com.google.common.collect.ImmutableList;
import io.netty.buffer.Unpooled;
import io.pravega.client.ClientConfig;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.FailingReplyProcessor;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommandType;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.AppendBlock;
import io.pravega.shared.protocol.netty.WireCommands.AppendBlockEnd;
import io.pravega.shared.protocol.netty.WireCommands.Event;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.InlineExecutor;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;

public class ClientConnectionTest {

    private static class ReplyProcessor extends FailingReplyProcessor {
        AtomicBoolean failure = new AtomicBoolean(false);
        List<Reply> replies = new Vector<>();

        @Override
        public void process(Reply reply) {
            if (reply.isFailure()) {
                failure.set(true);
            }
            replies.add(reply);
        }
        
        @Override
        public void processingFailure(Exception error) {
            failure.set(true);
        }

        @Override
        public void connectionDropped() {
            failure.set(true);
        }
    }

    @Test(timeout = 10000)
    public void testConnectionSetup() throws Exception {
        ReplyProcessor processor = new ReplyProcessor();
        @Cleanup
        MockServer server = new MockServer();
        server.start();
        @Cleanup
        InlineExecutor executor = new InlineExecutor();
        @Cleanup
        ClientConnection clientConnection = TcpClientConnection
            .connect(server.getUri(), ClientConfig.builder().build(), processor, executor, null)
            .join();
        clientConnection.send(new WireCommands.Hello(0, 1));
        LinkedBlockingQueue<WireCommand> messages = server.getReadCommands();
        WireCommand wireCommand = messages.take();
        assertEquals(WireCommandType.HELLO, wireCommand.getType());
        assertNull(messages.poll());
        clientConnection.send(new WireCommands.SetupAppend(1, new UUID(1, 2), "segment", ""));
        wireCommand = messages.take();
        assertEquals(WireCommandType.SETUP_APPEND, wireCommand.getType());
        assertNull(messages.poll());
        clientConnection.send(new Append("segment", new UUID(1, 2), 1, new Event(Unpooled.EMPTY_BUFFER), 2));
        wireCommand = messages.take();
        assertEquals(WireCommandType.APPEND_BLOCK, wireCommand.getType());
        assertFalse(processor.failure.get());
    }

    @Test
    public void testAppendThrows() throws Exception {
        ReplyProcessor processor = new ReplyProcessor();
        @Cleanup
        MockServer server = new MockServer();
        server.start();
        @Cleanup
        InlineExecutor executor = new InlineExecutor();
        @Cleanup
        ClientConnection clientConnection = TcpClientConnection
            .connect(server.getUri(), ClientConfig.builder().build(), processor, executor, null)
            .join();
        clientConnection.send(new WireCommands.SetupAppend(1, new UUID(1, 2), "segment", ""));
        clientConnection.send(new Append("segment", new UUID(1, 2), 1, new Event(Unpooled.EMPTY_BUFFER), 2));
        server.sendReply(new WireCommands.AuthTokenCheckFailed(1, "Injected error"));
        AssertExtensions.assertEventuallyEquals(true, () -> processor.failure.get(), 5000);
    }

    @Test
    public void testAppend() throws Exception {
        byte[] payload = new byte[100];
        ReplyProcessor processor = new ReplyProcessor();
        @Cleanup
        MockServer server = new MockServer();
        server.start();
        @Cleanup
        InlineExecutor executor = new InlineExecutor();
        @Cleanup
        ClientConnection clientConnection = TcpClientConnection
            .connect(server.getUri(), ClientConfig.builder().build(), processor, executor, null)
            .join();
        UUID writerId = new UUID(1, 2);
        clientConnection.send(new WireCommands.SetupAppend(1, writerId, "segment", ""));
        for (int i = 0; i < 100; i++) {
            clientConnection.send(new Append("segment", writerId, i, new Event(Unpooled.wrappedBuffer(payload)), 1));
            server.sendReply(new WireCommands.DataAppended(i, writerId, i, i - 1, i * 100));
        }
        AssertExtensions.assertEventuallyEquals(100, () -> processor.replies.size(), 5000);
        assertFalse(processor.failure.get());
    }
    
    @Test
    public void testFailedSendAppend() throws Exception {
        byte[] payload = new byte[100];
        ReplyProcessor processor = new ReplyProcessor();
        @Cleanup
        MockServer server = new MockServer();
        server.start();
        @Cleanup
        InlineExecutor executor = new InlineExecutor();
        @Cleanup
        TcpClientConnection clientConnection = TcpClientConnection
            .connect(server.getUri(), ClientConfig.builder().build(), processor, executor, null)
            .join();
        UUID writerId = new UUID(1, 2);
        clientConnection.send(new WireCommands.SetupAppend(1, writerId, "segment", ""));
        server.getOutputStream().join().close();
        AssertExtensions.assertThrows(ConnectionFailedException.class, () -> {
            for (int i = 0; i < 100; i++) {
                clientConnection.send(new Append("segment", writerId, i, new Event(Unpooled.wrappedBuffer(payload)), 1));
                Thread.sleep(100);
            }
        });
        assertTrue(clientConnection.toString(), clientConnection.isClosed());
        AssertExtensions.assertThrows(ConnectionFailedException.class,
                () -> clientConnection.send(new Append("segment", writerId, 100, new Event(Unpooled.wrappedBuffer(payload)), 1)));
    }
    
    @Test(timeout = 15000)
    public void testFailedSendSetup() throws Exception {
        ReplyProcessor processor = new ReplyProcessor();
        @Cleanup
        MockServer server = new MockServer();
        server.start();
        @Cleanup
        InlineExecutor executor = new InlineExecutor();
        @Cleanup
        TcpClientConnection clientConnection = TcpClientConnection
            .connect(server.getUri(), ClientConfig.builder().build(), processor, executor, null)
            .join();
        server.getOutputStream().join().close();
        AssertExtensions.assertThrows(ConnectionFailedException.class, () -> {
            for (int i = 0; i < 100; i++) {
                clientConnection.send(new WireCommands.KeepAlive());
                Thread.sleep(100);
            }
        });
        assertTrue(clientConnection.toString(), clientConnection.isClosed());
        AssertExtensions.assertThrows(ConnectionFailedException.class, () -> clientConnection.send(new WireCommands.KeepAlive()));

    }
    
    @Test(timeout = 15000)
    public void testFailedSendAsync() throws Exception {
        byte[] payload = new byte[100];
        ReplyProcessor processor = new ReplyProcessor();
        @Cleanup
        MockServer server = new MockServer();
        server.start();
        @Cleanup
        InlineExecutor executor = new InlineExecutor();
        @Cleanup
        TcpClientConnection clientConnection = TcpClientConnection
            .connect(server.getUri(), ClientConfig.builder().build(), processor, executor, null)
            .join();
        UUID writerId = new UUID(1, 2);
        clientConnection.send(new WireCommands.SetupAppend(1, writerId, "segment", ""));
        server.getOutputStream().join().close();
        AssertExtensions.assertThrows(ConnectionFailedException.class, () -> {
            for (int i = 0; i < 100; i++) {
                CompletableFuture<Exception> future = new CompletableFuture<>();
                List<Append> events = ImmutableList.of(new Append("segment", writerId, i, new Event(Unpooled.wrappedBuffer(payload)), 1));
                clientConnection.sendAsync(events, e -> future.complete(e));
                Exception exception = future.join();
                if (exception != null) {
                    throw exception;
                }
                Thread.sleep(100);
            }
        });
    }
    
    @Test
    public void testAckProcessingFailure() throws Exception {
        byte[] payload = new byte[100];
        ReplyProcessor processor = new ReplyProcessor() {
            @Override
            public void process(Reply reply) {
                throw new RuntimeException("Injected error");
            }
        };
        @Cleanup
        MockServer server = new MockServer();
        server.start();
        @Cleanup
        InlineExecutor executor = new InlineExecutor();
        @Cleanup
        ClientConnection clientConnection = TcpClientConnection
            .connect(server.getUri(), ClientConfig.builder().build(), processor, executor, null)
            .join();
        UUID writerId = new UUID(1, 2);
        clientConnection.send(new WireCommands.SetupAppend(1, writerId, "segment", ""));
        clientConnection.send(new Append("segment", writerId, 1, new Event(Unpooled.wrappedBuffer(payload)), 1));
        server.sendReply(new WireCommands.DataAppended(1, writerId, 1, 0, 100));
        AssertExtensions.assertEventuallyEquals(true, () -> processor.failure.get(), 5000);
    }
    
    @Test
    public void testSendAsync() throws Exception {
        byte[] payload = new byte[100];
        byte[] payload2 = new byte[101];
        ReplyProcessor processor = new ReplyProcessor();
        @Cleanup
        MockServer server = new MockServer();
        server.start();
        @Cleanup
        InlineExecutor executor = new InlineExecutor();
        @Cleanup
        ClientConnection clientConnection = TcpClientConnection
            .connect(server.getUri(), ClientConfig.builder().build(), processor, executor, null)
            .join();
        UUID writerId = new UUID(1, 2);
        clientConnection.send(new WireCommands.SetupAppend(1, writerId, "segment", ""));
        server.getReadCommands().take(); //clear setup.
        ArrayList<Append> appends = new ArrayList<>();
        appends.add(new Append("segment", writerId, 1, new Event(Unpooled.wrappedBuffer(payload)), 1));
        appends.add(new Append("segment", writerId, 2, new Event(Unpooled.wrappedBuffer(payload2)), 1));
        CompletableFuture<Exception> future = new CompletableFuture<Exception>();
        clientConnection.sendAsync(appends, e -> future.complete(e));
        assertNull(future.join());
        assertEquals(AppendBlock.class, server.getReadCommands().take().getClass());
        assertEquals(AppendBlockEnd.class, server.getReadCommands().take().getClass());
        assertEquals(AppendBlock.class, server.getReadCommands().take().getClass());
        assertEquals(AppendBlockEnd.class, server.getReadCommands().take().getClass());
        assertTrue(server.getReadCommands().isEmpty());
        assertFalse(processor.failure.get());
    }

    /**
     * This test verifies that the number of entries in the {@link FlowToBatchSizeTracker} object of a connection is
     * driven by the number of flows, not by the different ids from the requests.
     *
     * @throws Exception
     */
    @Test
    public void tcpClientConnectionBatchSizeTrackerUniquenessTest() throws Exception {
        byte[] payload = new byte[100];
        ClientConnectionTest.ReplyProcessor processor = new ClientConnectionTest.ReplyProcessor();
        @Cleanup
        MockServer server = new MockServer();
        server.start();
        @Cleanup
        InlineExecutor executor = new InlineExecutor();
        @Cleanup
        TcpClientConnection clientConnection = TcpClientConnection
                .connect(server.getUri(), ClientConfig.builder().build(), processor, executor, null)
                .join();
        UUID writerId = new UUID(1, 1);
        clientConnection.send(new WireCommands.SetupAppend(1, writerId, "segment", ""));
        server.getReadCommands().take(); //clear setup.
        // Check that the map is initially empty.
        Assert.assertEquals(0, clientConnection.getConnectionReaderFlowToBatchSizeTracker().getFlowToBatchSizeTrackerMap().size());
        // Create one flow and increase its sequence number on each append.
        Flow firstFlow = Flow.create();
        for (int i = 0; i < 100; i++) {
            clientConnection.send(new Append("segment", writerId, i, new Event(Unpooled.wrappedBuffer(payload)), firstFlow.asLong()));
            // Assert that the map always contains one entry, as there is only one flow (with increasing sequence number).
            Assert.assertEquals(1, clientConnection.getConnectionReaderFlowToBatchSizeTracker().getFlowToBatchSizeTrackerMap().size());
            firstFlow.getNextSequenceNumber();
        }
        // Start another writer on the same connection.
        UUID writerId2 = new UUID(1, 2);
        clientConnection.send(new WireCommands.SetupAppend(1, writerId2, "segment2", ""));
        // Create another flow.
        Flow secondFlow = Flow.create();
        // Check that with another flow appending, there are just 2 flows in the batch size tracker map.
        clientConnection.send(new Append("segment2", writerId2, 0, new Event(Unpooled.wrappedBuffer(payload)), secondFlow.getNextSequenceNumber()));
        Assert.assertEquals(2, clientConnection.getConnectionReaderFlowToBatchSizeTracker().getFlowToBatchSizeTrackerMap().size());
        clientConnection.send(new Append("segment2", writerId2, 1, new Event(Unpooled.wrappedBuffer(payload)), secondFlow.getNextSequenceNumber()));
        Assert.assertEquals(2, clientConnection.getConnectionReaderFlowToBatchSizeTracker().getFlowToBatchSizeTrackerMap().size());
    }

    @Test
    public void testGetLocation() throws Exception {
        ClientConnectionTest.ReplyProcessor processor = new ClientConnectionTest.ReplyProcessor();
        @Cleanup
        MockServer server = new MockServer();
        server.start();
        @Cleanup
        InlineExecutor executor = new InlineExecutor();
        @Cleanup
        TcpClientConnection clientConnection = TcpClientConnection
                .connect(server.getUri(), ClientConfig.builder().build(), processor, executor, null)
                .join();
        Assert.assertEquals("localhost", clientConnection.getLocation().getEndpoint());
    }

    @Test
    public void testGetLocationWithSpy() {
        ClientConnection mockConnection = spy(ClientConnection.class);
        assertNull("location returned is not null!", mockConnection.getLocation());
    }
}

/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.connection.impl;

import io.netty.buffer.Unpooled;
import io.pravega.client.ClientConfig;
import io.pravega.shared.protocol.netty.Append;
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
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ClientConnectionTest {

    private static class ReplyProcessor extends FailingReplyProcessor {
        AtomicBoolean falure = new AtomicBoolean(false);
        List<Reply> replies = new Vector<>();

        @Override
        public void process(Reply reply) {
            if (reply.isFailure()) {
                falure.set(true);
            }
            replies.add(reply);
        }
        
        @Override
        public void processingFailure(Exception error) {
            falure.set(true);
        }

        @Override
        public void connectionDropped() {
            falure.set(true);
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
        assertFalse(processor.falure.get());
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
        AssertExtensions.assertEventuallyEquals(true, () -> processor.falure.get(), 5000);
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
        assertFalse(processor.falure.get());
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
        AssertExtensions.assertEventuallyEquals(true, () -> processor.falure.get(), 5000);
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
        assertFalse(processor.falure.get());
    }

}

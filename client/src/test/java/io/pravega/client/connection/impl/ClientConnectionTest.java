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
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommandType;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.AuthTokenCheckFailed;
import io.pravega.shared.protocol.netty.WireCommands.Event;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.InlineExecutor;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class ClientConnectionTest {

    private static class ReplyProcessor extends FailingReplyProcessor {
        AtomicBoolean falure = new AtomicBoolean(false);

        @Override
        public void processingFailure(Exception error) {
            falure.set(true);
        }

        @Override
        public void connectionDropped() {
            falure.set(true);
        }

        @Override
        public void authTokenCheckFailed(AuthTokenCheckFailed authTokenCheckFailed) {
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

}

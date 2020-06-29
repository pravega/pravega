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

    //    @Test
    //    public void testAppendThrows() throws Exception {
    //        ReplyProcessor processor = new ReplyProcessor(); 
    //        Flow flow = new Flow(10, 0);
    //        FlowHandler flowHandler = new FlowHandler("testConnection");
    //        @Cleanup
    //        ClientConnection clientConnection = flowHandler.createFlow(flow, processor);
    //        EmbeddedChannel embeddedChannel = createChannelWithContext(flowHandler);
    //        embeddedChannel.runScheduledPendingTasks();
    //        embeddedChannel.runPendingTasks();
    //        Queue<Object> messages = embeddedChannel.outboundMessages();
    //        assertEquals(1, messages.size());
    //        clientConnection.send(new WireCommands.SetupAppend(1, new UUID(1, 2), "segment", ""));
    //        embeddedChannel.runPendingTasks();
    //        clientConnection.send(new Append("segment", new UUID(1, 2), 1, new Event(Unpooled.EMPTY_BUFFER), 2));
    //        embeddedChannel.disconnect();
    //        embeddedChannel.runPendingTasks();
    //        assertTrue(processor.falure.get());
    //    }
    //
    //    
    //    static EmbeddedChannel createChannelWithContext(ChannelInboundHandlerAdapter handler) {
    //        return new EmbeddedChannel(new ExceptionLoggingHandler(""), new CommandEncoder(null, NO_OP_METRIC_NOTIFIER),
    //                                   new LengthFieldBasedFrameDecoder(MAX_WIRECOMMAND_SIZE, 4, 4), new CommandDecoder(),
    //                                   new AppendDecoder(), handler);
    //    }
    //    
    //    @Test
    //    public void testThread() throws ConnectionFailedException {
    //        ReplyProcessor processor = new ReplyProcessor(); 
    //        Flow flow = new Flow(10, 0);
    //        FlowHandler flowHandler = new FlowHandler("testConnection");
    //        byte[] payload = new byte[100];
    //        @Cleanup
    //        ClientConnection clientConnection = flowHandler.createFlow(flow, processor);
    //        List<String> threadIds = new Vector<>();
    //        ChannelDuplexHandler spy = new ChannelDuplexHandler() {
    //            @Override
    //            public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    //                threadIds.add(Thread.currentThread().getName());
    //                super.write(ctx, msg, promise);
    //            }
    //
    //            @Override
    //            public void flush(ChannelHandlerContext ctx) throws Exception {
    //                threadIds.add(Thread.currentThread().getName());
    //                super.flush(ctx);
    //            }
    //        };
    //        EmbeddedChannel embeddedChannel = new EmbeddedChannel(spy, new CommandEncoder(null, NO_OP_METRIC_NOTIFIER),
    //                                                              new LengthFieldBasedFrameDecoder(MAX_WIRECOMMAND_SIZE, 4, 4), new CommandDecoder(),
    //                                                              new AppendDecoder(), flowHandler);
    //        clientConnection.send(new WireCommands.SetupAppend(1, new UUID(1, 2), "segment", ""));
    //        for (int i = 0; i < 100; i++) {
    //            clientConnection.send(new Append("segment", new UUID(1, 2), i, new Event(Unpooled.wrappedBuffer(payload)), 2));
    //            embeddedChannel.runPendingTasks();
    //        }
    //        assertTrue(threadIds.size() > 100);
    //        Queue<Object> messages = embeddedChannel.outboundMessages();
    //        assertEquals(102, messages.size());
    //        assertFalse(processor.falure.get());
    //        HashSet<String> uniqueThreads = new HashSet<>();
    //        for (String threadId : threadIds) {
    //            uniqueThreads.add(threadId);
    //        }
    //        assertEquals(1, uniqueThreads.size());
    //    }

}

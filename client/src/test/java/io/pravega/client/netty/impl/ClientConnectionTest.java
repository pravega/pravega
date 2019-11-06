/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.netty.impl;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.AppendDecoder;
import io.pravega.shared.protocol.netty.CommandDecoder;
import io.pravega.shared.protocol.netty.CommandEncoder;
import io.pravega.shared.protocol.netty.ExceptionLoggingHandler;
import io.pravega.shared.protocol.netty.FailingReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.AuthTokenCheckFailed;
import io.pravega.shared.protocol.netty.WireCommands.Event;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Cleanup;
import org.junit.Test;

import static io.pravega.shared.protocol.netty.WireCommands.MAX_WIRECOMMAND_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ClientConnectionTest {

    @Test
    public void testConnectionSetup() throws Exception {
        AtomicBoolean falure = new AtomicBoolean(false);
        FailingReplyProcessor processor = new FailingReplyProcessor() {

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
        };
        Flow flow = new Flow(10, 0);
        FlowHandler flowHandler = new FlowHandler("testConnection");
        @Cleanup
        ClientConnection clientConnection = flowHandler.createFlow(flow, processor);
        EmbeddedChannel embeddedChannel = createChannelWithContext(flowHandler);
        embeddedChannel.runScheduledPendingTasks();
        embeddedChannel.runPendingTasks();
        Queue<Object> messages = embeddedChannel.outboundMessages();
        assertEquals(1, messages.size());
        clientConnection.send(new WireCommands.SetupAppend(1, new UUID(1,2), "segment", ""));
        embeddedChannel.runPendingTasks();
        messages = embeddedChannel.outboundMessages();
        assertEquals(2, messages.size());
        clientConnection.send(new Append("segment", new UUID(1, 2), 1, new Event(Unpooled.EMPTY_BUFFER), 2));
        embeddedChannel.runPendingTasks();
        messages = embeddedChannel.outboundMessages();
        assertEquals(3, messages.size());
        assertFalse(falure.get());
    }

    static EmbeddedChannel createChannelWithContext(ChannelInboundHandlerAdapter handler) {
        return new EmbeddedChannel(new ExceptionLoggingHandler(""), new CommandEncoder(null),
                                   new LengthFieldBasedFrameDecoder(MAX_WIRECOMMAND_SIZE, 4, 4), new CommandDecoder(),
                                   new AppendDecoder(), handler);
    }
}

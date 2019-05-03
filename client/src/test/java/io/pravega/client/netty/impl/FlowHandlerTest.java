/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.netty.impl;


import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.concurrent.Futures;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.AppendBatchSizeTracker;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommands;
import java.io.IOException;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static java.lang.String.valueOf;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FlowHandlerTest {

    @Rule
    public Timeout globalTimeout = Timeout.seconds(15);

    private Flow flow;
    private FlowHandler flowHandler;
    @Mock
    private ReplyProcessor processor;
    @Mock
    private AppendBatchSizeTracker tracker;
    @Mock
    private Append appendCmd;
    @Mock
    private ChannelHandlerContext ctx;
    @Mock
    private ByteBuf buffer;
    @Mock
    private Channel ch;
    @Mock
    private EventLoop loop;
    @Mock
    private ChannelFuture completedFuture;

    @BeforeClass
    public static void beforeClass() {
        System.setProperty("pravega.client.netty.channel.timeout.millis", valueOf(SECONDS.toMillis(5)));
    }

    @Before
    public void setUp() throws Exception {
        flow = new Flow(10, 0);
        when(buffer.readableBytes()).thenReturn(10);
        appendCmd = new Append("segment0", UUID.randomUUID(), 2, 1, buffer, 10L, flow.asLong());
        doNothing().when(tracker).recordAppend(anyLong(), anyInt());

        when(ctx.channel()).thenReturn(ch);
        when(ch.eventLoop()).thenReturn(loop);
        when(ch.writeAndFlush(any(Object.class))).thenReturn(completedFuture);

        flowHandler = new FlowHandler("testConnection", tracker);
    }

    @Test
    public void sendNormal() throws Exception {
        @Cleanup
        ClientConnection clientConnection = flowHandler.createFlow(flow, processor);
        // channelRegistered is invoked before send is invoked.
        // No exceptions are expected here.
        flowHandler.channelRegistered(ctx);
        clientConnection.send(appendCmd);
    }

    @Test(expected = ConnectionFailedException.class)
    public void sendError() throws Exception {
        @Cleanup
        ClientConnection clientConnection = flowHandler.createFlow(flow, processor);
        //Send function is invoked without channel registered being invoked.
        //this causes a connectionFailed exception.
        clientConnection.send(appendCmd);
    }

    @Test(expected = ConnectionFailedException.class)
    public void sendErrorUnRegistered() throws Exception {
        @Cleanup
        ClientConnection clientConnection = flowHandler.createFlow(flow, processor);
        //any send after channelUnregistered should throw a ConnectionFailedException.
        flowHandler.channelRegistered(ctx);
        flowHandler.channelUnregistered(ctx);
        clientConnection.send(appendCmd);
    }

    @Test
    public void completeWhenRegisteredNormal() throws Exception {
        flowHandler.channelRegistered(ctx);
        CompletableFuture<Void> testFuture = new CompletableFuture<>();
        flowHandler.completeWhenRegistered(testFuture);
        Assert.assertTrue(Futures.isSuccessful(testFuture));
    }

    @Test
    public void completeWhenRegisteredDelayed() throws Exception {
        @Cleanup
        ClientConnection clientConnection = flowHandler.createFlow(flow, processor);
        CompletableFuture<Void> testFuture = new CompletableFuture<>();
        flowHandler.completeWhenRegistered(testFuture);
        flowHandler.channelRegistered(ctx);
        Assert.assertTrue(Futures.isSuccessful(testFuture));
    }

    @Test
    public void completeWhenRegisteredDelayedMultiple() throws Exception {
        CompletableFuture<Void> testFuture = new CompletableFuture<>();
        flowHandler.completeWhenRegistered(testFuture);

        CompletableFuture<Void> testFuture1 = new CompletableFuture<>();
        flowHandler.completeWhenRegistered(testFuture1);

        flowHandler.channelRegistered(ctx);

        Assert.assertTrue(Futures.isSuccessful(testFuture));
        testFuture1.get(); //wait until additional future is complete.
        Assert.assertTrue(Futures.isSuccessful(testFuture1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void createDuplicateSession() throws Exception {
        Flow flow = new Flow(11, 0);
        ClientConnection connection1 = flowHandler.createFlow(flow, processor);
        flowHandler.channelRegistered(ctx);
        connection1.send(appendCmd);
        // Creating a flow with the same flow id.
        flowHandler.createFlow(flow, processor);
    }

    @Test
    public void testCloseSession() throws Exception {
        @Cleanup
        ClientConnection clientConnection = flowHandler.createFlow(flow, processor);
        flowHandler.channelRegistered(ctx);
        clientConnection.send(appendCmd);
        flowHandler.closeFlow(clientConnection);
        assertEquals(0, flowHandler.getFlowIdReplyProcessorMap().size());
    }

    @Test
    public void testCloseSessionHandler() throws Exception {
        @Cleanup
        ClientConnection clientConnection = flowHandler.createFlow(flow, processor);
        flowHandler.channelRegistered(ctx);
        WireCommands.GetSegmentAttribute cmd = new WireCommands.GetSegmentAttribute(flow.asLong(), "seg", UUID.randomUUID(), "");
        clientConnection.sendAsync(cmd, e -> fail("Exception while invoking sendAsync"));
        flowHandler.close();
        // verify that the Channel.close is invoked.
        Mockito.verify(ch, times(1)).close();
        assertThrows(ObjectClosedException.class, () -> flowHandler.createFlow(flow, processor));
        assertThrows(ObjectClosedException.class, () -> flowHandler.createConnectionWithFlowDisabled(processor));
    }

    @Test
    public void testCreateConnectionWithSessionDisabled() throws Exception {
        flowHandler = new FlowHandler("testConnection1", tracker);
        flowHandler.channelRegistered(ctx);
        ClientConnection connection = flowHandler.createConnectionWithFlowDisabled(processor);
        connection.send(appendCmd);
        assertThrows(IllegalStateException.class, () -> flowHandler.createFlow(flow, processor));
    }

    @Test
    public void testChannelUnregistered() throws Exception {
        @Cleanup
        ClientConnection clientConnection = flowHandler.createFlow(flow, processor);
        flowHandler.channelRegistered(ctx);
        clientConnection.send(appendCmd);
        //simulate a connection dropped
        flowHandler.channelUnregistered(ctx);
        assertFalse(flowHandler.isConnectionEstablished());
        assertThrows(ConnectionFailedException.class, () -> clientConnection.send(appendCmd));
        WireCommands.GetSegmentAttribute cmd = new WireCommands.GetSegmentAttribute(flow.asLong(), "seg", UUID.randomUUID(), "");
        clientConnection.sendAsync(cmd, Assert::assertNotNull);
        clientConnection.sendAsync(Collections.singletonList(appendCmd), Assert::assertNotNull);
    }

    @Test
    public void testChannelReadWithHello() throws Exception {
        @Cleanup
        ClientConnection clientConnection = flowHandler.createFlow(flow, processor);
        WireCommands.Hello helloCmd = new WireCommands.Hello(8, 4);
        InOrder order = inOrder(processor);
        flowHandler.channelRegistered(ctx);
        flowHandler.channelRead(ctx, helloCmd);
        order.verify(processor, times(1)).hello(helloCmd);
    }

    @Test
    public void testChannelReadDataAppended() throws Exception {
        @Cleanup
        ClientConnection clientConnection = flowHandler.createFlow(flow, processor);
        WireCommands.DataAppended dataAppendedCmd = new WireCommands.DataAppended(flow.asLong(), UUID.randomUUID(), 2, 1);
        InOrder order = inOrder(processor);
        flowHandler.channelRegistered(ctx);
        flowHandler.channelRead(ctx, dataAppendedCmd);
        order.verify(processor, times(1)).process(dataAppendedCmd);
    }

    @Test
    public void testHelloWithErrorReplyProcessor() throws Exception {
        ReplyProcessor errorProcessor = mock(ReplyProcessor.class);
        @Cleanup
        ClientConnection connection1 = flowHandler.createFlow(new Flow(11, 0), errorProcessor);
        @Cleanup
        ClientConnection connection2 = flowHandler.createFlow(flow, processor);
        flowHandler.channelRegistered(ctx);
        doAnswer((Answer<Void>) invocation -> {
            throw new RuntimeException("Reply processor error");
        }).when(errorProcessor).hello(any(WireCommands.Hello.class));

        final WireCommands.Hello msg = new WireCommands.Hello(5, 4);
        flowHandler.channelRead(ctx, msg);
        verify(processor).hello(msg);
        verify(errorProcessor).hello(msg);
    }

    @Test
    public void testProcessWithErrorReplyProcessor() throws Exception {
        @Cleanup
        ClientConnection connection = flowHandler.createFlow(flow, processor);
        flowHandler.channelRegistered(ctx);
        doAnswer((Answer<Void>) invocation -> {
            throw new RuntimeException("ReplyProcessorError");
        }).when(processor).process(any(Reply.class));

        WireCommands.DataAppended msg = new WireCommands.DataAppended(flow.asLong(), UUID.randomUUID(), 2, 1);
        flowHandler.channelRead(ctx, msg);
        verify(processor).process(msg);
        verify(processor).processingFailure(any(RuntimeException.class));
    }

    @Test
    public void testExceptionCaughtWithErrorReplyProcessor() throws Exception {
        ReplyProcessor errorProcessor = mock(ReplyProcessor.class);
        @Cleanup
        ClientConnection connection1 = flowHandler.createFlow(new Flow(11, 0), errorProcessor);
        @Cleanup
        ClientConnection connection2 = flowHandler.createFlow(flow, processor);
        flowHandler.channelRegistered(ctx);
        doAnswer((Answer<Void>) invocation -> {
            throw new RuntimeException("Reply processor error");
        }).when(errorProcessor).processingFailure(any(ConnectionFailedException.class));

        flowHandler.exceptionCaught(ctx, new IOException("netty error"));
        verify(processor).processingFailure(any(ConnectionFailedException.class));
        verify(errorProcessor).processingFailure(any(ConnectionFailedException.class));
    }

    @Test
    public void testChannelUnregisteredWithErrorReplyProcessor() throws Exception {
        ReplyProcessor errorProcessor = mock(ReplyProcessor.class);
        @Cleanup
        ClientConnection connection1 = flowHandler.createFlow(new Flow(11, 0), errorProcessor);
        @Cleanup
        ClientConnection connection2 = flowHandler.createFlow(flow, processor);
        flowHandler.channelRegistered(ctx);
        doAnswer((Answer<Void>) invocation -> {
            throw new RuntimeException("Reply processor error");
        }).when(errorProcessor).connectionDropped();

        flowHandler.channelUnregistered(ctx);
        verify(processor).connectionDropped();
        verify(errorProcessor).connectionDropped();
    }
}

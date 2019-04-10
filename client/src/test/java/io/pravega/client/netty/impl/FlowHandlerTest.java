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
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommands;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
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

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static java.lang.String.valueOf;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FlowHandlerTest {

    @Rule
    public Timeout globalTimeout = Timeout.seconds(15);

    private Flow flow;
    private SessionHandler sessionHandler;
    private ClientConnection clientConnection;
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

        sessionHandler = new SessionHandler("testConnection", tracker);
        clientConnection = sessionHandler.createSession(flow, processor);
    }

    @Test
    public void sendNormal() throws Exception {
        // channelRegistered is invoked before send is invoked.
        // No exceptions are expected here.
        sessionHandler.channelRegistered(ctx);
        clientConnection.send(appendCmd);
    }

    @Test(expected = ConnectionFailedException.class)
    public void sendError() throws Exception {
        //Send function is invoked without channel registered being invoked.
        //this causes a connectionFailed exception.
        clientConnection.send(appendCmd);
    }

    @Test(expected = ConnectionFailedException.class)
    public void sendErrorUnRegistered() throws Exception {
        //any send after channelUnregistered should throw a ConnectionFailedException.
        sessionHandler.channelRegistered(ctx);
        sessionHandler.channelUnregistered(ctx);
        clientConnection.send(appendCmd);
    }

    @Test
    public void completeWhenRegisteredNormal() throws Exception {
        sessionHandler.channelRegistered(ctx);
        CompletableFuture<Void> testFuture = new CompletableFuture<>();
        sessionHandler.completeWhenRegistered(testFuture);
        Assert.assertTrue(Futures.isSuccessful(testFuture));
    }

    @Test
    public void completeWhenRegisteredDelayed() throws Exception {
        CompletableFuture<Void> testFuture = new CompletableFuture<>();
        sessionHandler.completeWhenRegistered(testFuture);
        sessionHandler.channelRegistered(ctx);
        Assert.assertTrue(Futures.isSuccessful(testFuture));
    }

    @Test
    public void completeWhenRegisteredDelayedMultiple() throws Exception {
        CompletableFuture<Void> testFuture = new CompletableFuture<>();
        sessionHandler.completeWhenRegistered(testFuture);

        CompletableFuture<Void> testFuture1 = new CompletableFuture<>();
        sessionHandler.completeWhenRegistered(testFuture1);

        sessionHandler.channelRegistered(ctx);

        Assert.assertTrue(Futures.isSuccessful(testFuture));
        testFuture1.get(); //wait until additional future is complete.
        Assert.assertTrue(Futures.isSuccessful(testFuture1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void createDuplicateSession() throws Exception {
        Flow flow = new Flow(11, 0);
        ClientConnection connection1 = sessionHandler.createSession(flow, processor);
        sessionHandler.channelRegistered(ctx);
        connection1.send(appendCmd);
        // Creating a flow with the same flow id.
        sessionHandler.createSession(flow, processor);
    }

    @Test
    public void testCloseSession() throws Exception {
        sessionHandler.channelRegistered(ctx);
        clientConnection.send(appendCmd);
        sessionHandler.closeSession(clientConnection);
        assertEquals(0, sessionHandler.getSessionIdReplyProcessorMap().size());
    }

    @Test
    public void testCloseSessionHandler() throws Exception {
        sessionHandler.channelRegistered(ctx);
        WireCommands.GetSegmentAttribute cmd = new WireCommands.GetSegmentAttribute(flow.asLong(), "seg", UUID.randomUUID(), "");
        clientConnection.sendAsync(cmd, e -> fail("Exception while invoking sendAsync"));
        sessionHandler.close();
        // verify that the Channel.close is invoked.
        Mockito.verify(ch, times(1)).close();
        assertThrows(ObjectClosedException.class, () -> sessionHandler.createSession(flow, processor));
        assertThrows(ObjectClosedException.class, () -> sessionHandler.createConnectionWithSessionDisabled(processor));
    }

    @Test
    public void testCreateConnectionWithSessionDisabled() throws Exception {
        sessionHandler = new SessionHandler("testConnection1", tracker);
        sessionHandler.channelRegistered(ctx);
        ClientConnection connection = sessionHandler.createConnectionWithSessionDisabled(processor);
        connection.send(appendCmd);
        assertThrows(IllegalStateException.class, () -> sessionHandler.createSession(flow, processor));
    }

    @Test
    public void testChannelUnregistered() throws Exception {
        sessionHandler.channelRegistered(ctx);
        clientConnection.send(appendCmd);
        //simulate a connection dropped
        sessionHandler.channelUnregistered(ctx);
        assertFalse(sessionHandler.isConnectionEstablished());
        assertThrows(ConnectionFailedException.class, () -> clientConnection.send(appendCmd));
        WireCommands.GetSegmentAttribute cmd = new WireCommands.GetSegmentAttribute(flow.asLong(), "seg", UUID.randomUUID(), "");
        clientConnection.sendAsync(cmd, Assert::assertNotNull);
        clientConnection.sendAsync(Collections.singletonList(appendCmd), Assert::assertNotNull);
    }

    @Test
    public void testChannelReadWithHello() throws Exception {
        WireCommands.Hello helloCmd = new WireCommands.Hello(8, 4);
        InOrder order = inOrder(processor);
        sessionHandler.channelRegistered(ctx);
        sessionHandler.channelRead(ctx, helloCmd);
        order.verify(processor, times(1)).hello(helloCmd);

    }

    @Test
    public void testChannelReadDataAppended() throws Exception {
        WireCommands.DataAppended dataAppendedCmd = new WireCommands.DataAppended(flow.asLong(), UUID.randomUUID(), 2, 1);
        InOrder order = inOrder(processor);
        sessionHandler.channelRegistered(ctx);
        sessionHandler.channelRead(ctx, dataAppendedCmd);
        order.verify(processor, times(1)).process(dataAppendedCmd);
    }
}

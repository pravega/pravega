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

import static java.lang.String.valueOf;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import io.pravega.common.concurrent.Futures;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.AppendBatchSizeTracker;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.ReplyProcessor;

@RunWith(MockitoJUnitRunner.class)
public class ClientConnectionInboundHandlerTest {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(15);

    private ClientConnectionInboundHandler handler;
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
        long requestId = 1L;
        when(buffer.readableBytes()).thenReturn(10);
        appendCmd = new Append("segment0", UUID.randomUUID(), 2, 1, buffer, 10L, requestId);
        doNothing().when(tracker).recordAppend(anyLong(), anyInt());

        when(ctx.channel()).thenReturn(ch);
        when(ch.eventLoop()).thenReturn(loop);
        when(ch.writeAndFlush(any(Object.class))).thenReturn(completedFuture);

        handler = new ClientConnectionInboundHandler("testConnection", processor, tracker);
    }

    @Test
    public void sendNormal() throws Exception {
        // channelRegistered is invoked before send is invoked.
        // No exceptions are expected here.
        handler.channelRegistered(ctx);
        handler.send(appendCmd);
    }

    @Test(expected = ConnectionFailedException.class)
    public void sendError() throws Exception {
        //Send function is invoked without channel registered being invoked.
        //this causes a connectionFailed exception.
        handler.send(appendCmd);
    }

    @Test(expected = ConnectionFailedException.class)
    public void sendErrorUnRegistered() throws Exception {
        //any send after channelUnregistered should throw a ConnectionFailedException.
        handler.channelRegistered(ctx);
        handler.channelUnregistered(ctx);
        handler.send(appendCmd);
    }

    @Test
    public void completeWhenRegisteredNormal() throws Exception {
        handler.channelRegistered(ctx);
        CompletableFuture<Void> testFuture = new CompletableFuture<>();
        handler.completeWhenRegistered(testFuture);
        Assert.assertTrue(Futures.isSuccessful(testFuture));
    }

    @Test
    public void completeWhenRegisteredDelayed() throws Exception {
        CompletableFuture<Void> testFuture = new CompletableFuture<>();
        handler.completeWhenRegistered(testFuture);
        handler.channelRegistered(ctx);
        Assert.assertTrue(Futures.isSuccessful(testFuture));
    }

    @Test
    public void completeWhenRegisteredDelayedMultiple() throws Exception {
        CompletableFuture<Void> testFuture = new CompletableFuture<>();
        handler.completeWhenRegistered(testFuture);

        CompletableFuture<Void> testFuture1 = new CompletableFuture<>();
        handler.completeWhenRegistered(testFuture1);

        handler.channelRegistered(ctx);

        Assert.assertTrue(Futures.isSuccessful(testFuture));
        testFuture1.get(); //wait until additional future is complete.
        Assert.assertTrue(Futures.isSuccessful(testFuture1));
    }
}

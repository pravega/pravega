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

import com.google.common.base.Preconditions;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.PromiseCombiner;
import io.netty.util.concurrent.ScheduledFuture;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.ReusableFutureLatch;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.AppendBatchSizeTracker;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommands;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

/**
 * Bridges the gap between netty and the ReplyProcessor on the client.
 */
@Slf4j
public class ClientConnectionInboundHandler extends ChannelInboundHandlerAdapter implements ClientConnection {

    private final String connectionName;
    private final ReplyProcessor processor;
    private final AtomicReference<Channel> channel = new AtomicReference<>();
    private final AtomicReference<ScheduledFuture<?>> keepAliveFuture = new AtomicReference<>();
    private final AtomicBoolean recentMessage = new AtomicBoolean(false);
    private final AppendBatchSizeTracker batchSizeTracker;
    private final ReusableFutureLatch<Void> registeredFutureLatch = new ReusableFutureLatch<>();

    ClientConnectionInboundHandler(String connectionName, ReplyProcessor processor, AppendBatchSizeTracker batchSizeTracker) {
        Preconditions.checkNotNull(processor);
        Preconditions.checkNotNull(batchSizeTracker);
        this.connectionName = connectionName;
        this.processor = processor;
        this.batchSizeTracker = batchSizeTracker;
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
        Channel c = ctx.channel();
        channel.set(c);
        registeredFutureLatch.release(null); //release all futures waiting for channel registration to complete.
        log.info("Connection established {} ", ctx);
        c.write(new WireCommands.Hello(WireCommands.WIRE_VERSION, WireCommands.OLDEST_COMPATIBLE_VERSION), c.voidPromise());
        ScheduledFuture<?> old = keepAliveFuture.getAndSet(c.eventLoop().scheduleWithFixedDelay(new KeepAliveTask(), 20, 10, TimeUnit.SECONDS));
        if (old != null) {
            old.cancel(false);
        }
    }

    /**
     * Disconnected.
     * @see io.netty.channel.ChannelInboundHandler#channelUnregistered(io.netty.channel.ChannelHandlerContext)
     */
    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        registeredFutureLatch.reset();
        ScheduledFuture<?> future = keepAliveFuture.get();
        if (future != null) {
            future.cancel(false);
        }
        channel.set(null);
        processor.connectionDropped();
        super.channelUnregistered(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        Reply cmd = (Reply) msg;
        log.debug(connectionName + " processing reply: {}", cmd);
        if (cmd instanceof WireCommands.DataAppended) {
            batchSizeTracker.recordAck(((WireCommands.DataAppended) cmd).getEventNumber());
        }
        try {
            processor.process(cmd);
        } catch (Exception e) {
            processor.processingFailure(e);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        processor.processingFailure(new ConnectionFailedException(cause));
    }

    @Override
    public void send(WireCommand cmd) throws ConnectionFailedException {
        recentMessage.set(true);
        Futures.getAndHandleExceptions(getChannel().writeAndFlush(cmd), ConnectionFailedException::new);
    }
    
    @Override
    public void send(Append append) throws ConnectionFailedException {
        recentMessage.set(true);
        batchSizeTracker.recordAppend(append.getEventNumber(), append.getData().readableBytes());
        Futures.getAndHandleExceptions(getChannel().writeAndFlush(append), ConnectionFailedException::new);
    }

    @Override
    public void sendAsync(WireCommand cmd) throws ConnectionFailedException {
        recentMessage.set(true);
        Channel channel = getChannel();
        try {
            channel.writeAndFlush(cmd, channel.voidPromise());
        } catch (RuntimeException e) {
            throw new ConnectionFailedException(e);
        }
    }
    
    @Override
    public void sendAsync(List<Append> appends, CompletedCallback callback) {
        recentMessage.set(true);
        Channel ch;
        try {
            ch = getChannel();
        } catch (ConnectionFailedException e) {
            callback.complete(new ConnectionFailedException("Connection to " + connectionName + " is not established."));
            return;
        }
        PromiseCombiner combiner = new PromiseCombiner();
        for (Append append : appends) {
            batchSizeTracker.recordAppend(append.getEventNumber(), append.getData().readableBytes());
            combiner.add(ch.write(append));
        }
        ch.flush();
        ChannelPromise promise = ch.newPromise();
        promise.addListener(new GenericFutureListener<Future<? super Void>>() {
            @Override
            public void operationComplete(Future<? super Void> future) throws Exception {
                Throwable cause = future.cause();
                callback.complete(cause == null ? null : new ConnectionFailedException(cause));
            }
        });
        combiner.finish(promise);
    }
    
    @Override
    public void close() {
        Channel ch = channel.get();
        if (ch != null) {
            log.debug("Closing channel:{}", ch);
            ch.close();
        }
    }

    private Channel getChannel() throws ConnectionFailedException {
        Channel ch = channel.get();
        if (ch == null) {
            throw new ConnectionFailedException("Connection to " + connectionName + " is not established.");
        }
        return ch;
    }

    /**
     * This function completes the input future when the channel is registered.
     *
     * @param future CompletableFuture which will be completed once the channel is registered.
     */
    void completeWhenRegistered(final CompletableFuture<Void> future) {
        Preconditions.checkNotNull(future, "future");
        registeredFutureLatch.register(future);
    }

    private final class KeepAliveTask implements Runnable {
        @Override
        public void run() {
            try {
                if (!recentMessage.getAndSet(false)) {
                    send(new WireCommands.KeepAlive());
                }
            } catch (Exception e) {
                log.warn("Keep alive failed, killing connection {} due to {} ", connectionName, e.getMessage());
                close();
            }
        }
    }

}

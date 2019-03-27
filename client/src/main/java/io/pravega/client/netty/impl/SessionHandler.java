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
import io.netty.util.concurrent.ScheduledFuture;
import io.pravega.client.Session;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.ReusableFutureLatch;
import io.pravega.shared.protocol.netty.AppendBatchSizeTracker;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommands;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SessionHandler extends ChannelInboundHandlerAdapter implements AutoCloseable {

    private final String connectionName;
    private final AtomicReference<Channel> channel = new AtomicReference<>();
    private final AtomicReference<ScheduledFuture<?>> keepAliveFuture = new AtomicReference<>();
    private final AtomicBoolean recentMessage = new AtomicBoolean(false);
    private final AppendBatchSizeTracker batchSizeTracker;
    private final ReusableFutureLatch<Void> registeredFutureLatch = new ReusableFutureLatch<>();
    private final ConcurrentHashMap<Integer, ReplyProcessor> sessionIdReplyProcessorMap = new ConcurrentHashMap<>();

    public SessionHandler(String connectionName, AppendBatchSizeTracker batchSizeTracker) {
        this.connectionName = connectionName;
        this.batchSizeTracker = batchSizeTracker;
    }

    public ClientConnection createSession(final Session session, final ReplyProcessor rp) {
        if (sessionIdReplyProcessorMap.put(session.getSessionId(), rp) != null) {
            throw new IllegalArgumentException("Multiple sessions cannot be created with the same Session id {}" + session.getSessionId());
        }
        ClientConnection connection = new ClientConnectionImpl(connectionName + session.getSessionId(), batchSizeTracker, this);
        return connection;
    }

    public void closeSession(ClientConnection session) {
        // reduce reference count.
    }

    void setRecentMessage() {
        recentMessage.set(true);
    }

    Channel getChannel() throws ConnectionFailedException {
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
     *
     * @see io.netty.channel.ChannelInboundHandler#channelUnregistered(ChannelHandlerContext)
     */
    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        registeredFutureLatch.reset();
        ScheduledFuture<?> future = keepAliveFuture.get();
        if (future != null) {
            future.cancel(false);
        }
        channel.set(null);
        sessionIdReplyProcessorMap.forEach((sessionId, rp) -> {
            rp.connectionDropped();
            log.debug("Connection dropped for session id : {}", sessionId);
        });
        super.channelUnregistered(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        Reply cmd = (Reply) msg;
        log.debug(connectionName + " processing reply: {}", cmd);
        if (cmd instanceof WireCommands.DataAppended) {
            batchSizeTracker.recordAck(((WireCommands.DataAppended) cmd).getEventNumber());
        }

        final ReplyProcessor processor = getReplyProcessor(cmd);
        try {
            processor.process(cmd);
        } catch (Exception e) {
            processor.processingFailure(e);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        sessionIdReplyProcessorMap.forEach((sessionId, rp) -> {
            rp.processingFailure(new ConnectionFailedException(cause));
            log.debug("Exception observed for session id : {}", sessionId);
        });
    }

    @Override
    public void close() {
        Channel ch = channel.get();
        if (ch != null) {
            log.debug("Closing channel:{}", ch);
            ch.close();
        }
    }

    private final class KeepAliveTask implements Runnable {
        @Override
        public void run() {
            try {
                if (!recentMessage.getAndSet(false)) {
                    recentMessage.set(true);
                    Futures.getAndHandleExceptions(getChannel().writeAndFlush(new WireCommands.KeepAlive()), ConnectionFailedException::new);
                }
            } catch (Exception e) {
                log.warn("Keep alive failed, killing connection {} due to {} ", connectionName, e.getMessage());
                //close();
            }
        }
    }

    private ReplyProcessor getReplyProcessor(Reply cmd) {
        final Session session = Session.from(cmd.getRequestId());
        final ReplyProcessor processor = sessionIdReplyProcessorMap.get(session.getSessionId());
        if (processor == null) {
            log.error("No ReplyProcessor found for the provided sessionId {}", session.getSessionId());
            throw new IllegalArgumentException("Invalid message received");
        }
        return processor;
    }
}

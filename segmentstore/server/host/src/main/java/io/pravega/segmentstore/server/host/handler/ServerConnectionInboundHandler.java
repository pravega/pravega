/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.handler;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoop;
import io.pravega.common.Exceptions;
import io.pravega.segmentstore.server.IllegalContainerStateException;
import io.pravega.shared.protocol.netty.Request;
import io.pravega.shared.protocol.netty.RequestProcessor;
import io.pravega.shared.protocol.netty.WireCommand;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

/**
 * Acts as a bridge between Netty and the RequestProcessor on the server.
 */
@Slf4j
public class ServerConnectionInboundHandler extends ChannelInboundHandlerAdapter implements ServerConnection {

    private AtomicReference<RequestProcessor> processor = new AtomicReference<>();
    private AtomicReference<Channel> channel = new AtomicReference<>();

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
        channel.set(ctx.channel());
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        Request cmd = (Request) msg;
        if (cmd.mustLog()) {
            log.debug("Received request: {}", cmd);
        } else {
            log.trace("Received request: {}", cmd);
        }

        RequestProcessor requestProcessor = processor.get();
        if (requestProcessor == null) {
            throw new IllegalStateException("No command processor set for connection");
        }
        cmd.process(requestProcessor);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        logError(cause);
        ctx.close();
    }

    @Override
    public void send(WireCommand cmd) {
        Channel c = getChannel();
        // Work around for https://github.com/netty/netty/issues/3246
        EventLoop eventLoop = c.eventLoop();
        if (eventLoop.inEventLoop()) {
            eventLoop.execute(() -> writeAndFlush(c, cmd));
        } else {
            writeAndFlush(c, cmd);
        }
    }
    
    private static void writeAndFlush(Channel channel, WireCommand data) {
        channel.writeAndFlush(data).addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
    }  
    
    @Override
    public void setRequestProcessor(RequestProcessor rp) {
        processor.set(rp);
    }

    @Override
    public void close() {
        Channel ch = channel.get();
        if (ch != null) {
            ch.close();
        }
    }

    @Override
    public void pauseReading() {
        getChannel().config().setAutoRead(false);
    }

    @Override
    public void resumeReading() {
        getChannel().config().setAutoRead(true);
    }

    private Channel getChannel() {
        Channel ch = channel.get();
        if (ch == null) {
            throw new IllegalStateException("Connection not yet established.");
        }
        return ch;
    }

    private void logError(Throwable cause) {
        if (Exceptions.unwrap(cause) instanceof IllegalContainerStateException) {
            log.warn("Caught exception on connection: {}", cause.toString());
        } else {
            log.error("Caught exception on connection: ", cause);
        }
    }

    @Override
    public String toString() {
        Channel c = channel.get();
        if (c == null) {
            return "NewServerConnection";
        }
        return c.toString();
    }
    
}

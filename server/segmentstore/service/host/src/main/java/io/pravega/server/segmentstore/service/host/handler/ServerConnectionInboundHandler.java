/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.server.segmentstore.service.host.handler;

import java.util.concurrent.atomic.AtomicReference;

import io.pravega.shared.protocol.netty.Request;
import io.pravega.shared.protocol.netty.RequestProcessor;
import io.pravega.shared.protocol.netty.WireCommand;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
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
        log.debug("Processing request: {}", cmd);
        RequestProcessor requestProcessor = processor.get();
        if (requestProcessor == null) {
            throw new IllegalStateException("No command processor set for connection");
        }
        cmd.process(requestProcessor);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        log.error("Caught exception on connection: ", cause);
        ctx.close();
    }

    @Override
    public void send(WireCommand cmd) {
        getChannel().writeAndFlush(cmd).addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
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
}
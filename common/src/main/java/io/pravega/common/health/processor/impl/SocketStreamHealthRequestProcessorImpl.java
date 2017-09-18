/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.health.processor.impl;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import java.io.DataOutputStream;
import lombok.extern.slf4j.Slf4j;

/**
 * A HealthRequestProcessor driven from a socket stream.
 */
@Slf4j
public class SocketStreamHealthRequestProcessorImpl extends HealthRequestProcessorImpl {
    private final int port;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private boolean nio = false;
    private Channel serverChannel;
    private String host;

    public SocketStreamHealthRequestProcessorImpl(String host, int port) {
        super();
        this.host = host;
        this.port = port;
    }

    public void startListening() {
        try {
            bossGroup = new EpollEventLoopGroup(1);
            workerGroup = new EpollEventLoopGroup();
        } catch (ExceptionInInitializerError | NoClassDefFoundError e) {
            nio = true;
            bossGroup = new NioEventLoopGroup(1);
            workerGroup = new NioEventLoopGroup();
        }

        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
         .channel(nio ? NioServerSocketChannel.class : EpollServerSocketChannel.class)
         .option(ChannelOption.SO_BACKLOG, 100)
         .handler(new LoggingHandler(LogLevel.INFO))
         .childHandler(new ChannelInitializer<SocketChannel>() {
             @Override
             public void initChannel(SocketChannel ch) throws Exception {
                 ChannelPipeline p = ch.pipeline();
                 p.addLast(new StringEncoder());
                 p.addLast(new StringDecoder());
                 HealthConnectionInboundHandler healthHandler = new HealthConnectionInboundHandler();
                 p.addLast( healthHandler);
             }
         });

        // Start the server.
        serverChannel = b.bind(host, port).awaitUninterruptibly().channel();
    }

    private class HealthConnectionInboundHandler extends SimpleChannelInboundHandler<String> {
        @Override
        public void channelActive(ChannelHandlerContext ctx) {
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
            try (DataOutputStream nioStream = new DataOutputStream(new NettyOutputStream(ctx.channel()))) {
                String input = msg;
                String cmd;
                log.debug("Processing health request: {}", input);
                input = input.trim();
                String target = null;
                if (input.indexOf(' ') == -1) {
                    cmd = input;
                } else {
                    String[] parts = input.split(" ");
                    cmd = parts[0];
                    target = parts[1];
                }
                processHealthRequest(nioStream, target, cmd);
                nioStream.flush();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                ctx.writeAndFlush("Error: " +
                        cause.getClass().getSimpleName() + ": " +
                        cause.getMessage() + '\n').addListener(ChannelFutureListener.CLOSE);
        }
    }
}

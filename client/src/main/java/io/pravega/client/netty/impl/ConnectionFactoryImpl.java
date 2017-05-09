/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.netty.impl;


import java.security.NoSuchAlgorithmException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.SSLException;
import io.pravega.common.Exceptions;
import io.pravega.shared.protocol.netty.AppendBatchSizeTracker;
import io.pravega.shared.protocol.netty.CommandDecoder;
import io.pravega.shared.protocol.netty.CommandEncoder;
import io.pravega.shared.protocol.netty.ExceptionLoggingHandler;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import com.google.common.base.Preconditions;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.FingerprintTrustManagerFactory;
import io.pravega.shared.protocol.netty.WireCommands;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class ConnectionFactoryImpl implements ConnectionFactory {

    private final boolean ssl;
    private EventLoopGroup group;
    private boolean nio = false;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Actual implementation of ConnectionFactory interface.
     *
     * @param ssl Whether connection should use SSL or not.
     */
    public ConnectionFactoryImpl(boolean ssl) {
        this.ssl = ssl;
        try {
            this.group = new EpollEventLoopGroup();
        } catch (ExceptionInInitializerError | NoClassDefFoundError e) {
            log.warn("Epoll not available. Falling back on NIO.");
            nio = true;
            this.group = new NioEventLoopGroup();
        }
    }

    @Override
    public CompletableFuture<ClientConnection> establishConnection(PravegaNodeUri location, ReplyProcessor rp) {
        Preconditions.checkNotNull(location);
        Exceptions.checkNotClosed(closed.get(), this);
        final SslContext sslCtx;
        if (ssl) {
            try {
                sslCtx = SslContextBuilder.forClient()
                                          .trustManager(FingerprintTrustManagerFactory
                                                  .getInstance(FingerprintTrustManagerFactory.getDefaultAlgorithm()))
                                          .build();
            } catch (SSLException | NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        } else {
            sslCtx = null;
        }
        AppendBatchSizeTracker batchSizeTracker = new AppendBatchSizeTrackerImpl();
        ClientConnectionInboundHandler handler = new ClientConnectionInboundHandler(location.getEndpoint(), rp, batchSizeTracker);
        Bootstrap b = new Bootstrap();
        b.group(group)
         .channel(nio ? NioSocketChannel.class : EpollSocketChannel.class)
         .option(ChannelOption.TCP_NODELAY, true)
         .handler(new ChannelInitializer<SocketChannel>() {
             @Override
             public void initChannel(SocketChannel ch) throws Exception {
                 ChannelPipeline p = ch.pipeline();
                 if (sslCtx != null) {
                     p.addLast(sslCtx.newHandler(ch.alloc(), location.getEndpoint(), location.getPort()));
                 }
                 // p.addLast(new LoggingHandler(LogLevel.INFO));
                 p.addLast(new ExceptionLoggingHandler(location.getEndpoint()),
                         new CommandEncoder(batchSizeTracker),
                         new LengthFieldBasedFrameDecoder(WireCommands.MAX_WIRECOMMAND_SIZE, 4, 4),
                         new CommandDecoder(),
                         handler);
             }
         });

        // Start the client.
        CompletableFuture<ClientConnection> result = new CompletableFuture<>();
        try {
            b.connect(location.getEndpoint(), location.getPort()).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) {
                    if (future.isSuccess()) {
                        result.complete(handler);
                    } else {
                        result.completeExceptionally(future.cause());
                    }
                }
            });
        } catch (Exception e) {
            result.completeExceptionally(e);
        }
        return result;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            // Shut down the event loop to terminate all threads.
            group.shutdownGracefully();
        }
    }

    @Override
    protected void finalize() {
        close();
    }
}
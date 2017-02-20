/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl.netty;


import static com.emc.pravega.common.netty.WireCommands.MAX_WIRECOMMAND_SIZE;

import java.security.NoSuchAlgorithmException;
import java.util.concurrent.CompletableFuture;

import javax.net.ssl.SSLException;

import com.emc.pravega.common.netty.AppendBatchSizeTracker;
import com.emc.pravega.common.netty.CommandDecoder;
import com.emc.pravega.common.netty.CommandEncoder;
import com.emc.pravega.common.netty.ExceptionLoggingHandler;
import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.common.netty.ReplyProcessor;
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
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class ConnectionFactoryImpl implements ConnectionFactory {

    private final boolean ssl;
    private EventLoopGroup group;
    private boolean nio = false;

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
                         new LengthFieldBasedFrameDecoder(MAX_WIRECOMMAND_SIZE, 4, 4),
                         new CommandDecoder(),
                         handler);
             }
         });

        // Start the client.
        CompletableFuture<ClientConnection> result = new CompletableFuture<>();
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
        return result;
    }

    @Override
    public void close() {
        // Shut down the event loop to terminate all threads.
        group.shutdownGracefully();
    }

    @Override
    protected void finalize() {
        group.shutdownGracefully();
    }
}
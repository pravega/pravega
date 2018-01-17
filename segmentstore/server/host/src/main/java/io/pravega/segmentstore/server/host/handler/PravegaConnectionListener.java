/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.handler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import io.pravega.common.Exceptions;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.host.stat.SegmentStatsRecorder;
import io.pravega.shared.protocol.netty.AppendDecoder;
import io.pravega.shared.protocol.netty.CommandDecoder;
import io.pravega.shared.protocol.netty.CommandEncoder;
import io.pravega.shared.protocol.netty.ExceptionLoggingHandler;
import java.security.cert.CertificateException;
import javax.net.ssl.SSLException;

import static io.pravega.shared.protocol.netty.WireCommands.MAX_WIRECOMMAND_SIZE;

/**
 * Hands off any received data from a client to the CommandProcessor.
 */
public final class PravegaConnectionListener implements AutoCloseable {
    //region Members

    private final boolean ssl;
    private final String host;
    private final int port;
    private final StreamSegmentStore store;
    private Channel serverChannel;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private final SegmentStatsRecorder statsRecorder;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the PravegaConnectionListener class listening on localhost with no StatsRecorder.
     *
     * @param ssl                Whether to use SSL.
     * @param port               The port to listen on.
     * @param streamSegmentStore The SegmentStore to delegate all requests to.
     */
    @VisibleForTesting
    public PravegaConnectionListener(boolean ssl, int port, StreamSegmentStore streamSegmentStore) {
        this(ssl, "0.0.0.0", port, streamSegmentStore, null);
    }

    /**
     * Creates a new instance of the PravegaConnectionListener class.
     *
     * @param ssl                Whether to use SSL.
     * @param host               The name of the host to listen to.
     * @param port               The port to listen on.
     * @param streamSegmentStore The SegmentStore to delegate all requests to.
     * @param statsRecorder      (Optional) A StatsRecorder for Metrics.
     */
    public PravegaConnectionListener(boolean ssl, String host, int port, StreamSegmentStore streamSegmentStore,
                                     SegmentStatsRecorder statsRecorder) {
        this.ssl = ssl;
        this.host = Exceptions.checkNotNullOrEmpty(host, "host");
        this.port = port;
        this.store = Preconditions.checkNotNull(streamSegmentStore, "streamSegmentStore");
        this.statsRecorder = statsRecorder;
        InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE);
    }

    //endregion

    public void startListening() {
        // Configure SSL.
        final SslContext sslCtx;
        if (ssl) {
            try {
                SelfSignedCertificate ssc = new SelfSignedCertificate();
                sslCtx = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();
            } catch (CertificateException | SSLException e) {
                throw new RuntimeException(e);
            }
        } else {
            sslCtx = null;
        }
        boolean nio = false;
        try {
            bossGroup = new EpollEventLoopGroup(1);
            workerGroup = new EpollEventLoopGroup();
        } catch (ExceptionInInitializerError | UnsatisfiedLinkError | NoClassDefFoundError e) {
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
                 if (sslCtx != null) {
                     p.addLast(sslCtx.newHandler(ch.alloc()));
                 }
                 ServerConnectionInboundHandler lsh = new ServerConnectionInboundHandler();
                 // p.addLast(new LoggingHandler(LogLevel.INFO));
                 p.addLast(new ExceptionLoggingHandler(ch.remoteAddress().toString()),
                         new CommandEncoder(null),
                         new LengthFieldBasedFrameDecoder(MAX_WIRECOMMAND_SIZE, 4, 4),
                         new CommandDecoder(),
                         new AppendDecoder(),
                         lsh);
                 lsh.setRequestProcessor(new AppendProcessor(store,
                         lsh,
                         new PravegaRequestProcessor(store, lsh, statsRecorder),
                         statsRecorder));
             }
         });

        // Start the server.
        serverChannel = b.bind(host, port).awaitUninterruptibly().channel();
    }

    @Override
    public void close() {
        // Wait until the server socket is closed.
        Exceptions.handleInterrupted(() -> {
            serverChannel.close();
            serverChannel.closeFuture().sync();
        });
        // Shut down all event loops to terminate all threads.
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }
}
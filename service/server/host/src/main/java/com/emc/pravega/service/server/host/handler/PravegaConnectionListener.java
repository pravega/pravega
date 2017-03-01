/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server.host.handler;

import static com.emc.pravega.common.netty.WireCommands.MAX_WIRECOMMAND_SIZE;

import java.security.cert.CertificateException;

import javax.net.ssl.SSLException;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.netty.AppendDecoder;
import com.emc.pravega.common.netty.CommandDecoder;
import com.emc.pravega.common.netty.CommandEncoder;
import com.emc.pravega.common.netty.ExceptionLoggingHandler;
import com.emc.pravega.service.contracts.StreamSegmentStore;

import com.emc.pravega.service.server.host.stat.SegmentStatsRecorder;
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

/**
 * Hands off any received data from a client to the CommandProcessor.
 */
public final class PravegaConnectionListener implements AutoCloseable {

    private final boolean ssl;
    private final int port;
    private final StreamSegmentStore store;
    private Channel serverChannel;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private final SegmentStatsRecorder statsRecorder;

    public PravegaConnectionListener(boolean ssl, int port, StreamSegmentStore streamSegmentStore) {
        this(ssl, port, streamSegmentStore, null);
    }

    public PravegaConnectionListener(boolean ssl, int port, StreamSegmentStore streamSegmentStore, SegmentStatsRecorder statsRecorder) {
        this.ssl = ssl;
        this.port = port;
        this.store = streamSegmentStore;
        this.statsRecorder = statsRecorder;
        InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE);
    }

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
        serverChannel = b.bind(port).awaitUninterruptibly().channel();
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
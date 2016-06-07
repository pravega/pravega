package com.emc.logservice.serverhost.handler;

import com.emc.logservice.contracts.StreamSegmentStore;
import com.emc.nautilus.common.netty.*;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;

import javax.net.ssl.SSLException;
import java.security.cert.CertificateException;

/**
 * Hands off any received data from a client to the CommandProcessor.
 */
public final class LogServiceConnectionListener implements ConnectionListener {

    private final boolean ssl;
    private final int port;
    private final StreamSegmentStore store;
    private Channel serverChannel;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    public LogServiceConnectionListener(boolean ssl, int port, StreamSegmentStore streamSegmentStore) {
        this.ssl = ssl;
        this.port = port;
        this.store = streamSegmentStore;
    }

    public void startListening() {
        // Configure SSL.
        final SslContext sslCtx;
        if (ssl) {
            try {
                SelfSignedCertificate ssc = new SelfSignedCertificate();
                sslCtx = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();
            }
            catch (CertificateException | SSLException e) {
                throw new RuntimeException(e);
            }
        }
        else {
            sslCtx = null;
        }

        bossGroup = new EpollEventLoopGroup(1);
        workerGroup = new EpollEventLoopGroup();

        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
         .channel(EpollServerSocketChannel.class)
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
                 //p.addLast(new LoggingHandler(LogLevel.INFO));
                 p.addLast(new ExceptionLoggingHandler(),
                         new CommandEncoder(),
                         new LengthFieldBasedFrameDecoder(1024 * 1024, 4, 4),
                         new CommandDecoder(),
                         lsh);
                 lsh.setRequestProcessor(new AppendProcessor(store,
                         lsh,
                         new LogServiceRequestProcessor(store, lsh)));
             }
         });

        // Start the server.
        serverChannel = b.bind(port).awaitUninterruptibly().channel();
    }

    public void shutdown() {
        // Wait until the server socket is closed.
        try {
            serverChannel.close();
            serverChannel.closeFuture().sync();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        // Shut down all event loops to terminate all threads.
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

    @Override
    public void close(){
        shutdown();
    }
}
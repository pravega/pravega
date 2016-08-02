/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.server.host.handler;

import static com.emc.nautilus.common.netty.WireCommands.MAX_WIRECOMMAND_SIZE;

import java.security.cert.CertificateException;

import javax.net.ssl.SSLException;

import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.nautilus.common.netty.CommandDecoder;
import com.emc.nautilus.common.netty.CommandEncoder;
import com.emc.nautilus.common.netty.ConnectionListener;
import com.emc.nautilus.common.netty.ExceptionLoggingHandler;

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

    @Override
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
        } catch (ExceptionInInitializerError e) {
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
                         new CommandEncoder(),
                         new LengthFieldBasedFrameDecoder(MAX_WIRECOMMAND_SIZE, 4, 4),
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

    @Override
    public void close() {
        // Wait until the server socket is closed.
        try {
            serverChannel.close();
            serverChannel.closeFuture().sync();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        // Shut down all event loops to terminate all threads.
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }
}
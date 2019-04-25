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
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.pravega.client.ClientConfig;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.FailingReplyProcessor;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.test.common.TestUtils;
import java.io.File;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import lombok.Cleanup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ConnectionFactoryImplTest {

    boolean ssl = false;
    private Channel serverChannel;
    private int port;

    @Before
    public void setUp() throws Exception {
        // Configure SSL.
        port = TestUtils.getAvailableListenPort();
        final SslContext sslCtx;
        if (ssl) {
            try {
                sslCtx = SslContextBuilder.forServer(new File("../config/server-cert.crt"), new File("../config/server-key.key")).build();
            } catch (SSLException e) {
                throw new RuntimeException(e);
            }
        } else {
            sslCtx = null;
        }
        boolean nio = false;
        EventLoopGroup bossGroup;
        EventLoopGroup workerGroup;
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
                     SslHandler handler = sslCtx.newHandler(ch.alloc());
                     SSLEngine sslEngine = handler.engine();
                     SSLParameters sslParameters = sslEngine.getSSLParameters();
                     sslParameters.setEndpointIdentificationAlgorithm("LDAPS");
                     sslEngine.setSSLParameters(sslParameters);
                     p.addLast(handler);
                 }
             }
         });

        // Start the server.
        serverChannel = b.bind("localhost", port).awaitUninterruptibly().channel();
    }

    @After
    public void tearDown() throws Exception {
        serverChannel.close();
        serverChannel.closeFuture();
    }

    @Test
    public void establishConnection() throws ConnectionFailedException {
        @Cleanup
        ConnectionFactoryImpl factory = new ConnectionFactoryImpl(ClientConfig.builder()
                                                                              .controllerURI(URI.create((this.ssl ? "tls://" : "tcp://") + "localhost"))
                                                                              .trustStore("../config/ca-cert.crt")
                                                                              .build());
        @Cleanup
        ClientConnection connection = factory.establishConnection(new PravegaNodeUri("localhost", port), new FailingReplyProcessor() {
            @Override
            public void connectionDropped() {

            }

            @Override
            public void processingFailure(Exception error) {

            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {

            }
        }).join();

        connection.send(new WireCommands.Hello(0, 0));
    }

    @Test
    public void getActiveChannelTest() throws InterruptedException, ConnectionFailedException {
        @Cleanup
        ConnectionFactoryImpl factory = new ConnectionFactoryImpl(ClientConfig.builder()
                                                                              .controllerURI(URI.create( "tcp://" + "localhost"))
                                                                              .build());
        // establish a connection.
        @Cleanup
        ClientConnectionImpl connection = (ClientConnectionImpl) factory.establishConnection(new PravegaNodeUri("localhost", port), new FailingReplyProcessor() {

            @Override
            public void connectionDropped() {

            }

            @Override
            public void processingFailure(Exception error) {

            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {

            }
        }).join();

        assertEquals("Expected active channel count is 1", 1, factory.getActiveChannelCount());

        // add a listener to track the channel close.
        final CountDownLatch latch = new CountDownLatch(1);
        connection.getNettyHandler().getChannel().closeFuture().addListener(future -> latch.countDown());

        // close the connection, this does not close the underlying network connection due to connection pooling.
        connection.close();
        factory.getConnectionPool().close();

        // wait until the channel is closed.
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertEquals("Expected active channel count is 0", 0, factory.getActiveChannelCount());
        // verify that the channel is removed from channelGroup too.
        assertEquals(0,  ((ConnectionPoolImpl) factory.getConnectionPool()).getChannelGroup().size());
    }
}

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
import io.pravega.client.PravegaClientConfig;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.ReplyProcessor;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.test.common.TestUtils;
import java.io.File;
import java.net.URI;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
                sslCtx = SslContextBuilder.forServer(new File("../config/cert.pem"), new File("../config/key.pem")).build();
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
        ConnectionFactoryImpl factory = new ConnectionFactoryImpl(PravegaClientConfig.builder()
                                    .controllerURI(URI.create((this.ssl ? "tls://" : "tcp://") + "localhost"))
                                    .pravegaTrustStore("../config/cert.pem")
                                   .build());
        ClientConnection connection = factory.establishConnection(new PravegaNodeUri("localhost", port), new ReplyProcessor() {
            @Override
            public void hello(WireCommands.Hello hello) {

            }

            @Override
            public void wrongHost(WireCommands.WrongHost wrongHost) {

            }

            @Override
            public void segmentAlreadyExists(WireCommands.SegmentAlreadyExists segmentAlreadyExists) {

            }

            @Override
            public void segmentIsSealed(WireCommands.SegmentIsSealed segmentIsSealed) {

            }

            @Override
            public void segmentIsTruncated(WireCommands.SegmentIsTruncated segmentIsTruncated) {

            }

            @Override
            public void noSuchSegment(WireCommands.NoSuchSegment noSuchSegment) {

            }

            @Override
            public void noSuchTransaction(WireCommands.NoSuchTransaction noSuchTransaction) {

            }

            @Override
            public void invalidEventNumber(WireCommands.InvalidEventNumber invalidEventNumber) {

            }

            @Override
            public void appendSetup(WireCommands.AppendSetup appendSetup) {

            }

            @Override
            public void dataAppended(WireCommands.DataAppended dataAppended) {

            }

            @Override
            public void conditionalCheckFailed(WireCommands.ConditionalCheckFailed dataNotAppended) {

            }

            @Override
            public void segmentRead(WireCommands.SegmentRead segmentRead) {

            }

            @Override
            public void segmentAttributeUpdated(WireCommands.SegmentAttributeUpdated segmentAttributeUpdated) {

            }

            @Override
            public void segmentAttribute(WireCommands.SegmentAttribute segmentAttribute) {

            }

            @Override
            public void streamSegmentInfo(WireCommands.StreamSegmentInfo streamInfo) {

            }

            @Override
            public void transactionInfo(WireCommands.TransactionInfo transactionInfo) {

            }

            @Override
            public void segmentCreated(WireCommands.SegmentCreated segmentCreated) {

            }

            @Override
            public void transactionCreated(WireCommands.TransactionCreated transactionCreated) {

            }

            @Override
            public void transactionCommitted(WireCommands.TransactionCommitted transactionCommitted) {

            }

            @Override
            public void transactionAborted(WireCommands.TransactionAborted transactionAborted) {

            }

            @Override
            public void segmentSealed(WireCommands.SegmentSealed segmentSealed) {

            }

            @Override
            public void segmentTruncated(WireCommands.SegmentTruncated segmentTruncated) {

            }

            @Override
            public void segmentDeleted(WireCommands.SegmentDeleted segmentDeleted) {

            }

            @Override
            public void operationUnsupported(WireCommands.OperationUnsupported operationUnsupported) {

            }

            @Override
            public void keepAlive(WireCommands.KeepAlive keepAlive) {

            }

            @Override
            public void connectionDropped() {

            }

            @Override
            public void segmentPolicyUpdated(WireCommands.SegmentPolicyUpdated segmentPolicyUpdated) {

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
}
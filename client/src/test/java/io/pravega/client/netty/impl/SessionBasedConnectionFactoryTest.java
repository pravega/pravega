/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.netty.impl;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
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
import io.netty.handler.ssl.SslHandler;
import io.pravega.client.ClientConfig;
import io.pravega.client.Session;
import io.pravega.common.ObjectClosedException;
import io.pravega.shared.protocol.netty.CommandDecoder;
import io.pravega.shared.protocol.netty.CommandEncoder;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.FailingReplyProcessor;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.test.common.TestUtils;
import java.io.File;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.function.Function;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import lombok.Cleanup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.shared.protocol.netty.WireCommands.MAX_WIRECOMMAND_SIZE;
import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SessionBasedConnectionFactoryTest {

    boolean ssl = false;
    private Channel serverChannel;
    private int port;
    private final String seg = "Segment-0";
    private final long offset = 1234L;
    private final int length = 1024;
    private final String data = "data";
    private Function<Long, WireCommands.ReadSegment> readRequestGenerator = session ->
            new WireCommands.ReadSegment(seg, offset, length, "", session);
    private Function<Long, WireCommands.SegmentRead> readResponseGenerator = session ->
            new WireCommands.SegmentRead(seg, offset, true, false, ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8)), session);

    private class EchoServerHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object message) {
            if (message instanceof WireCommands.Hello) {
                ctx.write(message);
                ctx.flush();
            } else if (message instanceof WireCommands.ReadSegment) {
                WireCommands.ReadSegment msg = (WireCommands.ReadSegment) message;
                ctx.write(readResponseGenerator.apply(msg.getRequestId()));
                ctx.flush();
            }
        }
    }

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
                 p.addLast(new CommandEncoder(null),
                           new LengthFieldBasedFrameDecoder(MAX_WIRECOMMAND_SIZE, 4, 4),
                           new CommandDecoder(),
                           new EchoServerHandler());
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
    public void testConnectionPooling() throws ConnectionFailedException, InterruptedException {
        @Cleanup
        SessionBasedConnectionFactory factory = new SessionBasedConnectionFactory(ClientConfig.builder()
                                                                                              .controllerURI(URI.create((this.ssl ? "tls://" : "tcp://") + "localhost"))
                                                                                              .trustStore("../config/cert.pem")
                                                                                              .maxConnectionsPerSegmentStore(1)
                                                                                              .build());

        ArrayBlockingQueue<WireCommands.SegmentRead> msgRead = new ArrayBlockingQueue<>(10);
        FailingReplyProcessor rp = new FailingReplyProcessor() {
            @Override
            public void connectionDropped() {

            }

            @Override
            public void segmentRead(WireCommands.SegmentRead data) {
                msgRead.add(data);
            }

            @Override
            public void processingFailure(Exception error) {

            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {

            }
        };

        Session session1 = new Session(1, 0);
        @Cleanup
        ClientConnection connection1 = factory.establishConnection(session1, new PravegaNodeUri("localhost", port), rp).join();

        connection1.send(readRequestGenerator.apply(session1.asLong()));

        WireCommands.SegmentRead msg = msgRead.take();
        assertEquals(readResponseGenerator.apply(session1.asLong()), msg);

        // create a second connection, since the max number of connections is 1 this should reuse the same connection.
        Session session2 = new Session(2, 0);
        @Cleanup
        ClientConnection connection2 = factory.establishConnection(session2, new PravegaNodeUri("localhost", port), rp).join();

        // send data over connection2 and verify.
        connection2.send(readRequestGenerator.apply(session2.asLong()));
        msg = msgRead.take();
        assertEquals(readResponseGenerator.apply(session2.asLong()), msg);

        // send data over connection1 and verify.
        connection1.send(readRequestGenerator.apply(session1.asLong()));
        msg = msgRead.take();
        assertEquals(readResponseGenerator.apply(session1.asLong()), msg);

        // send data over connection2 and verify.
        connection2.send(readRequestGenerator.apply(session2.asLong()));
        msg = msgRead.take();
        assertEquals(readResponseGenerator.apply(session2.asLong()), msg);

        // close a client connection, this should not close the channel.
        connection2.close();
        assertThrows(ObjectClosedException.class, () -> connection2.send(readRequestGenerator.apply(session2.asLong())));
        // verify we are able to send data over connection1.
        connection1.send(readRequestGenerator.apply(session1.asLong()));
        msg = msgRead.take();
        assertEquals(readResponseGenerator.apply(session1.asLong()), msg);

        // close connection1
        connection1.close();
        assertThrows(ObjectClosedException.class, () -> connection1.send(readRequestGenerator.apply(session2.asLong())));

    }

    @Test
    public void testConcurrentRequests() throws ConnectionFailedException, InterruptedException {
        @Cleanup
        SessionBasedConnectionFactory factory = new SessionBasedConnectionFactory(ClientConfig.builder()
                                                                                              .controllerURI(URI.create((this.ssl ? "tls://" : "tcp://") + "localhost"))
                                                                                              .trustStore("../config/cert.pem")
                                                                                              .maxConnectionsPerSegmentStore(1)
                                                                                              .build());

        ArrayBlockingQueue<WireCommands.SegmentRead> msgRead = new ArrayBlockingQueue<>(10);
        FailingReplyProcessor rp = new FailingReplyProcessor() {
            @Override
            public void connectionDropped() {

            }

            @Override
            public void segmentRead(WireCommands.SegmentRead data) {
                msgRead.add(data);
            }

            @Override
            public void processingFailure(Exception error) {

            }

            @Override
            public void authTokenCheckFailed(WireCommands.AuthTokenCheckFailed authTokenCheckFailed) {

            }
        };

        Session session1 = new Session(1, 0);
        @Cleanup
        ClientConnection connection1 = factory.establishConnection(session1, new PravegaNodeUri("localhost", port), rp).join();

        // create a second connection, since the max number of connections is 1 this should reuse the same connection.
        Session session2 = new Session(2, 0);
        @Cleanup
        ClientConnection connection2 = factory.establishConnection(session2, new PravegaNodeUri("localhost", port), rp).join();

        connection1.send(readRequestGenerator.apply(session1.asLong()));
        connection2.send(readRequestGenerator.apply(session2.asLong()));

        List<WireCommands.SegmentRead> msgs = new ArrayList<WireCommands.SegmentRead>();
        msgs.add(msgRead.take());
        msgs.add(msgRead.take());
        assertTrue(msgs.contains(readResponseGenerator.apply(session1.asLong())));
        assertTrue(msgs.contains(readResponseGenerator.apply(session1.asLong())));
    }

}

/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.client;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.common.Timer;
import io.pravega.common.concurrent.Futures;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.AppendDecoder;
import io.pravega.shared.protocol.netty.CommandDecoder;
import io.pravega.shared.protocol.netty.CommandEncoder;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.DelegatingRequestProcessor;
import io.pravega.shared.protocol.netty.ExceptionLoggingHandler;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.Request;
import io.pravega.shared.protocol.netty.RequestProcessor;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommands;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static io.pravega.shared.metrics.MetricNotifier.NO_OP_METRIC_NOTIFIER;
import static io.pravega.shared.protocol.netty.WireCommands.MAX_WIRECOMMAND_SIZE;
import static org.junit.Assert.assertEquals;

@Ignore("Client append path benchmark")
@Slf4j
public class AppendPathBenchmarkTest {
    private static final String HOST = "127.0.0.1";
    private static final int PORT = 9090;
    private static final String SCOPE = "scope";
    private static final String STREAM = "stream";
    private static final int EVENT_SIZE = 100;
    private static final int EVENT_COUNT = 100000;
    private static final int EVENT_COUNT_WARMUP = 100000;

    private MockConnectionFactoryImpl connectionFactory;
    private MockController controller;
    private EventStreamWriter<byte[]> writer;
    private byte[] payload;

    interface BenchConnection {
        void send(WireCommand cmd);
    }

    private static class ServerConnectionInboundHandler extends ChannelInboundHandlerAdapter implements BenchConnection {
        private final AtomicReference<RequestProcessor> processor = new AtomicReference<>();
        private final AtomicReference<Channel> channel = new AtomicReference<>();

        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            super.channelRegistered(ctx);
            channel.set(ctx.channel());
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            Request cmd = (Request) msg;
            cmd.process(processor.get());
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            // Close the connection when an exception is raised.
            ctx.close();
        }

        @Override
        public void send(WireCommand cmd) {
            Channel c = getChannel();
            // Work around for https://github.com/netty/netty/issues/3246
            EventLoop eventLoop = c.eventLoop();
            eventLoop.execute(() -> write(c, cmd));
        }

        private void write(Channel channel, WireCommand data) {
            channel.write(data).addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
        }

        private Channel getChannel() {
            Channel ch = channel.get();
            if (ch == null) {
                throw new IllegalStateException("Connection not yet established.");
            }
            return ch;
        }

        void setRequestProcessor(RequestProcessor rp) {
            processor.set(rp);
        }

        @Override
        public String toString() {
            Channel c = channel.get();
            if (c == null) {
                return "NewServerConnection";
            }
            return c.toString();
        }
    }

    private static class MockSegmentStoreServer {

        private static void startListening() {
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
                        public void initChannel(SocketChannel ch) {
                            ChannelPipeline p = ch.pipeline();

                            ServerConnectionInboundHandler handler = new ServerConnectionInboundHandler();
                            p.addLast(new ExceptionLoggingHandler(ch.remoteAddress().toString()),
                                    new CommandEncoder(null, NO_OP_METRIC_NOTIFIER),
                                    new LengthFieldBasedFrameDecoder(MAX_WIRECOMMAND_SIZE, 4, 4),
                                    new CommandDecoder(),
                                    new AppendDecoder(),
                                    handler);
                            handler.setRequestProcessor(new MockAppendProcessor(handler));
                        }
                    });

            // Start the server.
            b.bind(HOST, PORT).awaitUninterruptibly().channel();
            log.info("mock server start listening on {}:{}", HOST, PORT);
        }
    }

    private static class MockAppendProcessor extends DelegatingRequestProcessor {
        @NonNull
        private final BenchConnection connection;

        MockAppendProcessor(BenchConnection connection) {
            this.connection = connection;
        }

        @Override
        public void hello(WireCommands.Hello hello) {
            connection.send(new WireCommands.Hello(WireCommands.WIRE_VERSION, WireCommands.OLDEST_COMPATIBLE_VERSION));
        }

        @Override
        public RequestProcessor getNextRequestProcessor() {
            return null;
        }

        @Override
        public void setupAppend(WireCommands.SetupAppend setupAppend) {
            connection.send(new WireCommands.AppendSetup(setupAppend.getRequestId(), setupAppend.getSegment(), setupAppend.getWriterId(), Long.MIN_VALUE));
        }

        @Override
        public void append(Append append) {
            connection.send(new WireCommands.DataAppended(append.getRequestId(), append.getWriterId(), append.getEventNumber(),
                    0, 0));
        }

        @Override
        public void keepAlive(WireCommands.KeepAlive keepAlive) {}
    }

    @Before
    public void setup() throws ConnectionFailedException {
        PravegaNodeUri endpoint = new PravegaNodeUri(HOST, PORT);
        connectionFactory = new MockConnectionFactoryImpl();
        controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, false);
        controller.createScope(SCOPE);
        controller.createStream(SCOPE, STREAM, StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build());
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE, controller);
        EventWriterConfig writerConfig = EventWriterConfig.builder().build();
        this.writer = clientFactory.createEventWriter(STREAM, new JavaSerializer<byte[]>(), writerConfig);

        byte[] bytes = new byte[EVENT_SIZE];
        Arrays.fill( bytes, (byte) 0 );
        this.payload = bytes;
        assertEquals(payload.length, 100);
        MockSegmentStoreServer.startListening();
    }

    @After
    public void teardown() {
        writer.close();
        controller.close();
        connectionFactory.close();
    }

    @Test
    public void AppendPathBenchmark() {
        System.gc();
        List<CompletableFuture<Void>> list = new ArrayList<>();
        for (int i = 0; i < EVENT_COUNT_WARMUP; i++) {
            list.add(writer.writeEvent(payload));
        }
        Futures.allOf(list).join();
        list.clear();

        val timer = new Timer();
        for (int i = 0; i < EVENT_COUNT; i++) {
            list.add(writer.writeEvent(payload));
        }
        Futures.allOf(list).join();
        val elapsed = timer.getElapsed().toMillis();

        log.info("########################################################################################");
        log.info("AppendPath Benchmark: writes {} of {} byte size event to mock server in {} milliseconds", EVENT_COUNT, EVENT_SIZE, elapsed);
        log.info("########################################################################################");
    }
}
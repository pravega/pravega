/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import io.pravega.client.ClientConfig;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.segment.impl.ConditionalOutputStream;
import io.pravega.client.segment.impl.ConditionalOutputStreamFactoryImpl;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentMetadataClient;
import io.pravega.client.segment.impl.SegmentMetadataClientFactoryImpl;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentOutputStreamFactoryImpl;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.PendingEvent;
import io.pravega.client.stream.mock.MockController;
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.AppendProcessor;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.host.handler.PravegaRequestProcessor;
import io.pravega.segmentstore.server.host.handler.ServerConnectionInboundHandler;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.protocol.netty.AppendDecoder;
import io.pravega.shared.protocol.netty.CommandDecoder;
import io.pravega.shared.protocol.netty.CommandEncoder;
import io.pravega.shared.protocol.netty.ExceptionLoggingHandler;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.Request;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.test.common.TestUtils;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import lombok.Cleanup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.shared.protocol.netty.WireCommands.MAX_WIRECOMMAND_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class AppendReconnectTest {
    private Level originalLevel;
    private ServiceBuilder serviceBuilder;
    private final Consumer<Segment> segmentSealedCallback = segment -> { };

    @Before
    public void setup() throws Exception {
        originalLevel = ResourceLeakDetector.getLevel();
        ResourceLeakDetector.setLevel(Level.PARANOID);
        InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE);
        this.serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        this.serviceBuilder.initialize();
    }

    @After
    public void teardown() {
        this.serviceBuilder.close();
        ResourceLeakDetector.setLevel(originalLevel);
    }

    static Reply sendRequest(EmbeddedChannel channel, Request request) throws Exception {
        channel.writeInbound(request);
        Object encodedReply = channel.readOutbound();
        for (int i = 0; encodedReply == null && i < 50; i++) {
            channel.runPendingTasks();
            Thread.sleep(10);
            encodedReply = channel.readOutbound();
        }
        if (encodedReply == null) {
            throw new IllegalStateException("No reply to request: " + request);
        }
        WireCommand decoded = CommandDecoder.parseCommand((ByteBuf) encodedReply);
        ((ByteBuf) encodedReply).release();
        assertNotNull(decoded);
        return (Reply) decoded;
    }

    static EmbeddedChannel createChannel(StreamSegmentStore store) {
        ServerConnectionInboundHandler lsh = new ServerConnectionInboundHandler();
        EmbeddedChannel channel = new EmbeddedChannel(new ExceptionLoggingHandler(""),
                new CommandEncoder(null),
                new LengthFieldBasedFrameDecoder(MAX_WIRECOMMAND_SIZE, 4, 4),
                new CommandDecoder(),
                new AppendDecoder(),
                lsh);
        lsh.setRequestProcessor(new AppendProcessor(store, lsh, new PravegaRequestProcessor(store, mock(TableStore.class), lsh), null));
        return channel;
    }

    @Test(timeout = 30000)
    public void reconnectOnSegmentClient() throws Exception {
        String endpoint = "localhost";
        int port = TestUtils.getAvailableListenPort();
        byte[] payload = "Hello world\n".getBytes();
        String scope = "scope";
        String stream = "stream";
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();

        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, mock(TableStore.class));
        server.startListening();

        @Cleanup
        ConnectionFactoryImpl clientCF = new ConnectionFactoryImpl(ClientConfig.builder().build());
        Controller controller = new MockController(endpoint, port, clientCF);
        controller.createScope(scope);
        controller.createStream(scope, stream, StreamConfiguration.builder().build());

        SegmentOutputStreamFactoryImpl segmentClient = new SegmentOutputStreamFactoryImpl(controller, clientCF);

        Segment segment = Futures.getAndHandleExceptions(controller.getCurrentSegments(scope, stream), RuntimeException::new).getSegments().iterator().next();
        @Cleanup
        SegmentOutputStream out = segmentClient.createOutputStreamForSegment(segment, segmentSealedCallback, EventWriterConfig.builder().build(), "");
        CompletableFuture<Void> ack = new CompletableFuture<>();
        out.write(PendingEvent.withoutHeader(null, ByteBuffer.wrap(payload), ack));
        for (Channel c : clientCF.getActiveChannels()) {
            c.close();
        }
        CompletableFuture<Void> ack2 = new CompletableFuture<>();
        out.write(PendingEvent.withoutHeader(null, ByteBuffer.wrap(payload), ack2));
        ack.get(5, TimeUnit.SECONDS);
        ack2.get(5, TimeUnit.SECONDS);
        SegmentMetadataClient metadataClient = new SegmentMetadataClientFactoryImpl(controller, clientCF).createSegmentMetadataClient(segment, "");
        assertEquals(payload.length * 2, metadataClient.fetchCurrentSegmentLength());
    }
    
    @Test(timeout = 30000)
    public void reconnectThroughConditionalClient() throws Exception {
        String endpoint = "localhost";
        int port = TestUtils.getAvailableListenPort();
        byte[] payload = "Hello world\n".getBytes();
        String scope = "scope";
        String stream = "stream";
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();

        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, mock(TableStore.class));
        server.startListening();

        @Cleanup
        ConnectionFactoryImpl clientCF = new ConnectionFactoryImpl(ClientConfig.builder().build());
        Controller controller = new MockController(endpoint, port, clientCF);
        controller.createScope(scope);
        controller.createStream(scope, stream, StreamConfiguration.builder().build());

        ConditionalOutputStreamFactoryImpl segmentClient = new ConditionalOutputStreamFactoryImpl(controller, clientCF);

        Segment segment = Futures.getAndHandleExceptions(controller.getCurrentSegments(scope, stream), RuntimeException::new).getSegments().iterator().next();
        @Cleanup
        ConditionalOutputStream out = segmentClient.createConditionalOutputStream(segment, "", EventWriterConfig.builder().build());
        assertTrue(out.write(ByteBuffer.wrap(payload), 0));
        for (Channel c : clientCF.getActiveChannels()) {
            c.close();
        }
        assertTrue(out.write(ByteBuffer.wrap(payload), payload.length + WireCommands.TYPE_PLUS_LENGTH_SIZE));
        SegmentMetadataClient metadataClient = new SegmentMetadataClientFactoryImpl(controller, clientCF).createSegmentMetadataClient(segment, "");
        assertEquals((payload.length + WireCommands.TYPE_PLUS_LENGTH_SIZE) * 2,
                     metadataClient.fetchCurrentSegmentLength());
    }
}

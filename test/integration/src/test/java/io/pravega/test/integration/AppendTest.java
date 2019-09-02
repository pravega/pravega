/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import io.pravega.client.ClientConfig;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.segment.impl.ConditionalOutputStream;
import io.pravega.client.segment.impl.ConditionalOutputStreamFactoryImpl;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentOutputStreamFactoryImpl;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.PendingEvent;
import io.pravega.client.stream.mock.MockClientFactory;
import io.pravega.client.stream.mock.MockController;
import io.pravega.client.stream.mock.MockStreamManager;
import io.pravega.common.Timer;
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.AppendProcessor;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.host.handler.PravegaRequestProcessor;
import io.pravega.segmentstore.server.host.handler.ServerConnectionInboundHandler;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.AppendDecoder;
import io.pravega.shared.protocol.netty.CommandDecoder;
import io.pravega.shared.protocol.netty.CommandEncoder;
import io.pravega.shared.protocol.netty.ExceptionLoggingHandler;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.Request;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommands.AppendSetup;
import io.pravega.shared.protocol.netty.WireCommands.CreateSegment;
import io.pravega.shared.protocol.netty.WireCommands.DataAppended;
import io.pravega.shared.protocol.netty.WireCommands.Event;
import io.pravega.shared.protocol.netty.WireCommands.NoSuchSegment;
import io.pravega.shared.protocol.netty.WireCommands.SegmentCreated;
import io.pravega.shared.protocol.netty.WireCommands.SetupAppend;
import io.pravega.test.common.TestUtils;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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

public class AppendTest {
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

    @Test
    public void testSetupOnNonExistentSegment() throws Exception {
        String segment = "123";
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        @Cleanup
        EmbeddedChannel channel = createChannel(store);

        UUID uuid = UUID.randomUUID();
        NoSuchSegment setup = (NoSuchSegment) sendRequest(channel, new SetupAppend(1, uuid, segment, ""));

        assertEquals(segment, setup.getSegment());
    }

    @Test
    public void sendReceivingAppend() throws Exception {
        String segment = "123";
        ByteBuf data = Unpooled.wrappedBuffer("Hello world\n".getBytes());
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();

        @Cleanup
        EmbeddedChannel channel = createChannel(store);

        SegmentCreated created = (SegmentCreated) sendRequest(channel, new CreateSegment(1, segment, CreateSegment.NO_SCALE, 0, ""));
        assertEquals(segment, created.getSegment());

        UUID uuid = UUID.randomUUID();
        AppendSetup setup = (AppendSetup) sendRequest(channel, new SetupAppend(2, uuid, segment, ""));

        assertEquals(segment, setup.getSegment());
        assertEquals(uuid, setup.getWriterId());

        DataAppended ack = (DataAppended) sendRequest(channel,
                                                      new Append(segment, uuid, data.readableBytes(), new Event(data), 1L));
        assertEquals(uuid, ack.getWriterId());
        assertEquals(data.readableBytes(), ack.getEventNumber());
        assertEquals(Long.MIN_VALUE, ack.getPreviousEventNumber());
    }

    @Test(timeout = 10000)
    public void testMultipleAppends() throws Exception {
        String segment = "123";
        ByteBuf data = Unpooled.wrappedBuffer("Hello world\n".getBytes());
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        @Cleanup
        EmbeddedChannel channel = createChannel(store);

        SegmentCreated created = (SegmentCreated) sendRequest(channel, new CreateSegment(1, segment, CreateSegment.NO_SCALE, 0, ""));
        assertEquals(segment, created.getSegment());

        UUID uuid = UUID.randomUUID();
        AppendSetup setup = (AppendSetup) sendRequest(channel, new SetupAppend(2, uuid, segment, ""));

        assertEquals(segment, setup.getSegment());
        assertEquals(uuid, setup.getWriterId());

        data.retain();
        DataAppended ack = (DataAppended) sendRequest(channel,
                new Append(segment, uuid, 1, new Event(data), 1L));
        assertEquals(uuid, ack.getWriterId());
        assertEquals(1, ack.getEventNumber());
        assertEquals(Long.MIN_VALUE, ack.getPreviousEventNumber());

        DataAppended ack2 = (DataAppended) sendRequest(channel,
                new Append(segment, uuid, 2, new Event(data), 1L));
        assertEquals(uuid, ack2.getWriterId());
        assertEquals(2, ack2.getEventNumber());
        assertEquals(1, ack2.getPreviousEventNumber());
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

    @Test
    public void appendThroughSegmentClient() throws Exception {
        String endpoint = "localhost";
        int port = TestUtils.getAvailableListenPort();
        String testString = "Hello world\n";
        String scope = "scope";
        String stream = "stream";
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, tableStore);
        server.startListening();

        @Cleanup
        ConnectionFactory clientCF = new ConnectionFactoryImpl(ClientConfig.builder().build());
        Controller controller = new MockController(endpoint, port, clientCF, true);
        controller.createScope(scope);
        controller.createStream(scope, stream, StreamConfiguration.builder().build());

        SegmentOutputStreamFactoryImpl segmentClient = new SegmentOutputStreamFactoryImpl(controller, clientCF);

        Segment segment = Futures.getAndHandleExceptions(controller.getCurrentSegments(scope, stream), RuntimeException::new).getSegments().iterator().next();
        @Cleanup
        SegmentOutputStream out = segmentClient.createOutputStreamForSegment(segment, segmentSealedCallback, EventWriterConfig.builder().build(), "");
        CompletableFuture<Void> ack = new CompletableFuture<>();
        out.write(PendingEvent.withHeader(null, ByteBuffer.wrap(testString.getBytes()), ack));
        ack.get(5, TimeUnit.SECONDS);
    }
    
    @Test
    public void appendThroughConditionalClient() throws Exception {
        String endpoint = "localhost";
        int port = TestUtils.getAvailableListenPort();
        String testString = "Hello world\n";
        String scope = "scope";
        String stream = "stream";
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, tableStore);
        server.startListening();

        @Cleanup
        ConnectionFactory clientCF = new ConnectionFactoryImpl(ClientConfig.builder().build());
        Controller controller = new MockController(endpoint, port, clientCF, true);
        controller.createScope(scope);
        controller.createStream(scope, stream, StreamConfiguration.builder().build());

        ConditionalOutputStreamFactoryImpl segmentClient = new ConditionalOutputStreamFactoryImpl(controller, clientCF);

        Segment segment = Futures.getAndHandleExceptions(controller.getCurrentSegments(scope, stream), RuntimeException::new).getSegments().iterator().next();
        @Cleanup
        ConditionalOutputStream out = segmentClient.createConditionalOutputStream(segment, "", EventWriterConfig.builder().build());
        
        assertTrue(out.write(ByteBuffer.wrap(testString.getBytes()), 0));
    }

    @Test
    public void appendThroughStreamingClient() throws InterruptedException, ExecutionException, TimeoutException {
        String endpoint = "localhost";
        String streamName = "abc";
        int port = TestUtils.getAvailableListenPort();
        String testString = "Hello world\n";
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, tableStore);
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager("Scope", endpoint, port);
        @Cleanup
        MockClientFactory clientFactory = streamManager.getClientFactory();
        streamManager.createScope("Scope");
        streamManager.createStream("Scope", streamName, null);
        @Cleanup
        EventStreamWriter<String> producer = clientFactory.createEventWriter(streamName, new JavaSerializer<>(), EventWriterConfig.builder().build());
        Future<Void> ack = producer.writeEvent(testString);
        ack.get(5, TimeUnit.SECONDS);
    }
    
    @Test(timeout = 40000)
    public void miniBenchmark() throws InterruptedException, ExecutionException, TimeoutException {
        String endpoint = "localhost";
        String streamName = "abc";
        int port = TestUtils.getAvailableListenPort();
        String testString = "Hello world\n";
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, tableStore);
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager("Scope", endpoint, port);
        @Cleanup
        MockClientFactory clientFactory = streamManager.getClientFactory();
        streamManager.createScope("Scope");
        streamManager.createStream("Scope", streamName, null);
        @Cleanup
        EventStreamWriter<String> producer = clientFactory.createEventWriter(streamName, new JavaSerializer<>(), EventWriterConfig.builder().build());
        long blockingTime = timeWrites(testString, 200, producer, true);
        long nonBlockingTime = timeWrites(testString, 1000, producer, false);
        System.out.println("Blocking took: " + blockingTime + "ms.");
        System.out.println("Non blocking took: " + nonBlockingTime + "ms.");        
        assertTrue(blockingTime < 15000);
        assertTrue(nonBlockingTime < 15000);
    }

    private long timeWrites(String testString, int number, EventStreamWriter<String> producer, boolean synchronous)
            throws InterruptedException, ExecutionException, TimeoutException {
        Timer timer = new Timer();
        for (int i = 0; i < number; i++) {
            Future<Void> ack = producer.writeEvent(testString);
            if (synchronous) {
                ack.get(5, TimeUnit.SECONDS);
            }
        }
        producer.flush();
        return timer.getElapsedMillis();
    }
    
}

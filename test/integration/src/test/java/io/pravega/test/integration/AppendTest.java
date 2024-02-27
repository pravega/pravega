/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.test.integration;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.connection.impl.RawClient;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.security.auth.DelegationTokenProviderFactory;
import io.pravega.client.segment.impl.ConditionalOutputStream;
import io.pravega.client.segment.impl.ConditionalOutputStreamFactoryImpl;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentOutputStreamFactoryImpl;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ByteBufferSerializer;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.PendingEvent;
import io.pravega.client.stream.mock.MockClientFactory;
import io.pravega.client.stream.mock.MockController;
import io.pravega.client.stream.mock.MockStreamManager;
import io.pravega.common.Timer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.AppendProcessor;
import io.pravega.segmentstore.server.host.handler.IndexAppendProcessor;
import io.pravega.segmentstore.server.host.handler.IndexEntry;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.host.handler.PravegaRequestProcessor;
import io.pravega.segmentstore.server.host.handler.ServerConnectionInboundHandler;
import io.pravega.segmentstore.server.host.handler.TrackedConnection;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.segmentstore.server.writer.WriterConfig;
import io.pravega.shared.metrics.MetricNotifier;
import io.pravega.shared.protocol.netty.Append;
import io.pravega.shared.protocol.netty.AppendDecoder;
import io.pravega.shared.protocol.netty.CommandDecoder;
import io.pravega.shared.protocol.netty.CommandEncoder;
import io.pravega.shared.protocol.netty.ExceptionLoggingHandler;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.Reply;
import io.pravega.shared.protocol.netty.Request;
import io.pravega.shared.protocol.netty.WireCommand;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.AppendSetup;
import io.pravega.shared.protocol.netty.WireCommands.CreateSegment;
import io.pravega.shared.protocol.netty.WireCommands.DataAppended;
import io.pravega.shared.protocol.netty.WireCommands.Event;
import io.pravega.shared.protocol.netty.WireCommands.NoSuchSegment;
import io.pravega.shared.protocol.netty.WireCommands.SegmentCreated;
import io.pravega.shared.protocol.netty.WireCommands.SetupAppend;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.InlineExecutor;
import io.pravega.test.common.LeakDetectorTestSuite;
import io.pravega.test.common.TestUtils;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import lombok.Cleanup;
import lombok.val;
import lombok.extern.slf4j.Slf4j;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static io.pravega.shared.NameUtils.getIndexSegmentName;
import static io.pravega.shared.protocol.netty.WireCommands.MAX_WIRECOMMAND_SIZE;
import static io.pravega.shared.protocol.netty.WireCommands.TYPE_PLUS_LENGTH_SIZE;
import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@Slf4j
public class AppendTest extends LeakDetectorTestSuite {
    private static final ServiceBuilder SERVICE_BUILDER = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.builder()
                                                                              .include(ServiceConfig.builder().with(ServiceConfig.CONTAINER_COUNT, 1))
                                                                              .include(WriterConfig.builder().with(WriterConfig.MAX_ROLLOVER_SIZE, 10485760L))
                                                                              .build());
    private static final Duration TIMEOUT = Duration.ofMinutes(1);
    private static final InlineExecutor EXECUTOR = new InlineExecutor();
    private final Consumer<Segment> segmentSealedCallback = segment -> { };

    @BeforeClass
    public static void setup() throws Exception {
        SERVICE_BUILDER.initialize();
    }

    @AfterClass
    public static void teardown() {
        SERVICE_BUILDER.close();
        EXECUTOR.shutdown();
    }

    @Test(timeout = 10000)
    public void testSetupOnNonExistentSegment() throws Exception {
        String segment = "testSetupOnNonExistentSegment";
        StreamSegmentStore store = SERVICE_BUILDER.createStreamSegmentService();
        @Cleanup
        EmbeddedChannel channel = createChannel(store);

        UUID uuid = UUID.randomUUID();
        NoSuchSegment setup = (NoSuchSegment) sendRequest(channel, new SetupAppend(1, uuid, segment, ""));

        assertEquals(segment, setup.getSegment());
    }

    @Test(timeout = 10000)
    public void sendReceivingAppend() throws Exception {
        String segment = "sendReceivingAppend";
        ByteBuf data = Unpooled.wrappedBuffer("Hello world\n".getBytes());
        StreamSegmentStore store = SERVICE_BUILDER.createStreamSegmentService();

        @Cleanup
        EmbeddedChannel channel = createChannel(store);

        SegmentCreated created = (SegmentCreated) sendRequest(channel, new CreateSegment(1, segment, CreateSegment.NO_SCALE, 0, "", 1024L));
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
    public void sendLargeAppend() throws Exception {
        String segment = "sendLargeAppend";
        ByteBuf data = Unpooled.wrappedBuffer(new byte[Serializer.MAX_EVENT_SIZE]);
        StreamSegmentStore store = SERVICE_BUILDER.createStreamSegmentService();

        @Cleanup
        EmbeddedChannel channel = createChannel(store);

        SegmentCreated created = (SegmentCreated) sendRequest(channel, new CreateSegment(1, segment, CreateSegment.NO_SCALE, 0, "", 1024L));
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
        String segment = "testMultipleAppends";
        ByteBuf data = Unpooled.wrappedBuffer("Hello world\n".getBytes());
        StreamSegmentStore store = SERVICE_BUILDER.createStreamSegmentService();
        @Cleanup
        EmbeddedChannel channel = createChannel(store);

        SegmentCreated created = (SegmentCreated) sendRequest(channel, new CreateSegment(1, segment, CreateSegment.NO_SCALE, 0, "", 1024L));
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

        WireCommands.SegmentDeleted deleted = (WireCommands.SegmentDeleted) sendRequest(channel, new WireCommands.DeleteSegment(1, segment, ""));
        assertEquals(segment, deleted.getSegment());

        //validates that index segment is deleted
        assertThrows(StreamSegmentNotExistsException.class, () -> store.getStreamSegmentInfo(getIndexSegmentName(segment), Duration.ofMinutes(1)).join());
    }

    static Reply sendRequest(EmbeddedChannel channel, Request request) throws Exception {
        channel.writeInbound(request);
        log.info("Request {} sent to Segment store", request);
        Object encodedReply = channel.readOutbound();
        for (int i = 0; encodedReply == null && i < 500; i++) {
            channel.runPendingTasks();
            Thread.sleep(10);
            encodedReply = channel.readOutbound();
        }
        if (encodedReply == null) {
            log.error("Error while try waiting for a response from Segment Store");
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
                new CommandEncoder(null, MetricNotifier.NO_OP_METRIC_NOTIFIER),
                new LengthFieldBasedFrameDecoder(MAX_WIRECOMMAND_SIZE, 4, 4),
                new CommandDecoder(),
                new AppendDecoder(),
                lsh);
        IndexAppendProcessor indexAppendProcessor = new IndexAppendProcessor(EXECUTOR, store);
        lsh.setRequestProcessor(AppendProcessor.defaultBuilder(indexAppendProcessor)
                                               .store(store)
                                               .connection(new TrackedConnection(lsh))
                                               .nextRequestProcessor(new PravegaRequestProcessor(store, mock(TableStore.class), lsh,
                                                       indexAppendProcessor))
                                               .build());
        return channel;
    }

    @Test(timeout = 10000)
    public void appendThroughSegmentClient() throws Exception {
        String endpoint = "localhost";
        int port = TestUtils.getAvailableListenPort();
        String testString = "Hello world\n";
        String scope = "scope";
        String stream = "appendThroughSegmentClient";
        StreamSegmentStore store = SERVICE_BUILDER.createStreamSegmentService();
        TableStore tableStore = SERVICE_BUILDER.createTableStoreService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, tableStore,
                SERVICE_BUILDER.getLowPriorityExecutor(), new IndexAppendProcessor(SERVICE_BUILDER.getLowPriorityExecutor(), store));
        server.startListening();

        @Cleanup
        SocketConnectionFactoryImpl clientCF = new SocketConnectionFactoryImpl(ClientConfig.builder().build());
        @Cleanup
        ConnectionPoolImpl connectionPool = new ConnectionPoolImpl(ClientConfig.builder().build(), clientCF);
        @Cleanup
        Controller controller = new MockController(endpoint, port, connectionPool, true);
        controller.createScope(scope);
        controller.createStream(scope, stream, StreamConfiguration.builder().build());

        SegmentOutputStreamFactoryImpl segmentClient = new SegmentOutputStreamFactoryImpl(controller, connectionPool);

        Segment segment = Futures.getAndHandleExceptions(controller.getCurrentSegments(scope, stream), RuntimeException::new).getSegments().iterator().next();
        @Cleanup
        SegmentOutputStream out = segmentClient.createOutputStreamForSegment(segment, segmentSealedCallback, EventWriterConfig.builder().build(),
                DelegationTokenProviderFactory.createWithEmptyToken());
        CompletableFuture<Void> ack = new CompletableFuture<>();
        out.write(PendingEvent.withHeader(null, ByteBuffer.wrap(testString.getBytes()), ack));
        ack.get(5, TimeUnit.SECONDS);
    }
    
    @Test(timeout = 10000)
    public void appendThroughConditionalClient() throws Exception {
        String endpoint = "localhost";
        int port = TestUtils.getAvailableListenPort();
        String testString = "Hello world\n";
        String scope = "scope";
        String stream = "appendThroughConditionalClient";
        StreamSegmentStore store = SERVICE_BUILDER.createStreamSegmentService();
        TableStore tableStore = SERVICE_BUILDER.createTableStoreService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, tableStore,
                SERVICE_BUILDER.getLowPriorityExecutor(), new IndexAppendProcessor(SERVICE_BUILDER.getLowPriorityExecutor(), store));
        server.startListening();

        @Cleanup
        SocketConnectionFactoryImpl clientCF = new SocketConnectionFactoryImpl(ClientConfig.builder().build());
        @Cleanup
        ConnectionPoolImpl connectionPool = new ConnectionPoolImpl(ClientConfig.builder().build(), clientCF);
        @Cleanup
        Controller controller = new MockController(endpoint, port, connectionPool, true);
        controller.createScope(scope);
        controller.createStream(scope, stream, StreamConfiguration.builder().build());

        ConditionalOutputStreamFactoryImpl segmentClient = new ConditionalOutputStreamFactoryImpl(controller, connectionPool);

        Segment segment = Futures.getAndHandleExceptions(controller.getCurrentSegments(scope, stream), RuntimeException::new).getSegments().iterator().next();
        @Cleanup
        ConditionalOutputStream out = segmentClient.createConditionalOutputStream(segment,
                DelegationTokenProviderFactory.createWithEmptyToken(), EventWriterConfig.builder().build());
        
        assertTrue(out.write(ByteBuffer.wrap(testString.getBytes()), 0));
    }

    @Test(timeout = 10000)
    public void appendThroughStreamingClient() throws InterruptedException, ExecutionException, TimeoutException {
        String endpoint = "localhost";
        String streamName = "appendThroughStreamingClient";
        int port = TestUtils.getAvailableListenPort();
        String testString = "Hello world\n";
        StreamSegmentStore store = SERVICE_BUILDER.createStreamSegmentService();
        TableStore tableStore = SERVICE_BUILDER.createTableStoreService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, tableStore,
                SERVICE_BUILDER.getLowPriorityExecutor(), new IndexAppendProcessor(SERVICE_BUILDER.getLowPriorityExecutor(), store));
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
    

    @Test(timeout = 100000)
    public void appendALotOfData() {
        String endpoint = "localhost";
        String scope = "Scope";
        String streamName = "appendALotOfData";
        int port = TestUtils.getAvailableListenPort();
        long heapSize = Runtime.getRuntime().maxMemory();
        long messageSize = Math.min(1024 * 1024, heapSize / 20000);
        ByteBuffer payload = ByteBuffer.allocate((int) messageSize);
        StreamSegmentStore store = SERVICE_BUILDER.createStreamSegmentService();
        TableStore tableStore = SERVICE_BUILDER.createTableStoreService();
        @Cleanup("shutdown")
        InlineExecutor tokenExpiryExecutor = new InlineExecutor();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, tableStore, tokenExpiryExecutor,
                new IndexAppendProcessor(tokenExpiryExecutor, store));
        server.startListening();
        ClientConfig config = ClientConfig.builder().build();
        SocketConnectionFactoryImpl clientCF = new SocketConnectionFactoryImpl(config);
        @Cleanup
        ConnectionPoolImpl connectionPool = new ConnectionPoolImpl(config, clientCF);
        Controller controller = new MockController(endpoint, port, connectionPool, true);
        @Cleanup
        StreamManagerImpl streamManager = new StreamManagerImpl(controller, connectionPool);
        streamManager.createScope(scope);
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(scope, controller, config);
        streamManager.createStream("Scope", streamName,
                                   StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
        @Cleanup
        EventStreamWriter<ByteBuffer> producer = clientFactory.createEventWriter(streamName, new ByteBufferSerializer(),
                                                                                 EventWriterConfig.builder().build());
        @Cleanup
        RawClient rawClient = new RawClient(new PravegaNodeUri(endpoint, port), connectionPool);
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 100; j++) {
                producer.writeEvent(payload.slice());
            }
            producer.flush();
            long requestId = rawClient.getFlow().getNextSequenceNumber();
            String scopedName = new Segment(scope, streamName, 0).getScopedName();
            WireCommands.TruncateSegment request = new WireCommands.TruncateSegment(requestId, scopedName,
                                                                                    i * 100L * (payload.remaining() + TYPE_PLUS_LENGTH_SIZE), "");
            Reply reply = rawClient.sendRequest(requestId, request).join();
            assertFalse(reply.toString(), reply.isFailure());
        }
        producer.close();
    }

    @Test(timeout = 10000)
    public void testMultipleIndexAppends() throws Exception {
        String segment = "testMultipleIndexAppends/testStream/0";
        String indexSegment = getIndexSegmentName(segment);
        ByteBuf data = Unpooled.wrappedBuffer("Hello world\n".getBytes());
        StreamSegmentStore store = SERVICE_BUILDER.createStreamSegmentService();

        @Cleanup
        EmbeddedChannel channel = createChannel(store);

        SegmentCreated created = (SegmentCreated) sendRequest(channel, new CreateSegment(1, segment, CreateSegment.NO_SCALE, 0, "", 1024L));
        assertEquals(segment, created.getSegment());

        UUID uuid = UUID.randomUUID();
        AppendSetup setup = (AppendSetup) sendRequest(channel, new SetupAppend(2, uuid, segment, ""));

        assertEquals(segment, setup.getSegment());
        assertEquals(uuid, setup.getWriterId());

        data.retain();
        // Append 1 on main Segment
        DataAppended ack = (DataAppended) sendRequest(channel,
                new Append(segment, uuid, 1, new Event(data), 1L));
        assertEquals(uuid, ack.getWriterId());
        assertEquals(1, ack.getEventNumber());
        assertEquals(Long.MIN_VALUE, ack.getPreviousEventNumber());

        // Read the Actual data from main Segment. Will be helpful in asserting the indexdata
        WireCommands.SegmentRead result = (WireCommands.SegmentRead) sendRequest(channel, new WireCommands.ReadSegment(segment, 0, 20, "", 1L));
        ByteBuf resultAfterOneAppend = result.getData();

        // As only one append is done on Main segment we should have Event_count on index segment as 1
        assertEventCountAttributeforSegment(indexSegment, store, 1);

        //Append 2 on main segment
        DataAppended ack2 = (DataAppended) sendRequest(channel,
                new Append(segment, uuid, 2, new Event(data), 1L));
        assertEquals(uuid, ack2.getWriterId());
        assertEquals(2, ack2.getEventNumber());
        assertEquals(1, ack2.getPreviousEventNumber());

        // Read the Actual data from main Segment. Will be helpfull in asserting the indexdata
        WireCommands.SegmentRead resultMain = (WireCommands.SegmentRead) sendRequest(channel, new WireCommands.ReadSegment(segment, 0, 100, "", 1L));
        ByteBuf resultAfterTwoAppend = resultMain.getData();
        assertEquals(2 * resultAfterOneAppend.readableBytes(), resultAfterTwoAppend.readableBytes());

        // Read IndexSegment from 0 Offset.
        int readOffset = 0;
        WireCommands.SegmentRead resultIndexSegment1 = (WireCommands.SegmentRead) sendRequest(channel, new WireCommands.ReadSegment(indexSegment, readOffset, 40, "", 1L));
        ByteBuf actual1 = Unpooled.buffer(100);
        actual1.writeBytes(resultIndexSegment1.getData());
        IndexEntry indexEntry1 = IndexEntry.fromBytes(BufferView.wrap(actual1.array()));
        assertEquals(1, indexEntry1.getEventCount());
        assertEquals(resultAfterOneAppend.readableBytes(), indexEntry1.getOffset());

        // Read IndexSegment from next Offset this will give second append info
        readOffset += indexEntry1.toBytes().getLength();
        WireCommands.SegmentRead resultIndexSegment2 = (WireCommands.SegmentRead) sendRequest(channel, new WireCommands.ReadSegment(indexSegment, readOffset, 32, "", 1L));
        ByteBuf actual2 = Unpooled.buffer(32);
        actual2.writeBytes(resultIndexSegment2.getData());
        IndexEntry indexEntry2 = IndexEntry.fromBytes(BufferView.wrap(actual2.array()));

        // check the getSegment info and chck the attribute EVENt_COUNT

        assertEquals(2, indexEntry2.getEventCount());
        assertEquals(resultAfterTwoAppend.readableBytes(), indexEntry2.getOffset());

        // Two appends are done on Main segment we should have Event_count on index segment as 2
        assertEventCountAttributeforSegment(indexSegment, store, 2);
    }

    private void assertEventCountAttributeforSegment(String segment, StreamSegmentStore store, long expectedEventCount) throws Exception {
        // Assert Index Segment attribute values.
        AssertExtensions.assertEventuallyEquals(Long.valueOf(expectedEventCount), () -> {
            val si = store.getStreamSegmentInfo(segment, TIMEOUT).join();
            val segmentType = SegmentType.fromAttributes(si.getAttributes());
            assertFalse(segmentType.isInternal() || segmentType.isCritical() || segmentType.isSystem() || segmentType.isTableSegment());
            assertEquals(SegmentType.STREAM_SEGMENT, segmentType);
            return si.getAttributes().get(Attributes.EVENT_COUNT);
        }, 5000);
    }

    @Test(timeout = 20000)
    public void miniBenchmark() throws InterruptedException, ExecutionException, TimeoutException {
        String endpoint = "localhost";
        String streamName = "miniBenchmark";
        int port = TestUtils.getAvailableListenPort();
        byte[] testPayload = new byte[1000];
        StreamSegmentStore store = SERVICE_BUILDER.createStreamSegmentService();
        TableStore tableStore = SERVICE_BUILDER.createTableStoreService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, tableStore, SERVICE_BUILDER.getLowPriorityExecutor(),
                new IndexAppendProcessor(SERVICE_BUILDER.getLowPriorityExecutor(), store));
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager("Scope", endpoint, port);
        @Cleanup
        MockClientFactory clientFactory = streamManager.getClientFactory();
        streamManager.createScope("Scope");
        streamManager.createStream("Scope", streamName, null);
        @Cleanup
        EventStreamWriter<ByteBuffer> producer = clientFactory.createEventWriter(streamName, new ByteBufferSerializer(), EventWriterConfig.builder().build());
        long blockingTime = timeWrites(testPayload, 3000, producer, true);
        long nonBlockingTime = timeWrites(testPayload, 60000, producer, false);
        System.out.println("Blocking took: " + blockingTime + "ms.");
        System.out.println("Non blocking took: " + nonBlockingTime + "ms.");
        assertTrue(blockingTime < 10000);
        assertTrue(nonBlockingTime < 10000);
    }

    private long timeWrites(byte[] testPayload, int number, EventStreamWriter<ByteBuffer> producer, boolean synchronous)
            throws InterruptedException, ExecutionException, TimeoutException {
        Timer timer = new Timer();
        AtomicLong maxLatency = new AtomicLong(0);
        for (int i = 0; i < number; i++) {
            Timer latencyTimer = new Timer();
            CompletableFuture<Void> ack = producer.writeEvent(ByteBuffer.wrap(testPayload));
            ack.thenRun(() -> {
                long elapsed = latencyTimer.getElapsedNanos();
                maxLatency.getAndUpdate(l -> Math.max(elapsed, l));
            });
            if (synchronous) {
                ack.get(5, TimeUnit.SECONDS);
            }
        }
        producer.flush();
        System.out.println("Max latency: " + (maxLatency.get() / 1000000.0));
        return timer.getElapsedMillis();
    }

}

/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.integrationtests;

import com.emc.pravega.common.Timer;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.netty.Append;
import com.emc.pravega.common.netty.AppendDecoder;
import com.emc.pravega.common.netty.CommandDecoder;
import com.emc.pravega.common.netty.CommandEncoder;
import com.emc.pravega.common.netty.ExceptionLoggingHandler;
import com.emc.pravega.common.netty.Reply;
import com.emc.pravega.common.netty.Request;
import com.emc.pravega.common.netty.WireCommand;
import com.emc.pravega.common.netty.WireCommands.AppendSetup;
import com.emc.pravega.common.netty.WireCommands.CreateSegment;
import com.emc.pravega.common.netty.WireCommands.DataAppended;
import com.emc.pravega.common.netty.WireCommands.NoSuchSegment;
import com.emc.pravega.common.netty.WireCommands.SegmentCreated;
import com.emc.pravega.common.netty.WireCommands.SetupAppend;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.host.handler.AppendProcessor;
import com.emc.pravega.service.server.host.handler.PravegaConnectionListener;
import com.emc.pravega.service.server.host.handler.PravegaRequestProcessor;
import com.emc.pravega.service.server.host.handler.ServerConnectionInboundHandler;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.impl.PendingEvent;
import com.emc.pravega.stream.impl.netty.ConnectionFactory;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;
import com.emc.pravega.stream.impl.segment.SegmentOutputStream;
import com.emc.pravega.stream.impl.segment.SegmentOutputStreamFactoryImpl;
import com.emc.pravega.stream.mock.MockClientFactory;
import com.emc.pravega.stream.mock.MockController;
import com.emc.pravega.stream.mock.MockStreamManager;
import com.emc.pravega.testcommon.TestUtils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import lombok.Cleanup;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.emc.pravega.common.netty.WireCommands.MAX_WIRECOMMAND_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AppendTest {
    private Level originalLevel;
    private ServiceBuilder serviceBuilder;

    @Before
    public void setup() throws Exception {
        originalLevel = ResourceLeakDetector.getLevel();
        ResourceLeakDetector.setLevel(Level.PARANOID);
        InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE);
        this.serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        this.serviceBuilder.initialize().get();
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

        EmbeddedChannel channel = createChannel(store);

        UUID uuid = UUID.randomUUID();
        NoSuchSegment setup = (NoSuchSegment) sendRequest(channel, new SetupAppend(uuid, segment));

        assertEquals(segment, setup.getSegment());
    }

    @Test
    public void sendReceivingAppend() throws Exception {
        String segment = "123";
        ByteBuf data = Unpooled.wrappedBuffer("Hello world\n".getBytes());
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();

        EmbeddedChannel channel = createChannel(store);

        SegmentCreated created = (SegmentCreated) sendRequest(channel, new CreateSegment(segment, CreateSegment.NO_SCALE, 0));
        assertEquals(segment, created.getSegment());

        UUID uuid = UUID.randomUUID();
        AppendSetup setup = (AppendSetup) sendRequest(channel, new SetupAppend(uuid, segment));

        assertEquals(segment, setup.getSegment());
        assertEquals(uuid, setup.getConnectionId());

        DataAppended ack = (DataAppended) sendRequest(channel,
                                                      new Append(segment, uuid, data.readableBytes(), data, null));
        assertEquals(uuid, ack.getConnectionId());
        assertEquals(data.readableBytes(), ack.getEventNumber());
    }

    static Reply sendRequest(EmbeddedChannel channel, Request request) throws Exception {
        channel.writeInbound(request);
        Object encodedReply = channel.readOutbound();
        for (int i = 0; encodedReply == null && i < 50; i++) {
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
        lsh.setRequestProcessor(new AppendProcessor(store, lsh, new PravegaRequestProcessor(store, lsh)));
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
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store);
        server.startListening();

        ConnectionFactory clientCF = new ConnectionFactoryImpl(false);
        Controller controller = new MockController(endpoint, port, clientCF);
        controller.createScope(scope);
        controller.createStream(StreamConfiguration.builder().scope(scope).streamName(stream).build());

        SegmentOutputStreamFactoryImpl segmentClient = new SegmentOutputStreamFactoryImpl(controller, clientCF);

        Segment segment = FutureHelpers.getAndHandleExceptions(controller.getCurrentSegments(scope, stream), RuntimeException::new).getSegments().iterator().next();
        @Cleanup("close")
        SegmentOutputStream out = segmentClient.createOutputStreamForSegment(segment);
        CompletableFuture<Boolean> ack = new CompletableFuture<>();
        out.write(new PendingEvent(null, ByteBuffer.wrap(testString.getBytes()), ack));
        assertTrue(ack.get(5, TimeUnit.SECONDS));
    }

    @Test
    public void appendThroughStreamingClient() throws InterruptedException, ExecutionException, TimeoutException {
        String endpoint = "localhost";
        String streamName = "abc";
        int port = TestUtils.getAvailableListenPort();
        String testString = "Hello world\n";
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store);
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager("Scope", endpoint, port);
        MockClientFactory clientFactory = streamManager.getClientFactory();
        streamManager.createScope("Scope");
        streamManager.createStream("Scope", streamName, null);
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
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store);
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager("Scope", endpoint, port);
        @Cleanup
        MockClientFactory clientFactory = streamManager.getClientFactory();
        streamManager.createScope("Scope");
        streamManager.createStream("Scope", streamName, null);
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

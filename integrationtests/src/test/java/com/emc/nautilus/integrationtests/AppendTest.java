package com.emc.nautilus.integrationtests;

import com.emc.logservice.contracts.StreamSegmentStore;
import com.emc.logservice.server.host.handler.AppendProcessor;
import com.emc.logservice.server.host.handler.LogServiceConnectionListener;
import com.emc.logservice.server.host.handler.LogServiceRequestProcessor;
import com.emc.logservice.server.host.handler.ServerConnectionInboundHandler;
import com.emc.logservice.server.mocks.InMemoryServiceBuilder;
import com.emc.logservice.server.service.ServiceBuilder;
import com.emc.logservice.server.service.ServiceBuilderConfig;
import com.emc.nautilus.common.netty.CommandDecoder;
import com.emc.nautilus.common.netty.CommandEncoder;
import com.emc.nautilus.common.netty.ConnectionFactory;
import com.emc.nautilus.common.netty.Reply;
import com.emc.nautilus.common.netty.Request;
import com.emc.nautilus.common.netty.WireCommand;
import com.emc.nautilus.common.netty.WireCommands.Append;
import com.emc.nautilus.common.netty.WireCommands.AppendSetup;
import com.emc.nautilus.common.netty.WireCommands.CreateSegment;
import com.emc.nautilus.common.netty.WireCommands.DataAppended;
import com.emc.nautilus.common.netty.WireCommands.NoSuchSegment;
import com.emc.nautilus.common.netty.WireCommands.SegmentCreated;
import com.emc.nautilus.common.netty.WireCommands.SetupAppend;
import com.emc.nautilus.stream.Producer;
import com.emc.nautilus.stream.ProducerConfig;
import com.emc.nautilus.stream.Stream;
import com.emc.nautilus.stream.impl.JavaSerializer;
import com.emc.nautilus.stream.impl.SingleSegmentStreamManagerImpl;
import com.emc.nautilus.stream.impl.netty.ConnectionFactoryImpl;
import com.emc.nautilus.stream.segment.SegmentOutputStream;
import com.emc.nautilus.stream.segment.impl.SegmentManagerImpl;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;
import lombok.Cleanup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.emc.nautilus.common.netty.WireCommands.MAX_WIRECOMMAND_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AppendTest {
    private Level originalLevel;
    private ServiceBuilder serviceBuilder;

    @Before
    public void setup() throws Exception {
        originalLevel = ResourceLeakDetector.getLevel();
        ResourceLeakDetector.setLevel(Level.PARANOID);

        this.serviceBuilder = new InMemoryServiceBuilder(ServiceBuilderConfig.getDefaultConfig());
        this.serviceBuilder.getContainerManager().initialize(Duration.ofMinutes(1)).get();
    }

    @After
    public void teardown() {
        this.serviceBuilder.close();
        ResourceLeakDetector.setLevel(originalLevel);
    }

    @Test
    public void testSetupOnNonExistentSegment() throws Exception {
        String segment = "123";
        ByteBuf data = Unpooled.wrappedBuffer("Hello world\n".getBytes());
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();

        EmbeddedChannel channel = createChannel(store);
        CommandDecoder decoder = new CommandDecoder();

        UUID uuid = UUID.randomUUID();
        NoSuchSegment setup = (NoSuchSegment) sendRequest(channel, decoder, new SetupAppend(uuid, segment));

        assertEquals(segment, setup.getSegment());
    }

    @Test
    public void sendReceivingAppend() throws Exception {
        String segment = "123";
        ByteBuf data = Unpooled.wrappedBuffer("Hello world\n".getBytes());
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();

        EmbeddedChannel channel = createChannel(store);
        CommandDecoder decoder = new CommandDecoder();

        SegmentCreated created = (SegmentCreated) sendRequest(channel, decoder, new CreateSegment(segment));
        assertEquals(segment, created.getSegment());

        UUID uuid = UUID.randomUUID();
        AppendSetup setup = (AppendSetup) sendRequest(channel, decoder, new SetupAppend(uuid, segment));

        assertEquals(segment, setup.getSegment());
        assertEquals(uuid, setup.getConnectionId());

        DataAppended ack = (DataAppended) sendRequest(channel,
                                                      decoder,
                                                      new Append(segment, uuid, data.readableBytes(), data));
        assertEquals(uuid, ack.getConnectionId());
        assertEquals(data.readableBytes(), ack.getEventNumber());
    }

    private Reply sendRequest(EmbeddedChannel channel, CommandDecoder decoder, Request request) throws Exception {
        channel.writeInbound(request);
        Object encodedReply = channel.readOutbound();
        for (int i = 0; encodedReply == null && i < 50; i++) {
            Thread.sleep(10);
            encodedReply = channel.readOutbound();
        }
        if (encodedReply == null) {
            throw new IllegalStateException("No reply to request: " + request);
        }
        WireCommand decoded = decoder.parseCommand((ByteBuf) encodedReply);
        ((ByteBuf) encodedReply).release();
        assertNotNull(decoded);
        decoded = decoder.processCommand(decoded);
        assertNotNull(decoded);
        return (Reply) decoded;
    }

    private EmbeddedChannel createChannel(StreamSegmentStore store) {
        ServerConnectionInboundHandler lsh = new ServerConnectionInboundHandler();
        lsh.setRequestProcessor(new AppendProcessor(store, lsh, new LogServiceRequestProcessor(store, lsh)));
        EmbeddedChannel channel = new EmbeddedChannel(new CommandEncoder(),
                new LengthFieldBasedFrameDecoder(MAX_WIRECOMMAND_SIZE, 4, 4),
                new CommandDecoder(),
                lsh);
        return channel;
    }

    @Test
    public void appendThroughLogClient() throws Exception {
        String endpoint = "localhost";
        String segmentName = "abc";
        int port = 8765;
        String testString = "Hello world\n";
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        @Cleanup
        LogServiceConnectionListener server = new LogServiceConnectionListener(false, port, store);
        server.startListening();

        ConnectionFactory clientCF = new ConnectionFactoryImpl(false, port);
        SegmentManagerImpl logClient = new SegmentManagerImpl(endpoint, clientCF);
        logClient.createSegment(segmentName);
        @Cleanup("close")
        SegmentOutputStream out = logClient.openSegmentForAppending(segmentName, null);
        CompletableFuture<Void> ack = new CompletableFuture<>();
        out.write(ByteBuffer.wrap(testString.getBytes()), ack);
        out.flush();
        assertEquals(null, ack.get(5, TimeUnit.SECONDS));
    }

    @Test
    public void appendThroughStreamingClient() {
        String endpoint = "localhost";
        String streamName = "abc";
        int port = 8910;
        String testString = "Hello world\n";
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        @Cleanup
        LogServiceConnectionListener server = new LogServiceConnectionListener(false, port, store);
        server.startListening();
        @Cleanup
        SingleSegmentStreamManagerImpl streamManager = new SingleSegmentStreamManagerImpl(endpoint, port, "Scope");
        Stream stream = streamManager.createStream(streamName, null);
        @Cleanup
        Producer<String> producer = stream.createProducer(new JavaSerializer<>(), new ProducerConfig(null));
        producer.publish("RoutingKey", testString);
        producer.flush();
    }
}

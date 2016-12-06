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

package com.emc.pravega.integrationtests;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.netty.Append;
import com.emc.pravega.common.netty.AppendDecoder;
import com.emc.pravega.common.netty.CommandDecoder;
import com.emc.pravega.common.netty.CommandEncoder;
import com.emc.pravega.common.netty.ConnectionFactory;
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
import com.emc.pravega.stream.Producer;
import com.emc.pravega.stream.ProducerConfig;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.impl.StreamConfigurationImpl;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;
import com.emc.pravega.stream.impl.segment.SegmentOutputStream;
import com.emc.pravega.stream.impl.segment.SegmentOutputStreamFactoryImpl;
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
import lombok.Cleanup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.emc.pravega.common.netty.WireCommands.MAX_WIRECOMMAND_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AppendTest {
    private Level originalLevel;
    private ServiceBuilder serviceBuilder;

    @Before
    public void setup() throws Exception {
        originalLevel = ResourceLeakDetector.getLevel();
        ResourceLeakDetector.setLevel(Level.PARANOID);
        InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
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

        SegmentCreated created = (SegmentCreated) sendRequest(channel, new CreateSegment(segment));
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
                new CommandEncoder(),
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
        int port = TestUtils.randomPort();
        String testString = "Hello world\n";
        String scope = "scope";
        String stream = "stream";
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store);
        server.startListening();

        ConnectionFactory clientCF = new ConnectionFactoryImpl(false);
        Controller controller = new MockController(endpoint, port, clientCF);
        controller.createStream(new StreamConfigurationImpl(scope, stream, null));

        SegmentOutputStreamFactoryImpl segmentClient = new SegmentOutputStreamFactoryImpl(controller, clientCF);

        Segment segment = FutureHelpers.getAndHandleExceptions(controller.getCurrentSegments(scope, stream), RuntimeException::new).getSegments().iterator().next();
        @Cleanup("close")
        SegmentOutputStream out = segmentClient.createOutputStreamForSegment(segment, null);
        CompletableFuture<Boolean> ack = new CompletableFuture<>();
        out.write(ByteBuffer.wrap(testString.getBytes()), ack);
        out.flush();
        assertEquals(true, ack.get(5, TimeUnit.SECONDS));
    }

    @Test
    public void appendThroughStreamingClient() throws InterruptedException {
        String endpoint = "localhost";
        String streamName = "abc";
        int port = TestUtils.randomPort();
        String testString = "Hello world\n";
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store);
        server.startListening();

        @Cleanup
        MockStreamManager streamManager = new MockStreamManager("Scope", endpoint, port);
        Stream stream = streamManager.createStream(streamName, null);

        Producer<String> producer = stream.createProducer(new JavaSerializer<>(), new ProducerConfig(null));
        producer.publish("RoutingKey", testString);
        producer.flush();
    }
}

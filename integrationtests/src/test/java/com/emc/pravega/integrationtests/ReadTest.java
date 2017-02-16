/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
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
import com.emc.pravega.common.netty.WireCommands.ReadSegment;
import com.emc.pravega.common.netty.WireCommands.SegmentRead;
import com.emc.pravega.service.contracts.AppendContext;
import com.emc.pravega.service.contracts.ReadResult;
import com.emc.pravega.service.contracts.ReadResultEntry;
import com.emc.pravega.service.contracts.ReadResultEntryContents;
import com.emc.pravega.service.contracts.ReadResultEntryType;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.host.handler.PravegaConnectionListener;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.stream.EventPointer;
import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.ReaderConfig;
import com.emc.pravega.stream.ReaderGroupConfig;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Sequence;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.impl.StreamConfigurationImpl;
import com.emc.pravega.stream.impl.netty.ConnectionFactory;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;
import com.emc.pravega.stream.impl.segment.EndOfSegmentException;
import com.emc.pravega.stream.impl.segment.NoSuchEventException;
import com.emc.pravega.stream.impl.segment.SegmentInputConfiguration;
import com.emc.pravega.stream.impl.segment.SegmentInputStream;
import com.emc.pravega.stream.impl.segment.SegmentInputStreamFactoryImpl;
import com.emc.pravega.stream.impl.segment.SegmentOutputStream;
import com.emc.pravega.stream.impl.segment.SegmentOutputStreamFactoryImpl;
import com.emc.pravega.stream.impl.segment.SegmentSealedException;
import com.emc.pravega.stream.mock.MockClientFactory;
import com.emc.pravega.stream.mock.MockController;
import com.emc.pravega.stream.mock.MockStreamManager;
import com.emc.pravega.testcommon.TestUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import lombok.Cleanup;

public class ReadTest {

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
    public void testReadDirectlyFromStore() throws InterruptedException, ExecutionException, IOException {
        String segmentName = "testReadFromStore";
        final int entries = 10;
        final byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        UUID clientId = UUID.randomUUID();

        StreamSegmentStore segmentStore = serviceBuilder.createStreamSegmentService();

        fillStoreForSegment(segmentName, clientId, data, entries, segmentStore);

        ReadResult result = segmentStore.read(segmentName, 0, entries * data.length, Duration.ZERO).get();
        int index = 0;
        while (result.hasNext()) {
            ReadResultEntry entry = result.next();
            ReadResultEntryType type = entry.getType();
            assertEquals(ReadResultEntryType.Cache, type);

            // Each ReadResultEntryContents may be of an arbitrary length - we should make no assumptions.
            ReadResultEntryContents contents = entry.getContent().get();
            byte next;
            while ((next = (byte) contents.getData().read()) != -1) {
                byte expected = data[index % data.length];
                assertEquals(expected, next);
                index++;
            }
        }
        assertEquals(entries * data.length, index);
    }

    @Test
    public void testReceivingReadCall() throws Exception {
        String segmentName = "testReceivingReadCall";
        int entries = 10;
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        UUID clientId = UUID.randomUUID();

        StreamSegmentStore segmentStore = serviceBuilder.createStreamSegmentService();

        fillStoreForSegment(segmentName, clientId, data, entries, segmentStore);

        EmbeddedChannel channel = AppendTest.createChannel(segmentStore);

        SegmentRead result = (SegmentRead) AppendTest.sendRequest(channel, new ReadSegment(segmentName, 0, 10000));

        assertEquals(result.getSegment(), segmentName);
        assertEquals(result.getOffset(), 0);
        assertTrue(result.isAtTail());
        assertFalse(result.isEndOfSegment());

        ByteBuffer expected = ByteBuffer.allocate(entries * data.length);
        for (int i = 0; i < entries; i++) {
            expected.put(data);
        }
        expected.rewind();
        assertEquals(expected, result.getData());
    }

    @Test
    public void readThroughSegmentClient() throws SegmentSealedException, EndOfSegmentException {
        String endpoint = "localhost";
        String scope = "scope";
        String stream = "stream";
        int port = TestUtils.randomPort();
        String testString = "Hello world\n";
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store);
        server.startListening();
        ConnectionFactory clientCF = new ConnectionFactoryImpl(false);
        Controller controller = new MockController(endpoint, port, clientCF);
        controller.createStream(new StreamConfigurationImpl(scope, stream, null));

        SegmentOutputStreamFactoryImpl segmentproducerClient = new SegmentOutputStreamFactoryImpl(controller, clientCF);

        SegmentInputStreamFactoryImpl segmentConsumerClient = new SegmentInputStreamFactoryImpl(controller, clientCF);

        Segment segment = FutureHelpers.getAndHandleExceptions(controller.getCurrentSegments(scope, stream), RuntimeException::new)
                                       .getSegments().iterator().next();

        @Cleanup("close")
        SegmentOutputStream out = segmentproducerClient.createOutputStreamForSegment(segment, null);
        out.write(ByteBuffer.wrap(testString.getBytes()), new CompletableFuture<>());
        out.flush();

        @Cleanup("close")
        SegmentInputStream in = segmentConsumerClient.createInputStreamForSegment(segment, new SegmentInputConfiguration());
        ByteBuffer result = in.read();
        assertEquals(ByteBuffer.wrap(testString.getBytes()), result);
    }

    @Test
    public void readThroughStreamClient() {
        String endpoint = "localhost";
        String streamName = "abc";
        String readerName = "reader";
        String readerGroup = "group";
        int port = TestUtils.randomPort();
        String testString = "Hello world\n";
        String scope = "Scope1";
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store);
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(scope, endpoint, port);
        MockClientFactory clientFactory = streamManager.getClientFactory();
        ReaderGroupConfig groupConfig = ReaderGroupConfig.builder().startingPosition(Sequence.MIN_VALUE).build();
        streamManager.createStream(streamName, null);
        streamManager.createReaderGroup(readerGroup, groupConfig, Collections.singletonList(streamName));
        JavaSerializer<String> serializer = new JavaSerializer<>();
        EventStreamWriter<String> producer = clientFactory.createEventWriter(streamName, serializer, new EventWriterConfig(null));

        producer.writeEvent("RoutingKey", testString);
        producer.flush();

        @Cleanup
        EventStreamReader<String> reader = clientFactory
                .createReader(readerName, readerGroup, serializer, new ReaderConfig());
        String read = reader.readNextEvent(5000).getEvent();
        assertEquals(testString, read);
    }

    @Test
    public void testEventPointer() {
        String endpoint = "localhost";
        String streamName = "abc";
        String readerName = "reader";
        String readerGroup = "group";
        int port = TestUtils.randomPort();
        String testString = "Hello world\n";
        String scope = "Scope1";
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store);
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(scope, endpoint, port);
        MockClientFactory clientFactory = streamManager.getClientFactory();
        ReaderGroupConfig groupConfig = ReaderGroupConfig.builder().startingPosition(Sequence.MIN_VALUE).build();
        streamManager.createStream(streamName, null);
        streamManager.createReaderGroup(readerGroup, groupConfig, Collections.singletonList(streamName));
        JavaSerializer<String> serializer = new JavaSerializer<>();
        EventStreamWriter<String> producer = clientFactory.createEventWriter(streamName, serializer, new EventWriterConfig(null));

        producer.writeEvent("RoutingKey", testString);
        producer.flush();

        @Cleanup
        EventStreamReader<String> reader = clientFactory
                .createReader(readerName, readerGroup, serializer, new ReaderConfig());
        try {
            EventPointer pointer = reader.readNextEvent(5000).getEventPointer();
            String read = reader.read(pointer);
            assertEquals(testString, read);
        } catch (NoSuchEventException e) {
            fail("Failed to read event using event pointer");
        }

    }

    private void fillStoreForSegment(String segmentName, UUID clientId, byte[] data, int numEntries,
                                     StreamSegmentStore segmentStore) {
        try {
            segmentStore.createStreamSegment(segmentName, Duration.ZERO).get();
            for (int eventNumber = 1; eventNumber <= numEntries; eventNumber++) {
                AppendContext appendContext = new AppendContext(clientId, eventNumber);
                segmentStore.append(segmentName, data, appendContext, Duration.ZERO).get();
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}

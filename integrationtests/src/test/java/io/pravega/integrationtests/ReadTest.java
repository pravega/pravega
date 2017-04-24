/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.integrationtests;

import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.common.netty.WireCommands.ReadSegment;
import io.pravega.common.netty.WireCommands.SegmentRead;
import io.pravega.service.contracts.ReadResult;
import io.pravega.service.contracts.ReadResultEntry;
import io.pravega.service.contracts.ReadResultEntryContents;
import io.pravega.service.contracts.ReadResultEntryType;
import io.pravega.service.contracts.StreamSegmentStore;
import io.pravega.service.server.host.handler.PravegaConnectionListener;
import io.pravega.service.server.store.ServiceBuilder;
import io.pravega.service.server.store.ServiceBuilderConfig;
import io.pravega.stream.EventPointer;
import io.pravega.stream.EventStreamReader;
import io.pravega.stream.EventStreamWriter;
import io.pravega.stream.EventWriterConfig;
import io.pravega.stream.ReaderConfig;
import io.pravega.stream.ReaderGroupConfig;
import io.pravega.stream.ReinitializationRequiredException;
import io.pravega.stream.Segment;
import io.pravega.stream.Sequence;
import io.pravega.stream.StreamConfiguration;
import io.pravega.stream.impl.Controller;
import io.pravega.stream.impl.JavaSerializer;
import io.pravega.stream.impl.PendingEvent;
import io.pravega.stream.impl.netty.ConnectionFactory;
import io.pravega.stream.impl.netty.ConnectionFactoryImpl;
import io.pravega.stream.impl.segment.EndOfSegmentException;
import io.pravega.stream.impl.segment.NoSuchEventException;
import io.pravega.stream.impl.segment.SegmentInputStream;
import io.pravega.stream.impl.segment.SegmentInputStreamFactoryImpl;
import io.pravega.stream.impl.segment.SegmentOutputStream;
import io.pravega.stream.impl.segment.SegmentOutputStreamFactoryImpl;
import io.pravega.stream.impl.segment.SegmentSealedException;
import io.pravega.stream.mock.MockClientFactory;
import io.pravega.stream.mock.MockController;
import io.pravega.stream.mock.MockStreamManager;
import io.pravega.testcommon.TestUtils;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import lombok.Cleanup;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ReadTest {

    private Level originalLevel;
    private ServiceBuilder serviceBuilder;

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
        int port = TestUtils.getAvailableListenPort();
        String testString = "Hello world\n";
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store);
        server.startListening();
        ConnectionFactory clientCF = new ConnectionFactoryImpl(false);
        Controller controller = new MockController(endpoint, port, clientCF);
        controller.createScope(scope);
        controller.createStream(StreamConfiguration.builder().scope(scope).streamName(stream).build());

        SegmentOutputStreamFactoryImpl segmentproducerClient = new SegmentOutputStreamFactoryImpl(controller, clientCF);

        SegmentInputStreamFactoryImpl segmentConsumerClient = new SegmentInputStreamFactoryImpl(controller, clientCF);

        Segment segment = FutureHelpers.getAndHandleExceptions(controller.getCurrentSegments(scope, stream), RuntimeException::new)
                                       .getSegments().iterator().next();

        @Cleanup("close")
        SegmentOutputStream out = segmentproducerClient.createOutputStreamForSegment(segment);
        out.write(new PendingEvent(null, ByteBuffer.wrap(testString.getBytes()), new CompletableFuture<>()));
        out.flush();

        @Cleanup("close")
        SegmentInputStream in = segmentConsumerClient.createInputStreamForSegment(segment);
        ByteBuffer result = in.read();
        assertEquals(ByteBuffer.wrap(testString.getBytes()), result);
    }

    @Test
    public void readThroughStreamClient() throws ReinitializationRequiredException {
        String endpoint = "localhost";
        String streamName = "abc";
        String readerName = "reader";
        String readerGroup = "group";
        int port = TestUtils.getAvailableListenPort();
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
        streamManager.createScope(scope);
        streamManager.createStream(scope, streamName, null);
        streamManager.createReaderGroup(readerGroup, groupConfig, Collections.singleton(streamName));
        JavaSerializer<String> serializer = new JavaSerializer<>();
        EventStreamWriter<String> producer = clientFactory.createEventWriter(streamName, serializer, EventWriterConfig.builder().build());

        producer.writeEvent(testString);
        producer.flush();

        @Cleanup
        EventStreamReader<String> reader = clientFactory
                .createReader(readerName, readerGroup, serializer, ReaderConfig.builder().build());
        String read = reader.readNextEvent(5000).getEvent();
        assertEquals(testString, read);
    }

    @Test(timeout = 10000)
    public void testEventPointer() throws ReinitializationRequiredException {
        String endpoint = "localhost";
        String streamName = "abc";
        String readerName = "reader";
        String readerGroup = "group";
        int port = TestUtils.getAvailableListenPort();
        String testString = "Hello world ";
        String scope = "Scope1";
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store);
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(scope, endpoint, port);
        MockClientFactory clientFactory = streamManager.getClientFactory();
        ReaderGroupConfig groupConfig = ReaderGroupConfig.builder().startingPosition(Sequence.MIN_VALUE).build();
        streamManager.createScope(scope);
        streamManager.createStream(scope, streamName, null);
        streamManager.createReaderGroup(readerGroup, groupConfig, Collections.singleton(streamName));
        JavaSerializer<String> serializer = new JavaSerializer<>();
        EventStreamWriter<String> producer = clientFactory.createEventWriter(streamName, serializer, EventWriterConfig.builder().build());

        for (int i = 0; i < 100; i++) {
            producer.writeEvent(testString + i);
        }
        producer.flush();

        @Cleanup
        EventStreamReader<String> reader = clientFactory
                .createReader(readerName, readerGroup, serializer, ReaderConfig.builder().build());
        try {
            EventPointer pointer;
            String read;

            for (int i = 0; i < 100; i++) {
                pointer = reader.readNextEvent(5000).getEventPointer();
                read = reader.read(pointer);
                assertEquals(testString + i, read);
            }
        } catch (NoSuchEventException e) {
            fail("Failed to read event using event pointer");
        }

    }

    private void fillStoreForSegment(String segmentName, UUID clientId, byte[] data, int numEntries,
                                     StreamSegmentStore segmentStore) {
        try {
            segmentStore.createStreamSegment(segmentName, null, Duration.ZERO).get();
            for (int eventNumber = 1; eventNumber <= numEntries; eventNumber++) {
                segmentStore.append(segmentName, data, null, Duration.ZERO).get();
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}

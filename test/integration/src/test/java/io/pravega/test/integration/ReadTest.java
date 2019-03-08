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

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import io.pravega.client.ClientConfig;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.segment.impl.ConditionalOutputStream;
import io.pravega.client.segment.impl.ConditionalOutputStreamFactoryImpl;
import io.pravega.client.segment.impl.EndOfSegmentException;
import io.pravega.client.segment.impl.EventSegmentReader;
import io.pravega.client.segment.impl.NoSuchEventException;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentInputStreamFactoryImpl;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentOutputStreamFactoryImpl;
import io.pravega.client.segment.impl.SegmentSealedException;
import io.pravega.client.segment.impl.SegmentTruncatedException;
import io.pravega.client.stream.EventPointer;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.PendingEvent;
import io.pravega.client.stream.mock.MockClientFactory;
import io.pravega.client.stream.mock.MockController;
import io.pravega.client.stream.mock.MockStreamManager;
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.contracts.ReadResultEntryContents;
import io.pravega.segmentstore.contracts.ReadResultEntryType;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.ReadSegment;
import io.pravega.shared.protocol.netty.WireCommands.SegmentRead;
import io.pravega.test.common.TestUtils;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import lombok.Cleanup;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class ReadTest {

    private static final int TIMEOUT_MILLIS = 30000;
    private Level originalLevel;
    private ServiceBuilder serviceBuilder;
    private final Consumer<Segment> segmentSealedCallback = segment -> { };
    @Rule
    public Timeout globalTimeout = Timeout.millis(TIMEOUT_MILLIS);

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
    public void testReadDirectlyFromStore() throws Exception {
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
            assertTrue(type == ReadResultEntryType.Cache || type == ReadResultEntryType.Future);

            // Each ReadResultEntryContents may be of an arbitrary length - we should make no assumptions.
            // Also put a timeout when fetching the response in case we get back a Future read and it never completes.
            ReadResultEntryContents contents = entry.getContent().get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
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
        @Cleanup
        EmbeddedChannel channel = AppendTest.createChannel(segmentStore);

        ByteBuffer actual = ByteBuffer.allocate(entries * data.length);
        while (actual.position() < actual.capacity()) {
            SegmentRead result = (SegmentRead) AppendTest.sendRequest(channel, new ReadSegment(segmentName, actual.position(), 10000, ""));

            assertEquals(segmentName, result.getSegment());
            assertEquals(result.getOffset(), actual.position());
            assertTrue(result.isAtTail());
            assertFalse(result.isEndOfSegment());
            actual.put(result.getData());
            if (actual.position() < actual.capacity()) {
                // Prevent entering a tight loop by giving the store a bit of time to process al the appends internally
                // before trying again.
                Thread.sleep(10);
            }
        }

        ByteBuffer expected = ByteBuffer.allocate(entries * data.length);
        for (int i = 0; i < entries; i++) {
            expected.put(data);
        }

        expected.rewind();
        actual.rewind();
        assertEquals(expected, actual);
    }

    @Test
    public void readThroughSegmentClient() throws SegmentSealedException, EndOfSegmentException, SegmentTruncatedException {
        String endpoint = "localhost";
        String scope = "scope";
        String stream = "stream";
        int port = TestUtils.getAvailableListenPort();
        String testString = "Hello world\n";
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, mock(TableStore.class));
        server.startListening();
        ConnectionFactory clientCF = new ConnectionFactoryImpl(ClientConfig.builder().build());
        Controller controller = new MockController(endpoint, port, clientCF);
        controller.createScope(scope);
        controller.createStream(scope, stream, StreamConfiguration.builder().build());

        SegmentOutputStreamFactoryImpl segmentproducerClient = new SegmentOutputStreamFactoryImpl(controller, clientCF);

        SegmentInputStreamFactoryImpl segmentConsumerClient = new SegmentInputStreamFactoryImpl(controller, clientCF);

        Segment segment = Futures.getAndHandleExceptions(controller.getCurrentSegments(scope, stream), RuntimeException::new)
                                 .getSegments().iterator().next();

        @Cleanup("close")
        SegmentOutputStream out = segmentproducerClient.createOutputStreamForSegment(segment, segmentSealedCallback, EventWriterConfig.builder().build(), "");
        out.write(PendingEvent.withHeader(null, ByteBuffer.wrap(testString.getBytes()), new CompletableFuture<>()));
        out.flush();

        @Cleanup("close")
        EventSegmentReader in = segmentConsumerClient.createEventReaderForSegment(segment);
        ByteBuffer result = in.read();
        assertEquals(ByteBuffer.wrap(testString.getBytes()), result);

        // Test large write followed by read
        out.write(PendingEvent.withHeader(null, ByteBuffer.wrap(new byte[15]), new CompletableFuture<>()));
        out.write(PendingEvent.withHeader(null, ByteBuffer.wrap(new byte[15]), new CompletableFuture<>()));
        out.write(PendingEvent.withHeader(null, ByteBuffer.wrap(new byte[150000]), new CompletableFuture<>()));
        assertEquals(in.read().capacity(), 15);
        assertEquals(in.read().capacity(), 15);
        assertEquals(in.read().capacity(), 150000);
    }
    
    @Test(timeout = 10000)
    public void readConditionalData() throws SegmentSealedException, EndOfSegmentException, SegmentTruncatedException {
        String endpoint = "localhost";
        String scope = "scope";
        String stream = "stream";
        int port = TestUtils.getAvailableListenPort();
        byte[] testString = "Hello world\n".getBytes();
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, mock(TableStore.class));
        server.startListening();
        ConnectionFactory clientCF = new ConnectionFactoryImpl(ClientConfig.builder().build());
        Controller controller = new MockController(endpoint, port, clientCF);
        controller.createScope(scope);
        controller.createStream(scope, stream, StreamConfiguration.builder().build());
        
        ConditionalOutputStreamFactoryImpl segmentproducerClient = new ConditionalOutputStreamFactoryImpl(controller, clientCF);

        SegmentInputStreamFactoryImpl segmentConsumerClient = new SegmentInputStreamFactoryImpl(controller, clientCF);

        Segment segment = Futures.getAndHandleExceptions(controller.getCurrentSegments(scope, stream), RuntimeException::new)
                                 .getSegments().iterator().next();

        @Cleanup("close")
        ConditionalOutputStream out = segmentproducerClient.createConditionalOutputStream(segment, "", EventWriterConfig.builder().build());
        assertTrue(out.write(ByteBuffer.wrap(testString), 0));

        @Cleanup("close")
        EventSegmentReader in = segmentConsumerClient.createEventReaderForSegment(segment);
        ByteBuffer result = in.read();
        assertEquals(ByteBuffer.wrap(testString), result);
        assertNull(in.read(100));
        assertFalse(out.write(ByteBuffer.wrap(testString), 0));
        assertTrue(out.write(ByteBuffer.wrap(testString), testString.length + WireCommands.TYPE_PLUS_LENGTH_SIZE));
        result = in.read();
        assertEquals(ByteBuffer.wrap(testString), result);
        assertNull(in.read(100));
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
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, mock(TableStore.class));
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(scope, endpoint, port);
        MockClientFactory clientFactory = streamManager.getClientFactory();
        ReaderGroupConfig groupConfig = ReaderGroupConfig.builder().stream(Stream.of(scope, streamName)).build();
        streamManager.createScope(scope);
        streamManager.createStream(scope, streamName, null);
        streamManager.createReaderGroup(readerGroup, groupConfig);
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
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, mock(TableStore.class));
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(scope, endpoint, port);
        MockClientFactory clientFactory = streamManager.getClientFactory();
        ReaderGroupConfig groupConfig = ReaderGroupConfig.builder().disableAutomaticCheckpoints().stream(Stream.of(scope, streamName)).build();
        streamManager.createScope(scope);
        streamManager.createStream(scope, streamName, null);
        streamManager.createReaderGroup(readerGroup, groupConfig);
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
                read = reader.fetchEvent(pointer);
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

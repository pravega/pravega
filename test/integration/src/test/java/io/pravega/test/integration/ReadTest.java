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
import io.pravega.client.ClientConfig;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.security.auth.DelegationTokenProviderFactory;
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
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.PendingEvent;
import io.pravega.client.stream.impl.PositionImpl;
import io.pravega.client.stream.mock.MockClientFactory;
import io.pravega.client.stream.mock.MockController;
import io.pravega.client.stream.mock.MockStreamManager;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.contracts.ReadResultEntryType;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.IndexAppendProcessor;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.protocol.netty.ByteBufWrapper;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.ReadSegment;
import io.pravega.shared.protocol.netty.WireCommands.SegmentRead;
import io.pravega.test.common.LeakDetectorTestSuite;
import io.pravega.test.common.NoOpScheduledExecutor;
import io.pravega.test.common.TestUtils;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Slf4j
public class ReadTest extends LeakDetectorTestSuite {

    private static final int TIMEOUT_MILLIS = 60000;
    private static final ServiceBuilder SERVICE_BUILDER = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
    private final Consumer<Segment> segmentSealedCallback = segment -> { };
    @Rule
    public Timeout globalTimeout = Timeout.millis(TIMEOUT_MILLIS);

    @BeforeClass
    public static void setup() throws Exception {
        SERVICE_BUILDER.initialize();
    }

    @AfterClass
    public static void teardown() {
        SERVICE_BUILDER.close();
    }

    @Test(timeout = 10000)
    public void testReadDirectlyFromStore() throws Exception {
        String segmentName = "testReadFromStore";
        final int entries = 10;
        final byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        UUID clientId = UUID.randomUUID();

        StreamSegmentStore segmentStore = SERVICE_BUILDER.createStreamSegmentService();

        fillStoreForSegment(segmentName, data, entries, segmentStore);

        @Cleanup
        ReadResult result = segmentStore.read(segmentName, 0, entries * data.length, Duration.ZERO).get();
        int index = 0;
        while (result.hasNext()) {
            ReadResultEntry entry = result.next();
            ReadResultEntryType type = entry.getType();
            assertTrue(type == ReadResultEntryType.Cache || type == ReadResultEntryType.Future);

            // Each ReadResultEntryContents may be of an arbitrary length - we should make no assumptions.
            // Also put a timeout when fetching the response in case we get back a Future read and it never completes.
            BufferView contents = entry.getContent().get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            @Cleanup
            InputStream contentStream = contents.getReader();
            byte next;
            while ((next = (byte) contentStream.read()) != -1) {
                byte expected = data[index % data.length];
                assertEquals(expected, next);
                index++;
            }
        }
        assertEquals(entries * data.length, index);
    }

    @Test(timeout = 10000)
    public void testReceivingReadCall() throws Exception {
        String segmentName = "testReceivingReadCall";
        int entries = 10;
        byte[] data = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

        StreamSegmentStore segmentStore = SERVICE_BUILDER.createStreamSegmentService();
        // fill segment store with 10 entries; the total data size is 100 bytes.
        fillStoreForSegment(segmentName, data, entries, segmentStore);
        @Cleanup
        EmbeddedChannel channel = AppendTest.createChannel(segmentStore);

        ByteBuf actual = Unpooled.buffer(entries * data.length);
        while (actual.writerIndex() < actual.capacity()) {
            SegmentRead result = (SegmentRead) AppendTest.sendRequest(channel, new ReadSegment(segmentName, actual.writerIndex(), 10000, "", 1L));
            assertEquals(segmentName, result.getSegment());
            assertEquals(result.getOffset(), actual.writerIndex());
            assertFalse(result.isEndOfSegment());
            actual.writeBytes(result.getData());
            // release the ByteBuf and ensure it is deallocated.
            assertTrue(result.getData().release());
            if (actual.writerIndex() < actual.capacity()) {
                // Prevent entering a tight loop by giving the store a bit of time to process al the appends internally
                // before trying again.
                Thread.sleep(10);
            } else {
                // Verify the last read result has the the atTail flag set to true.
                assertTrue(result.isAtTail());
                // mark the channel as finished
                assertFalse(channel.finish());
            }
        }
        ByteBuf expected = Unpooled.buffer(entries * data.length);
        for (int i = 0; i < entries; i++) {
            expected.writeBytes(data);
        }

        expected.writerIndex(expected.capacity()).resetReaderIndex();
        actual.writerIndex(actual.capacity()).resetReaderIndex();
        assertEquals(expected, actual);
        // Release the ByteBuf and ensure it is deallocated.
        assertTrue(actual.release());
        assertTrue(expected.release());
    }

    @Test(timeout = 10000)
    public void readThroughSegmentClient() throws SegmentSealedException, EndOfSegmentException, SegmentTruncatedException {
        String endpoint = "localhost";
        String scope = "scope";
        String stream = "readThroughSegmentClient";
        int port = TestUtils.getAvailableListenPort();
        String testString = "Hello world\n";
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

        SegmentOutputStreamFactoryImpl segmentproducerClient = new SegmentOutputStreamFactoryImpl(controller, connectionPool);

        SegmentInputStreamFactoryImpl segmentConsumerClient = new SegmentInputStreamFactoryImpl(controller, connectionPool);

        Segment segment = Futures.getAndHandleExceptions(controller.getCurrentSegments(scope, stream), RuntimeException::new)
                                 .getSegments().iterator().next();

        @Cleanup
        SegmentOutputStream out = segmentproducerClient.createOutputStreamForSegment(segment, segmentSealedCallback, EventWriterConfig.builder().build(),
                DelegationTokenProviderFactory.createWithEmptyToken());
        out.write(PendingEvent.withHeader(null, ByteBuffer.wrap(testString.getBytes()), new CompletableFuture<>()));
        out.flush();

        @Cleanup
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
        String stream = "readConditionalData";
        int port = TestUtils.getAvailableListenPort();
        byte[] testString = "Hello world\n".getBytes();
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

        ConditionalOutputStreamFactoryImpl segmentproducerClient = new ConditionalOutputStreamFactoryImpl(controller, connectionPool);

        SegmentInputStreamFactoryImpl segmentConsumerClient = new SegmentInputStreamFactoryImpl(controller, connectionPool);

        Segment segment = Futures.getAndHandleExceptions(controller.getCurrentSegments(scope, stream), RuntimeException::new)
                                 .getSegments().iterator().next();

        @Cleanup
        ConditionalOutputStream out = segmentproducerClient.createConditionalOutputStream(segment,
                DelegationTokenProviderFactory.createWithEmptyToken(), EventWriterConfig.builder().build());
        assertTrue(out.write(ByteBuffer.wrap(testString), 0));

        @Cleanup
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

    @Test(timeout = 10000)
    public void readThroughStreamClient() throws ReinitializationRequiredException {
        String endpoint = "localhost";
        String streamName = "readThroughStreamClient";
        String readerName = "reader";
        String readerGroup = "readThroughStreamClient-group";
        int port = TestUtils.getAvailableListenPort();
        String testString = "Hello world\n";
        String scope = "Scope1";
        StreamSegmentStore store = SERVICE_BUILDER.createStreamSegmentService();
        TableStore tableStore = SERVICE_BUILDER.createTableStoreService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, tableStore, NoOpScheduledExecutor.get(),
                new IndexAppendProcessor(SERVICE_BUILDER.getLowPriorityExecutor(), store));
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(scope, endpoint, port);
        @Cleanup
        MockClientFactory clientFactory = streamManager.getClientFactory();
        ReaderGroupConfig groupConfig = ReaderGroupConfig.builder().stream(Stream.of(scope, streamName)).disableAutomaticCheckpoints().build();
        streamManager.createScope(scope);
        streamManager.createStream(scope, streamName, null);
        streamManager.createReaderGroup(readerGroup, groupConfig);
        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
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
    public void testEventPointer() throws ReinitializationRequiredException, NoSuchEventException {
        String endpoint = "localhost";
        String streamName = "testEventPointer";
        String readerName = "reader";
        String readerGroup = "testEventPointer-group";
        int port = TestUtils.getAvailableListenPort();
        String testString = "Hello world ";
        String scope = "Scope1";
        StreamSegmentStore store = SERVICE_BUILDER.createStreamSegmentService();
        TableStore tableStore = SERVICE_BUILDER.createTableStoreService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, tableStore, NoOpScheduledExecutor.get(),
                new IndexAppendProcessor(SERVICE_BUILDER.getLowPriorityExecutor(), store));
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(scope, endpoint, port);
        @Cleanup
        MockClientFactory clientFactory = streamManager.getClientFactory();
        ReaderGroupConfig groupConfig = ReaderGroupConfig.builder().disableAutomaticCheckpoints().stream(Stream.of(scope, streamName)).build();
        streamManager.createScope(scope);
        streamManager.createStream(scope, streamName, null);
        streamManager.createReaderGroup(readerGroup, groupConfig);
        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<String> producer = clientFactory.createEventWriter(streamName, serializer, EventWriterConfig.builder().build());

        for (int i = 0; i < 100; i++) {
            producer.writeEvent(testString + i);
        }
        producer.flush();

        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader(readerName, readerGroup, serializer, ReaderConfig.builder().build());
        for (int i = 0; i < 100; i++) {
            EventPointer pointer = reader.readNextEvent(5000).getEventPointer();
            String read = reader.fetchEvent(pointer);
            assertEquals(testString + i, read);
        }
    }

    /**
     * This test performs concurrent writes, reads and position checks on a Stream. Readers are checkers exercising the
     * lazy construction of PositionImpl objects while the internal segmentOffsetUpdates list in EventStreamReaderImpl is
     * being updated due to new read events. This test generates enough events to make segmentOffsetUpdates list in
     * EventStreamReaderImpl to be filled and cleaned at least once. This test verifies the thread safety of the new
     * optimization in EventStreamReaderImpl to prevent generating segmentOffset maps on every event read, as well as
     * to check for the correctness of the segment offsets returned by PositionImpl.
     */
    @Test(timeout = 60000)
    public void testEventPositions() {
        String endpoint = "localhost";
        String streamName = "eventPositions";
        String readerGroup = "groupPositions";
        String scope = "scopePositions";
        // Generate enough events to make the internal segment offset update buffer in EventStreamReaderImpl to be
        // emptied and filled again.
        int eventsToWrite = 2000;
        BlockingQueue<Entry<Integer, PositionImpl>> readEventsPositions = new ArrayBlockingQueue<>(eventsToWrite);
        @Cleanup("shutdown")
        ScheduledExecutorService readersWritersAndCheckers = ExecutorServiceHelpers.newScheduledThreadPool(4, "readers-writers-checkers");
        AtomicInteger finishedProcesses = new AtomicInteger(0);
        int port = TestUtils.getAvailableListenPort();
        StreamSegmentStore store = SERVICE_BUILDER.createStreamSegmentService();
        TableStore tableStore = SERVICE_BUILDER.createTableStoreService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, tableStore, NoOpScheduledExecutor.get(),
                new IndexAppendProcessor(SERVICE_BUILDER.getLowPriorityExecutor(), store));
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(scope, endpoint, port);
        @Cleanup
        MockClientFactory clientFactory = streamManager.getClientFactory();
        ReaderGroupConfig groupConfig = ReaderGroupConfig.builder().groupRefreshTimeMillis(1000).stream(Stream.of(scope, streamName)).build();
        streamManager.createScope(scope);
        // Create a Stream with 2 segments.
        streamManager.createStream(scope, streamName, StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(2)).build());
        streamManager.createReaderGroup(readerGroup, groupConfig);
        JavaSerializer<String> serializer = new JavaSerializer<>();

        @Cleanup
        EventStreamWriter<String> producer = clientFactory.createEventWriter(streamName, serializer, EventWriterConfig.builder().build());
        @Cleanup
        EventStreamReader<String> reader1 = clientFactory.createReader("reader1", readerGroup, serializer, ReaderConfig.builder().build());
        @Cleanup
        EventStreamReader<String> reader2 = clientFactory.createReader("reader2", readerGroup, serializer, ReaderConfig.builder().build());

        // Leave some time for readers to re-balance the segments and acquire one each.
        Exceptions.handleInterrupted(() -> Thread.sleep(2000));

        // Start writers and readers in parallel.
        CompletableFuture reader1Future =  CompletableFuture.runAsync(() -> {
            readAndQueueEvents(reader1, eventsToWrite, readEventsPositions);
            finishedProcesses.incrementAndGet();
        }, readersWritersAndCheckers);

        CompletableFuture reader2Future = CompletableFuture.runAsync(() -> {
            readAndQueueEvents(reader2, eventsToWrite, readEventsPositions);
            finishedProcesses.incrementAndGet();
        }, readersWritersAndCheckers);

        CompletableFuture writerFuture = CompletableFuture.runAsync(() -> {
            for (int i = 0; i < eventsToWrite; i++) {
                producer.writeEvent("segment1", "a");
                producer.writeEvent("segment2", "b");
                Exceptions.handleInterrupted(() -> Thread.sleep(1));
            }
            finishedProcesses.incrementAndGet();
        }, readersWritersAndCheckers);

        // This process access the positions read by the reader threads, which means that this thread is concurrently
        // accessing the shared segmentOffsetUpdates list, whereas readers are appending data to it.
        CompletableFuture checkOffsets = CompletableFuture.runAsync(() -> {
            int sizeOfEvent = 16; // 1-char string is assumed to be the payload of events
            while (finishedProcesses.get() < 2) {
                Entry<Integer, PositionImpl> element;
                try {
                    element = readEventsPositions.poll(1, TimeUnit.MINUTES);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                int numberOfSegments = element.getValue().getOwnedSegmentsWithOffsets().size();
                assertEquals("Reader owning too many segments.", 1, numberOfSegments);
                // The segment position should increase by sizeOfEvent every event.
                long segmentPositionOffset = element.getValue().getOwnedSegmentsWithOffsets().values().iterator().next();
                assertEquals("Wrong event position", sizeOfEvent * element.getKey(), segmentPositionOffset);
            }
            finishedProcesses.incrementAndGet();
        }, readersWritersAndCheckers);

        // Wait for all futures to complete.
        CompletableFuture.allOf(writerFuture, reader1Future, reader2Future, checkOffsets).join();
        // Any failure reading, writing or checking positions will make this assertion to fail.
        assertEquals(finishedProcesses.get(), 4);
        ExecutorServiceHelpers.shutdown(readersWritersAndCheckers);
    }

    /**
     * Reads events and puts them in a queue for later checking (potentially by another thread).
     */
    private void readAndQueueEvents(EventStreamReader<String> reader, int eventsToWrite, Queue<Entry<Integer, PositionImpl>> readEventsPositions) {
        int eventCount = 1;
        for (int i = 0; i < eventsToWrite; i++) {
            final EventRead<String> event = reader.readNextEvent(1000);
            if (event.getEvent() != null && !event.isCheckpoint()) {
                // The reader should own only 1 segment.
                readEventsPositions.add(new AbstractMap.SimpleEntry<>(eventCount, (PositionImpl) event.getPosition()));
                eventCount++;
            }
        }
    }

    private void fillStoreForSegment(String segmentName, byte[] data, int numEntries,
                                     StreamSegmentStore segmentStore) {
        try {
            segmentStore.createStreamSegment(segmentName, SegmentType.STREAM_SEGMENT, null, Duration.ZERO).get();
            for (int eventNumber = 1; eventNumber <= numEntries; eventNumber++) {
                segmentStore.append(segmentName, new ByteBufWrapper(Unpooled.wrappedBuffer(data)), null, Duration.ZERO).get();
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}

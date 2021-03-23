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

import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.MaxNumberOfCheckpointsExceededException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.mock.MockClientFactory;
import io.pravega.client.stream.mock.MockStreamManager;
import io.pravega.test.common.InlineExecutor;
import io.pravega.test.common.TestUtils;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import lombok.Cleanup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class CheckpointTest {

    private static final long CLOCK_ADVANCE_INTERVAL = 60 * 1000000000L;
    private ServiceBuilder serviceBuilder;

    @Before
    public void setup() throws Exception {
        InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE);
        this.serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        this.serviceBuilder.initialize();
    }

    @After
    public void teardown() {
        this.serviceBuilder.close();
    }

    @Test(timeout = 20000)
    public void testCheckpointAndRestore() throws ReinitializationRequiredException, InterruptedException,
                                           ExecutionException, TimeoutException {
        String endpoint = "localhost";
        String streamName = "abc";
        String readerName = "reader";
        String readerGroupName = "group";
        int port = TestUtils.getAvailableListenPort();
        String testString = "Hello world\n";
        String scope = "Scope1";
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, tableStore, this.serviceBuilder.getLowPriorityExecutor());
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(scope, endpoint, port);
        @Cleanup
        MockClientFactory clientFactory = streamManager.getClientFactory();
        ReaderGroupConfig groupConfig = ReaderGroupConfig.builder()
                                                         .disableAutomaticCheckpoints()
                                                         .stream(Stream.of(scope, streamName)).build();
        streamManager.createScope(scope);
        streamManager.createStream(scope, streamName, StreamConfiguration.builder()
                                                                         .scalingPolicy(ScalingPolicy.fixed(1))
                                                                         .build());
        streamManager.createReaderGroup(readerGroupName, groupConfig);
        @Cleanup
        ReaderGroup readerGroup = streamManager.getReaderGroup(readerGroupName);
        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<String> producer = clientFactory.createEventWriter(streamName, serializer,
                                                                             EventWriterConfig.builder().build());
        producer.writeEvent(testString);
        producer.writeEvent(testString);
        producer.writeEvent(testString);
        producer.flush();

        AtomicLong clock = new AtomicLong();
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader(readerName, readerGroupName, serializer,
                                                                      ReaderConfig.builder().build(), clock::get,
                                                                      clock::get);
        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        EventRead<String> read = reader.readNextEvent(60000);
        assertEquals(testString, read.getEvent());

        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        read = reader.readNextEvent(60000);
        assertEquals(testString, read.getEvent());

        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        @Cleanup("shutdown")
        final InlineExecutor backgroundExecutor = new InlineExecutor();
        CompletableFuture<Checkpoint> checkpoint = readerGroup.initiateCheckpoint("Checkpoint", backgroundExecutor);
        assertFalse(checkpoint.isDone());
        read = reader.readNextEvent(60000);
        assertTrue(read.isCheckpoint());
        assertEquals("Checkpoint", read.getCheckpointName());
        assertNull(read.getEvent());

        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        read = reader.readNextEvent(60000);
        assertEquals(testString, read.getEvent());
        Checkpoint cpResult = checkpoint.get(5, TimeUnit.SECONDS);
        assertTrue(checkpoint.isDone());
        assertEquals("Checkpoint", cpResult.getName());
        read = reader.readNextEvent(100);
        assertNull(read.getEvent());
        assertFalse(read.isCheckpoint());

        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        readerGroup.resetReaderGroup(ReaderGroupConfig.builder().startFromCheckpoint(cpResult).disableAutomaticCheckpoints().build());
        try {
            reader.readNextEvent(60000);
            fail();
        } catch (ReinitializationRequiredException e) {
            //Expected
        }
        reader.close();
        reader = clientFactory.createReader(readerName, readerGroupName, serializer, ReaderConfig.builder().build());

        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        read = reader.readNextEvent(60000);
        assertEquals(testString, read.getEvent());

        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        read = reader.readNextEvent(100);
        assertNull(read.getEvent());
        assertFalse(read.isCheckpoint());
    }

    @Test(timeout = 20000)
    public void testMoreReadersThanSegments() throws ReinitializationRequiredException, InterruptedException,
                                              ExecutionException, TimeoutException {
        String endpoint = "localhost";
        String streamName = "abc";
        String readerGroupName = "group";
        int port = TestUtils.getAvailableListenPort();
        String testString = "Hello world\n";
        String scope = "Scope1";
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, tableStore, this.serviceBuilder.getLowPriorityExecutor());
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(scope, endpoint, port);
        @Cleanup
        MockClientFactory clientFactory = streamManager.getClientFactory();
        ReaderGroupConfig groupConfig = ReaderGroupConfig.builder().stream(Stream.of(scope, streamName)).build();
        streamManager.createScope(scope);
        streamManager.createStream(scope, streamName, StreamConfiguration.builder()
                                                                         .scalingPolicy(ScalingPolicy.fixed(1))
                                                                         .build());
        streamManager.createReaderGroup(readerGroupName, groupConfig);
        @Cleanup
        ReaderGroup readerGroup = streamManager.getReaderGroup(readerGroupName);
        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<String> producer = clientFactory.createEventWriter(streamName, serializer,
                                                                             EventWriterConfig.builder().build());
        producer.writeEvent(testString);
        producer.writeEvent(testString);
        producer.writeEvent(testString);
        producer.flush();

        AtomicLong clock = new AtomicLong();
        @Cleanup
        EventStreamReader<String> reader1 = clientFactory.createReader("reader1", readerGroupName, serializer,
                                                                       ReaderConfig.builder().build(), clock::get,
                                                                       clock::get);
        @Cleanup
        EventStreamReader<String> reader2 = clientFactory.createReader("reader2", readerGroupName, serializer,
                                                                       ReaderConfig.builder().build(), clock::get,
                                                                       clock::get);
        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        @Cleanup("shutdown")
        final InlineExecutor backgroundExecutor = new InlineExecutor();
        CompletableFuture<Checkpoint> checkpoint = readerGroup.initiateCheckpoint("Checkpoint",
                backgroundExecutor);
        assertFalse(checkpoint.isDone());
        EventRead<String> read = reader1.readNextEvent(60000);
        assertTrue(read.isCheckpoint());
        assertEquals("Checkpoint", read.getCheckpointName());
        assertNull(read.getEvent());
        read = reader2.readNextEvent(60000);
        assertTrue(read.isCheckpoint());
        assertEquals("Checkpoint", read.getCheckpointName());
        assertNull(read.getEvent());
        
        read = reader1.readNextEvent(100);
        assertFalse(read.isCheckpoint());
        assertEquals(testString, read.getEvent());
        read = reader2.readNextEvent(100);
        assertFalse(read.isCheckpoint());
        assertNull(read.getEvent());
        
        Checkpoint cpResult = checkpoint.get(5, TimeUnit.SECONDS);
        assertTrue(checkpoint.isDone());
        assertEquals("Checkpoint", cpResult.getName());
    }

    @Test(timeout = 20000)
    public void testMaxPendingCheckpoint() throws ReinitializationRequiredException, InterruptedException,
            ExecutionException, TimeoutException {
        String endpoint = "localhost";
        String streamName = "abcd";
        String readerGroupName = "group1";
        int port = TestUtils.getAvailableListenPort();
        String testString = "Hello world\n";
        String scope = "Scope12";
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, tableStore, this.serviceBuilder.getLowPriorityExecutor());
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(scope, endpoint, port);
        @Cleanup
        MockClientFactory clientFactory = streamManager.getClientFactory();
        int maxOutstandingCheckpointRequest = 1;
        ReaderGroupConfig groupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(scope, streamName))
                .maxOutstandingCheckpointRequest(maxOutstandingCheckpointRequest)
                .build();
        streamManager.createScope(scope);
        streamManager.createStream(scope, streamName, StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build());
        streamManager.createReaderGroup(readerGroupName, groupConfig);

        @Cleanup
        ReaderGroup readerGroup = streamManager.getReaderGroup(readerGroupName);
        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<String> producer = clientFactory.createEventWriter(streamName, serializer,
                EventWriterConfig.builder().build());
        producer.writeEvent(testString);
        producer.writeEvent(testString);
        producer.writeEvent(testString);
        producer.flush();

        AtomicLong clock = new AtomicLong();
        @Cleanup
        EventStreamReader<String> reader1 = clientFactory.createReader("reader1", readerGroupName, serializer,
                ReaderConfig.builder().build(), clock::get,
                clock::get);
        @Cleanup
        EventStreamReader<String> reader2 = clientFactory.createReader("reader2", readerGroupName, serializer,
                ReaderConfig.builder().build(), clock::get,
                clock::get);
        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);

        @Cleanup("shutdown")
        final InlineExecutor backgroundExecutor1 = new InlineExecutor();
        @Cleanup("shutdown")
        final InlineExecutor backgroundExecutor2 = new InlineExecutor();

        CompletableFuture<Checkpoint> checkpoint1 = readerGroup.initiateCheckpoint("Checkpoint1", backgroundExecutor1);
        assertFalse(checkpoint1.isDone());

        CompletableFuture<Checkpoint> checkpoint2 = readerGroup.initiateCheckpoint("Checkpoint2", backgroundExecutor2);
        assertTrue(checkpoint2.isCompletedExceptionally());
        try {
            checkpoint2.get();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof MaxNumberOfCheckpointsExceededException);
            assertTrue(e.getCause().getMessage()
                    .equals("rejecting checkpoint request since pending checkpoint reaches max allowed limit"));
        }

        EventRead<String> read = reader1.readNextEvent(100);
        assertTrue(read.isCheckpoint());
        assertEquals("Checkpoint1", read.getCheckpointName());
        assertNull(read.getEvent());

        read = reader2.readNextEvent(100);
        assertTrue(read.isCheckpoint());
        assertEquals("Checkpoint1", read.getCheckpointName());
        assertNull(read.getEvent());

        read = reader1.readNextEvent(100);
        assertFalse(read.isCheckpoint());

        read = reader2.readNextEvent(100);
        assertFalse(read.isCheckpoint());

        Checkpoint cpResult = checkpoint1.get(5, TimeUnit.SECONDS);
        assertTrue(checkpoint1.isDone());
        assertEquals("Checkpoint1", cpResult.getName());
    }

    @Test(timeout = 20000)
    public void testGenerateStreamCuts() throws Exception {
        String endpoint = "localhost";
        String streamName = "abc";
        String readerName = "reader";
        String readerGroupName = "group";
        int port = TestUtils.getAvailableListenPort();
        String testString = "Hello world\n";
        String scope = "Scope1";
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, mock(TableStore.class), this.serviceBuilder.getLowPriorityExecutor());
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(scope, endpoint, port);
        @Cleanup
        MockClientFactory clientFactory = streamManager.getClientFactory();
        ReaderGroupConfig groupConfig = ReaderGroupConfig.builder()
                                                         .disableAutomaticCheckpoints()
                                                         .stream(Stream.of(scope, streamName)).build();
        streamManager.createScope(scope);
        streamManager.createStream(scope, streamName, StreamConfiguration.builder()
                                                                         .scalingPolicy(ScalingPolicy.fixed(1))
                                                                         .build());
        streamManager.createReaderGroup(readerGroupName, groupConfig);
        @Cleanup
        ReaderGroup readerGroup = streamManager.getReaderGroup(readerGroupName);
        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<String> producer = clientFactory.createEventWriter(streamName, serializer,
                                                                             EventWriterConfig.builder().build());
        producer.writeEvent(testString);
        producer.writeEvent(testString);
        producer.writeEvent(testString);
        producer.flush();

        AtomicLong clock = new AtomicLong();
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader(readerName, readerGroupName, serializer,
                                                                      ReaderConfig.builder().build(), clock::get,
                                                                      clock::get);
        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        EventRead<String> read = reader.readNextEvent(60000);
        assertEquals(testString, read.getEvent());

        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        read = reader.readNextEvent(60000);
        assertEquals(testString, read.getEvent());

        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        @Cleanup("shutdown")
        final InlineExecutor backgroundExecutor = new InlineExecutor();
        CompletableFuture<Map<Stream, StreamCut>> sc = readerGroup.generateStreamCuts(backgroundExecutor);
        assertFalse(sc.isDone());

        read = reader.readNextEvent(60000);
        assertEquals(testString, read.getEvent());
    }
}

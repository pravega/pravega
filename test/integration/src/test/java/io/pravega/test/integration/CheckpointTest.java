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

import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import io.pravega.client.segment.impl.Segment;
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
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.client.stream.impl.MaxNumberOfCheckpointsExceededException;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.mock.MockClientFactory;
import io.pravega.client.stream.mock.MockStreamManager;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.IndexAppendProcessor;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.InlineExecutor;
import io.pravega.test.common.TestUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import lombok.Cleanup;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;

public class CheckpointTest {

    private static final long CLOCK_ADVANCE_INTERVAL = 60 * 1000000000L;
    private static final ServiceBuilder SERVICE_BUILDER = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
    private static final String SILENT = "_SILENT_";

    @BeforeClass
    public static void setup() throws Exception {
        InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE);
        SERVICE_BUILDER.initialize();
    }

    @AfterClass
    public static void teardown() {
        SERVICE_BUILDER.close();
    }

    @Test(timeout = 20000)
    public void testCancelOutstandingCheckpoints() throws InterruptedException, ExecutionException {

        String endpoint = "localhost";
        String streamName = "testCancelOutstandingCheckpoints";
        String readerGroupName = "testCancelOutstandingCheckpoints-group1";
        int port = TestUtils.getAvailableListenPort();
        String testString = "Hello world\n";
        String scope = "testCancelOutstandingCheckpoints-Scope";
        StreamSegmentStore store = SERVICE_BUILDER.createStreamSegmentService();
        TableStore tableStore = SERVICE_BUILDER.createTableStoreService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, tableStore,
                SERVICE_BUILDER.getLowPriorityExecutor(), new IndexAppendProcessor(SERVICE_BUILDER.getLowPriorityExecutor(), store));
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(scope, endpoint, port);
        @Cleanup
        MockClientFactory clientFactory = streamManager.getClientFactory();
        int maxOutstandingCheckpointRequest = 3;
        ReaderGroupConfig groupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(scope, streamName))
                .maxOutstandingCheckpointRequest(maxOutstandingCheckpointRequest)
                .build();
        streamManager.createScope(scope);
        streamManager.createStream(scope, streamName, StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(3))
                .build());
        streamManager.createReaderGroup(readerGroupName, groupConfig);
        @Cleanup
        ReaderGroup readerGroup = streamManager.getReaderGroup(readerGroupName);
        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<String> eventWriter = clientFactory.createEventWriter(streamName, serializer,
                EventWriterConfig.builder().build());
        eventWriter.writeEvent(testString);
        eventWriter.writeEvent(testString);
        eventWriter.writeEvent(testString);
        eventWriter.flush();
        AtomicLong clock = new AtomicLong();
        @Cleanup
        EventStreamReader<String> reader1 = clientFactory.createReader("reader1", readerGroupName, serializer,
                ReaderConfig.builder().build(), clock::get,
                clock::get);
        @Cleanup
        EventStreamReader<String> reader2 = clientFactory.createReader("reader2", readerGroupName, serializer,
                ReaderConfig.builder().build(), clock::get,
                clock::get);
        @Cleanup
        EventStreamReader<String> reader3 = clientFactory.createReader("reader3", readerGroupName, serializer,
                ReaderConfig.builder().build(), clock::get,
                clock::get);
        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);

        @Cleanup("shutdown")
        final InlineExecutor backgroundExecutor1 = new InlineExecutor();
        @Cleanup("shutdown")
        final InlineExecutor backgroundExecutor2 = new InlineExecutor();
        @Cleanup("shutdown")
        final InlineExecutor backgroundExecutor3 = new InlineExecutor();
        @Cleanup("shutdown")
        final InlineExecutor backgroundExecutor4 = new InlineExecutor();
        @Cleanup("shutdown")
        final InlineExecutor backgroundExecutor5 = new InlineExecutor();

        CompletableFuture<Checkpoint> checkpoint1 = readerGroup.initiateCheckpoint("Checkpoint1", backgroundExecutor1);
        assertFalse(checkpoint1.isDone());
        CompletableFuture<Checkpoint> checkpoint2 = readerGroup.initiateCheckpoint("Checkpoint2", backgroundExecutor2);
        assertFalse(checkpoint2.isDone());
        CompletableFuture<Checkpoint> checkpoint3 = readerGroup.initiateCheckpoint("Checkpoint3", backgroundExecutor3);
        assertFalse(checkpoint3.isDone());
        CompletableFuture<Checkpoint> checkpoint4 = readerGroup.initiateCheckpoint("Checkpoint4", backgroundExecutor4);
        assertTrue(checkpoint4.isCompletedExceptionally());
        try {
            checkpoint4.get();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof MaxNumberOfCheckpointsExceededException);
            assertTrue(e.getCause().getMessage()
                    .equals("rejecting checkpoint request since pending checkpoint reaches max allowed limit"));
        }
        readerGroup.cancelOutstandingCheckpoints();
        CompletableFuture<Checkpoint> checkpoint5 = readerGroup.initiateCheckpoint("Checkpoint5", backgroundExecutor5);

        EventRead<String> read = reader1.readNextEvent(100);
        assertTrue(read.isCheckpoint());
        assertEquals("Checkpoint5", read.getCheckpointName());
        assertNull(read.getEvent());

        read = reader2.readNextEvent(100);
        assertTrue(read.isCheckpoint());
        assertEquals("Checkpoint5", read.getCheckpointName());
        assertNull(read.getEvent());

        read = reader3.readNextEvent(100);
        assertTrue(read.isCheckpoint());
        assertEquals("Checkpoint5", read.getCheckpointName());
        assertNull(read.getEvent());

        read = reader1.readNextEvent(100);
        assertFalse(read.isCheckpoint());

        read = reader2.readNextEvent(100);
        assertFalse(read.isCheckpoint());

        read = reader3.readNextEvent(100);
        assertFalse(read.isCheckpoint());

        readerGroup.resetReaderGroup(ReaderGroupConfig.builder().startFromCheckpoint(checkpoint5.get()).disableAutomaticCheckpoints().build());
        assertThrows("Checkpoint was cleared before results could be read.", ExecutionException.class, () -> checkpoint1.get(5, TimeUnit.SECONDS));
        assertThrows("Checkpoint was cleared before results could be read.", ExecutionException.class, () -> checkpoint2.get(5, TimeUnit.SECONDS));
        assertThrows("Checkpoint was cleared before results could be read.", ExecutionException.class, () -> checkpoint3.get(5, TimeUnit.SECONDS));
        assertTrue(checkpoint5.isDone());
    }

    @Test(timeout = 20000)
    public void testCheckpointAndRestore() throws ReinitializationRequiredException, InterruptedException,
            ExecutionException, TimeoutException {
        String endpoint = "localhost";
        String streamName = "testCheckpointAndRestore";
        String readerName = "reader";
        String readerGroupName = "testCheckpointAndRestore-group";
        int port = TestUtils.getAvailableListenPort();
        String testString = "Hello world\n";
        String scope = "Scope1";
        StreamSegmentStore store = SERVICE_BUILDER.createStreamSegmentService();
        TableStore tableStore = SERVICE_BUILDER.createTableStoreService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, tableStore, SERVICE_BUILDER.getLowPriorityExecutor(),
                new IndexAppendProcessor(SERVICE_BUILDER.getLowPriorityExecutor(), store));
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

    /*
    *  Initiate checkpoint with internal executor and restore back.
    */
    @Test(timeout = 20000)
    public void testCheckpointInternalExecutorAndRestore() throws ReinitializationRequiredException, InterruptedException,
            ExecutionException, TimeoutException {
        String endpoint = "localhost";
        String streamName = "testCPInternalExecutorAndRestore";
        String readerName = "reader";
        String readerGroupName = "testCPInternalExecutorAndRestore-group";
        int port = TestUtils.getAvailableListenPort();
        String testString = "Hello world\n";
        String scope = "ScopeInExecutor";
        StreamSegmentStore store = SERVICE_BUILDER.createStreamSegmentService();
        TableStore tableStore = SERVICE_BUILDER.createTableStoreService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, tableStore, SERVICE_BUILDER.getLowPriorityExecutor(),
                new IndexAppendProcessor(SERVICE_BUILDER.getLowPriorityExecutor(), store));
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
        UTF8StringSerializer serializer = new UTF8StringSerializer();
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
        CompletableFuture<Checkpoint> checkpoint = readerGroup.initiateCheckpoint("CheckpointInternalExcutor");
        assertFalse(checkpoint.isDone());
        read = reader.readNextEvent(60000);
        assertTrue(read.isCheckpoint());
        assertEquals("CheckpointInternalExcutor", read.getCheckpointName());
        assertNull(read.getEvent());

        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        read = reader.readNextEvent(60000);
        assertEquals(testString, read.getEvent());
        Checkpoint cpResult = checkpoint.get(5, TimeUnit.SECONDS);
        assertTrue(checkpoint.isDone());
        assertEquals("CheckpointInternalExcutor", cpResult.getName());
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
    public void testCheckpointAndRestoreToLastCheckpoint() throws ReinitializationRequiredException, InterruptedException,
            ExecutionException, TimeoutException {
        String endpoint = "localhost";
        String streamName = "testCheckpointAndRestoreLastCP";
        String readerName = "reader";
        String readerGroupName = "testCheckpointAndRestore-groupCP1";
        int port = TestUtils.getAvailableListenPort();
        String testString = "Hello world\n";
        String testString1 = "Hello world 1\n";
        String testString2 = "Hello world 2\n";
        String testString3 = "Hello world 3\n";
        String testString4 = "Hello world 4\n";
        String testString5 = "Hello world 5\n";
        String testString6 = "Hello world 6\n";

        String scope = "ScopeLCP";
        StreamSegmentStore store = SERVICE_BUILDER.createStreamSegmentService();
        TableStore tableStore = SERVICE_BUILDER.createTableStoreService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, tableStore, SERVICE_BUILDER.getLowPriorityExecutor(),
                new IndexAppendProcessor(SERVICE_BUILDER.getLowPriorityExecutor(), store));
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
        producer.writeEvent(testString1);
        producer.writeEvent(testString2);
        producer.writeEvent(testString3);
        producer.writeEvent(testString4);
        producer.writeEvent(testString5);
        producer.writeEvent(testString6);
        producer.flush();

        AtomicLong clock = new AtomicLong();
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader(readerName, readerGroupName, serializer,
                ReaderConfig.builder().build(), clock::get,
                clock::get);
        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        EventRead<String> read = reader.readNextEvent(60000);
        assertEquals(testString, read.getEvent());

        //Read First event
        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        read = reader.readNextEvent(60000);
        assertEquals(testString1, read.getEvent());

        //Initiate 1st checkpoint
        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        @Cleanup("shutdown")
        final InlineExecutor backgroundExecutor = new InlineExecutor();
        CompletableFuture<Checkpoint> checkpoint = readerGroup.initiateCheckpoint("Checkpoint", backgroundExecutor);
        assertFalse(checkpoint.isDone());
        read = reader.readNextEvent(60000);
        assertTrue(read.isCheckpoint());
        assertEquals("Checkpoint", read.getCheckpointName());
        assertNull(read.getEvent());

        //Read 2nd event
        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        read = reader.readNextEvent(60000);
        assertEquals(testString2, read.getEvent());
        Checkpoint cpResult = checkpoint.get(5, TimeUnit.SECONDS);
        assertTrue(checkpoint.isDone());
        assertEquals("Checkpoint", cpResult.getName());

        //Read 3rd Event.
        read = reader.readNextEvent(1000);
        assertEquals(testString3, read.getEvent());
        assertFalse(read.isCheckpoint());

        //Initiate 2nd checkpoint -- silent
        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        @Cleanup("shutdown")
        final InlineExecutor backgroundExecutor1 = new InlineExecutor();
        CompletableFuture<Checkpoint> silentCheckpoint = readerGroup.initiateCheckpoint("SilentCheckpoint" + SILENT, backgroundExecutor1);
        assertFalse(silentCheckpoint.isDone());
        read = reader.readNextEvent(1000);
        assertFalse(read.isCheckpoint());

        //Read 4th event
        assertEquals(testString4, read.getEvent());

        //Initiate 3rd checkpoint
        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        @Cleanup("shutdown")
        final InlineExecutor backgroundExecutor3 = new InlineExecutor();
        CompletableFuture<Checkpoint> checkpointLast = readerGroup.initiateCheckpoint("CheckpointLast", backgroundExecutor3);
        assertFalse(checkpointLast.isDone());
        read = reader.readNextEvent(60000);
        assertTrue(read.isCheckpoint());
        assertEquals("CheckpointLast", read.getCheckpointName());
        assertNull(read.getEvent());

        //Read 5th event onwards
        assertEquals(testString5, reader.readNextEvent(100).getEvent());
        assertEquals(testString6, reader.readNextEvent(100).getEvent());
        assertNull(reader.readNextEvent(100).getEvent());

        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        readerGroup.resetReaderGroup();
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
        assertEquals(testString5, read.getEvent());
        assertEquals(testString6, reader.readNextEvent(100).getEvent());
        assertNull(reader.readNextEvent(100).getEvent());

    }

    @Test(timeout = 20000)
    public void testCheckpointAndRestoreNoLastCheckpoint() throws ReinitializationRequiredException {
        String endpoint = "localhost";
        String streamName = "testCPAndRestoreNolastCP";
        String readerName = "readerCP";
        String readerGroupName = "testCPAndRestore-groupNolastCP";
        int port = TestUtils.getAvailableListenPort();
        String testString = "Hello world\n";
        String testString1 = "Hello world 1\n";
        String testString2 = "Hello world 2\n";

        String scope = "ScopeNoLastCP";
        StreamSegmentStore store = SERVICE_BUILDER.createStreamSegmentService();
        TableStore tableStore = SERVICE_BUILDER.createTableStoreService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, tableStore, SERVICE_BUILDER.getLowPriorityExecutor(),
                new IndexAppendProcessor(SERVICE_BUILDER.getLowPriorityExecutor(), store));
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
        producer.writeEvent(testString1);
        producer.writeEvent(testString2);
        producer.flush();                                               

        AtomicLong clock = new AtomicLong();
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader(readerName, readerGroupName, serializer,
                ReaderConfig.builder().build(), clock::get,
                clock::get);
        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        EventRead<String> read = reader.readNextEvent(60000);
        assertEquals(testString, read.getEvent());

        //Read First event
        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        read = reader.readNextEvent(1000);
        assertEquals(testString1, read.getEvent());

        //Read 2nd event.
        assertEquals(testString2, reader.readNextEvent(1000).getEvent());
        assertNull(reader.readNextEvent(100).getEvent());

        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        readerGroup.resetReaderGroup();
        try {
            reader.readNextEvent(60000);
            fail();
        } catch (ReinitializationRequiredException e) {
            //Expected
        }
        reader.close();
        reader = clientFactory.createReader(readerName, readerGroupName, serializer, ReaderConfig.builder().build());

        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        read = reader.readNextEvent(1000);
        assertEquals(testString, read.getEvent());
        assertEquals(testString1, reader.readNextEvent(1000).getEvent());
        assertEquals(testString2, reader.readNextEvent(1000).getEvent());
        assertNull(reader.readNextEvent(100).getEvent());

    }

    /* Scenario where application trigger a call to read data from a specific streamcut.
    * Consider scenario when there is no checkpoint created in the stream.
    * Now when resetReaderGroup() call triggers, then reader group starts reading from start of streamcut not start of stream.
    */
    @Test(timeout = 20000)
    public void testCPAndRestoreToStreamCutNoLastCheckpoint() throws ReinitializationRequiredException, InterruptedException,
            ExecutionException, TimeoutException {
        String endpoint = "localhost";
        String streamName = "testCPAndRestoreToSCNoCP";
        String readerName = "reader";
        String readerGroupName = "testCheckpointAndRestore-groupSCNoCP";
        int port = TestUtils.getAvailableListenPort();
        String testString = "Hello world\n";
        String testString1 = "Hello world 1\n";
        String testString2 = "Hello world 2\n";
        String testString3 = "Hello world 3\n";
        String testString4 = "Hello world 4\n";
        String testString5 = "Hello world 5\n";
        String testString6 = "Hello world 6\n";

        String scope = "ScopeNoCP";
        StreamSegmentStore store = SERVICE_BUILDER.createStreamSegmentService();
        TableStore tableStore = SERVICE_BUILDER.createTableStoreService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, tableStore, SERVICE_BUILDER.getLowPriorityExecutor(),
                new IndexAppendProcessor(SERVICE_BUILDER.getLowPriorityExecutor(), store));
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(scope, endpoint, port);
        @Cleanup
        MockClientFactory clientFactory = streamManager.getClientFactory();
        Map<Segment, Long> positions = new HashMap<>();
        IntStream.of(0).forEach(segNum -> positions.put(new Segment(scope, streamName, segNum), 114L));
        StreamCut streamCut = new StreamCutImpl(Stream.of(scope, streamName), positions);
        Map<Stream, StreamCut> streamcuts = new HashMap<Stream, StreamCut>();
        streamcuts.put(Stream.of(scope, streamName), streamCut);
        ReaderGroupConfig groupConfig = ReaderGroupConfig.builder()
                                                         .disableAutomaticCheckpoints()
                                                         .startFromStreamCuts(streamcuts).build();
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
        producer.writeEvent(testString1);
        producer.writeEvent(testString2);
        producer.writeEvent(testString3);
        producer.writeEvent(testString4);
        producer.writeEvent(testString5);
        producer.writeEvent(testString6);
        producer.flush();

        AtomicLong clock = new AtomicLong();
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader(readerName, readerGroupName, serializer,
                ReaderConfig.builder().build(), clock::get,
                clock::get);

        //Read event from stream cut. Stream cut created after completion of event 3 so read next returns from event 4 onwards
        EventRead<String> read = reader.readNextEvent(60000);
        assertEquals(testString4, read.getEvent());
        assertEquals(testString5, reader.readNextEvent(1000).getEvent());

        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        readerGroup.resetReaderGroup();
        try {
            reader.readNextEvent(60000);
            fail();
        } catch (ReinitializationRequiredException e) {
            //Expected
        }
        reader.close();
        reader = clientFactory.createReader(readerName, readerGroupName, serializer, ReaderConfig.builder().build());

        //reset to start of streamcut ie read from event 4 onwards
        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        assertEquals(testString4, reader.readNextEvent(1000).getEvent());
        assertEquals(testString5, reader.readNextEvent(1000).getEvent());
        assertEquals(testString6, reader.readNextEvent(1000).getEvent());
        assertNull(reader.readNextEvent(100).getEvent());

    }

    /*
    * Consider the scenario where reader in RG reading Stream s1 and created checkpoint CP1(say position p1 in segment).
    *  Now there is call reset readergroup by adding new stream s2 and streamcut sc1 on stream s1(say position p2(>p1) in segment
    *  Now If there is call to resetReaderGroup() then It should start reading stream s1 from streamcut sc1 and s2 from head.
    */
    @Test(timeout = 20000)
    public void testCPAndRestoreWithAddDeleteStream() throws ReinitializationRequiredException, InterruptedException,
            ExecutionException, TimeoutException {
        String endpoint = "localhost";
        String streamName = "testCPAndRestoreMewStream";
        String streamNameNew = "testCPAndRestoreMewStream1";
        String readerName = "reader";
        String readerGroupName = "testCheckpointAndRestore-groupSCNoCP";
        int port = TestUtils.getAvailableListenPort();
        String testString = "Hello world\n";
        String testString1 = "Hello world 1\n";
        String testString2 = "Hello world 2\n";
        String testString3 = "Hello world 3\n";
        String testString4 = "Hello world 4\n";
        String testString5 = "Hello world 5\n";
        String testString6 = "Hello world 6\n";

        String streamString = "Hello Stream2\n";
        String streamString1 = "Hello Stream2 1\n";
        String streamString2 = "Hello Stream2 2\n";
        String streamString3 = "Hello Stream2 3\n";
        String streamString4 = "Hello Stream2 4\n";
        String streamString5 = "Hello Stream2 5\n";
        String streamString6 = "Hello Stream2 6\n";

        String scope = "ScopeNewStream";
        StreamSegmentStore store = SERVICE_BUILDER.createStreamSegmentService();
        TableStore tableStore = SERVICE_BUILDER.createTableStoreService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, tableStore, SERVICE_BUILDER.getLowPriorityExecutor(),
                new IndexAppendProcessor(SERVICE_BUILDER.getLowPriorityExecutor(), store));
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(scope, endpoint, port);
        @Cleanup
        MockClientFactory clientFactory = streamManager.getClientFactory();
        Map<Segment, Long> positions = new HashMap<>();
        IntStream.of(0).forEach(segNum -> positions.put(new Segment(scope, streamName, segNum), 114L));
        StreamCut streamCut = new StreamCutImpl(Stream.of(scope, streamName), positions);
        Map<Stream, StreamCut> streamcuts = new HashMap<Stream, StreamCut>();
        streamcuts.put(Stream.of(scope, streamName), streamCut);
        ReaderGroupConfig groupConfig = ReaderGroupConfig.builder()
                                                         .disableAutomaticCheckpoints()
                                                         .stream(Stream.of(scope, streamName)).build();

        //readerGroup pointing to new streamcut for s1 and head of newly added stream s2
        ReaderGroupConfig groupConfigSC = ReaderGroupConfig.builder()
                                                         .disableAutomaticCheckpoints()
                                                         .stream(Stream.of(scope, streamName), streamCut)
                                                         .stream(Stream.of(scope, streamNameNew)).build();
        streamManager.createScope(scope);
        streamManager.createStream(scope, streamName, StreamConfiguration.builder()
                                                                         .scalingPolicy(ScalingPolicy.fixed(1))
                                                                         .build());
        streamManager.createStream(scope, streamNameNew, StreamConfiguration.builder()
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
        producer.writeEvent(testString1);
        producer.writeEvent(testString2);
        producer.writeEvent(testString3);
        producer.writeEvent(testString4);
        producer.writeEvent(testString5);
        producer.writeEvent(testString6);
        producer.flush();

        @Cleanup
        EventStreamWriter<String> producer1 = clientFactory.createEventWriter(streamNameNew, serializer,
                EventWriterConfig.builder().build());
        producer1.writeEvent(streamString);
        producer1.writeEvent(streamString1);
        producer1.writeEvent(streamString2);
        producer1.writeEvent(streamString3);
        producer1.writeEvent(streamString4);
        producer1.writeEvent(streamString5);
        producer1.writeEvent(streamString6);
        producer1.flush();

        AtomicLong clock = new AtomicLong();
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader(readerName, readerGroupName, serializer,
                ReaderConfig.builder().build(), clock::get,
                clock::get);

        //Read event from stream s1.
        EventRead<String> read = reader.readNextEvent(60000);
        assertEquals(testString, read.getEvent());
        assertEquals(testString1, reader.readNextEvent(1000).getEvent());

        //Initiate 1st checkpoint after 2 events read by reader ie 0 and 1
        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        @Cleanup("shutdown")
        final InlineExecutor backgroundExecutor = new InlineExecutor();
        CompletableFuture<Checkpoint> checkpoint = readerGroup.initiateCheckpoint("Checkpoint", backgroundExecutor);
        assertFalse(checkpoint.isDone());
        read = reader.readNextEvent(60000);
        assertTrue(read.isCheckpoint());
        assertEquals("Checkpoint", read.getCheckpointName());
        assertNull(read.getEvent());

        //Read 2 more event ie event 2 and event 3
        assertEquals(testString2, reader.readNextEvent(1000).getEvent());
        assertEquals(testString3, reader.readNextEvent(1000).getEvent());

        //Modify RG to add new stream s2 and start reading from stream s1 (streamcut sc1). SC1 positions are greater than checkpoint.
        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        readerGroup.resetReaderGroup(groupConfigSC);
        try {
            reader.readNextEvent(60000);
            fail();
        } catch (ReinitializationRequiredException e) {
            //Expected
        }
        reader.close();
        reader = clientFactory.createReader(readerName, readerGroupName, serializer, ReaderConfig.builder().build());

        //reset to start of streamcut of S1 and new stream s2
        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        assertNotNull(reader.readNextEvent(1000).getEvent());
        assertNotNull(reader.readNextEvent(1000).getEvent());
        assertNotNull(reader.readNextEvent(1000).getEvent());
        assertNotNull(reader.readNextEvent(1000).getEvent());
        assertNotNull(reader.readNextEvent(1000).getEvent());
        assertNotNull(reader.readNextEvent(1000).getEvent());

        //Reset reader group without passing config been called which essentially reset readergroup to streamcut sc1 of stream s1 and head of stream s2.
        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        readerGroup.resetReaderGroup();
        try {
            reader.readNextEvent(60000);

        } catch (ReinitializationRequiredException e) {
            //Expected
        }
        reader.close();
        reader = clientFactory.createReader(readerName, readerGroupName, serializer, ReaderConfig.builder().build());

        //reset to start of streamcut of S1(event 4 onwards in total 3 event from stream s1) and new stream s2(7 events)
        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        assertNotNull(reader.readNextEvent(1000).getEvent());
        assertNotNull(reader.readNextEvent(1000).getEvent());
        assertNotNull(reader.readNextEvent(1000).getEvent());
        assertNotNull(reader.readNextEvent(1000).getEvent());
        assertNotNull(reader.readNextEvent(1000).getEvent());
        assertNotNull(reader.readNextEvent(1000).getEvent());
        assertNotNull(reader.readNextEvent(1000).getEvent());
        assertNotNull(reader.readNextEvent(1000).getEvent());
        assertNotNull(reader.readNextEvent(1000).getEvent());
        assertNotNull(reader.readNextEvent(1000).getEvent());
        assertNull(reader.readNextEvent(100).getEvent());

    }

    @Test(timeout = 20000)
    public void testMoreReadersThanSegments() throws ReinitializationRequiredException, InterruptedException,
                                              ExecutionException, TimeoutException {
        String endpoint = "localhost";
        String streamName = "testMoreReadersThanSegments";
        String readerGroupName = "testMoreReadersThanSegments-group";
        int port = TestUtils.getAvailableListenPort();
        String testString = "Hello world\n";
        String scope = "Scope1";
        StreamSegmentStore store = SERVICE_BUILDER.createStreamSegmentService();
        TableStore tableStore = SERVICE_BUILDER.createTableStoreService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, tableStore, SERVICE_BUILDER.getLowPriorityExecutor(),
                new IndexAppendProcessor(SERVICE_BUILDER.getLowPriorityExecutor(), store));
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
        String streamName = "testGenerateStreamCuts";
        String readerGroupName = "testGenerateStreamCuts-group1";
        int port = TestUtils.getAvailableListenPort();
        String testString = "Hello world\n";
        String scope = "Scope12";
        StreamSegmentStore store = SERVICE_BUILDER.createStreamSegmentService();
        TableStore tableStore = SERVICE_BUILDER.createTableStoreService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, tableStore, SERVICE_BUILDER.getLowPriorityExecutor(),
                new IndexAppendProcessor(SERVICE_BUILDER.getLowPriorityExecutor(), store));
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
        String streamName = "testGenerateStreamCuts";
        String readerName = "reader";
        String readerGroupName = "testGenerateStreamCuts-group";
        int port = TestUtils.getAvailableListenPort();
        String testString = "Hello world\n";
        String scope = "Scope1";
        StreamSegmentStore store = SERVICE_BUILDER.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, mock(TableStore.class),
                SERVICE_BUILDER.getLowPriorityExecutor(), new IndexAppendProcessor(SERVICE_BUILDER.getLowPriorityExecutor(), store));
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


    /*
     *  Test for check isReadCompleted() immediate after checkpoint event, post reading all data till end of streamCut
     * Multiple checkpoint initiate successfully post reaching isReadCompleted
     */
    @Test(timeout = 20000)
    public void testCPEndOfStream1RBeforeEOS() throws ReinitializationRequiredException, InterruptedException,
            ExecutionException, TimeoutException {
        String endpoint = "localhost";
        String streamName = "testCP2RBeforeEOS";
        String readerName = "reader";
        String readerGroupName = "testCheckpoint2R-groupBefEOS";
        int port = TestUtils.getAvailableListenPort();
        String testString = "Hello world\n";
        String testString1 = "Hello world 1\n";
        String testString2 = "Hello world 2\n";
        String testString3 = "Hello world 3\n";

        String scope = "Scope2RBeforeEOS";
        StreamSegmentStore store = SERVICE_BUILDER.createStreamSegmentService();
        TableStore tableStore = SERVICE_BUILDER.createTableStoreService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store, tableStore, SERVICE_BUILDER.getLowPriorityExecutor(),
                new IndexAppendProcessor(SERVICE_BUILDER.getLowPriorityExecutor(), store));
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(scope, endpoint, port);
        @Cleanup
        MockClientFactory clientFactory = streamManager.getClientFactory();
        Map<Segment, Long> positions = new HashMap<>();
        //StreamCut position is decided depending on event length, here below as end streamcut is 64L that means only 2 events are read from stream
        IntStream.of(0).forEach(segNum -> positions.put(new Segment(scope, streamName, segNum), 64L));
        StreamCut streamCut = new StreamCutImpl(Stream.of(scope, streamName), positions);
        Map<Stream, StreamCut> streamcuts = new HashMap<Stream, StreamCut>();
        streamcuts.put(Stream.of(scope, streamName), streamCut);
        ReaderGroupConfig groupConfigSC = ReaderGroupConfig.builder()
                                                           .disableAutomaticCheckpoints()
                                                           .stream(Stream.of(scope, streamName), StreamCut.UNBOUNDED, streamCut)
                                                           .build();
        streamManager.createScope(scope);
        streamManager.createStream(scope, streamName, StreamConfiguration.builder()
                                                                         .scalingPolicy(ScalingPolicy.fixed(1))
                                                                         .build());

        streamManager.createReaderGroup(readerGroupName, groupConfigSC);
        @Cleanup
        ReaderGroup readerGroup = streamManager.getReaderGroup(readerGroupName);
        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<String> producer = clientFactory.createEventWriter(streamName, serializer,
                EventWriterConfig.builder().build());
        producer.writeEvent("seg0", testString);
        producer.writeEvent("seg0", testString1);
        producer.writeEvent("seg0", testString2);
        producer.writeEvent("seg0", testString3);
        producer.flush();

        AtomicLong clock = new AtomicLong();
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader(readerName, readerGroupName, serializer,
                ReaderConfig.builder().build(), clock::get,
                clock::get);

        assertNotNull(reader.readNextEvent(1000).getEvent());
        assertNotNull(reader.readNextEvent(500).getEvent());
        assertNull(reader.readNextEvent(100).getEvent());
        assertFalse(reader.readNextEvent(100).isReadCompleted());
        //Initiating 1st checkpoint
        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        final InlineExecutor backgroundExecutor = new InlineExecutor();
        CompletableFuture<Checkpoint> cpreader1 = readerGroup.initiateCheckpoint("CheckpointReader1", backgroundExecutor);
        assertFalse(cpreader1.isDone());
        // reader read next event at checkpoint
        EventRead<String> read = reader.readNextEvent(100);
        assertTrue(read.isCheckpoint());
        assertEquals("CheckpointReader1", read.getCheckpointName());
        assertNull(read.getEvent());
        read = reader.readNextEvent(100);
        assertNull(read.getEvent());

        Checkpoint cpResultReader1 = cpreader1.get(5, TimeUnit.SECONDS);
        assertTrue(cpreader1.isDone());
        assertTrue(reader.readNextEvent(100).isReadCompleted());

        //Initiating 2 more checkpoints and verified its completion.
        //Initiating 2nd checkpoint.
        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        CompletableFuture<Checkpoint> cpreader2 = readerGroup.initiateCheckpoint("CheckpointReader2", backgroundExecutor);
        assertFalse(cpreader2.isDone());
        // reader read next event at checkpoint
        read = reader.readNextEvent(100);
        assertTrue(read.isCheckpoint());
        assertEquals("CheckpointReader2", read.getCheckpointName());
        assertNull(read.getEvent());
        read = reader.readNextEvent(100);
        assertNull(read.getEvent());

        Checkpoint cpResultReader2 = cpreader2.get(5, TimeUnit.SECONDS);
        assertTrue(cpreader2.isDone());
        assertEquals(Collections.emptyMap(), cpResultReader2.asImpl().getPositions());
        assertTrue(reader.readNextEvent(100).isReadCompleted());

        //Initiating 3rd checkpoint.
        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        CompletableFuture<Checkpoint> cpreader3 = readerGroup.initiateCheckpoint("CheckpointReader3", backgroundExecutor);
        assertFalse(cpreader3.isDone());
        // reader read next event at checkpoint
        read = reader.readNextEvent(100);
        assertTrue(read.isCheckpoint());
        assertEquals("CheckpointReader3", read.getCheckpointName());
        assertNull(read.getEvent());
        read = reader.readNextEvent(100);
        assertNull(read.getEvent());

        Checkpoint cpResultReader3 = cpreader3.get(5, TimeUnit.SECONDS);
        assertTrue(cpreader3.isDone());
        assertEquals(Collections.emptyMap(), cpResultReader3.asImpl().getPositions());
        assertTrue(reader.readNextEvent(100).isReadCompleted());
    }

}

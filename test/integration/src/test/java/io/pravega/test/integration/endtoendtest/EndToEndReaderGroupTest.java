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
package io.pravega.test.integration.endtoendtest;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.Checkpoint;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.server.eventProcessor.LocalController;
import io.pravega.test.common.InlineExecutor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Slf4j
public class EndToEndReaderGroupTest extends AbstractEndToEndTest {

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }


    @Test(timeout = 30000)
    public void testReaderOffline() throws Exception {
        StreamConfiguration config = getStreamConfig();
        LocalController controller = (LocalController) PRAVEGA.getLocalController();
        String scopeName = "test";
        String streamName = "testReaderOffline";
        controller.createScope(scopeName).get();
        controller.createStream(scopeName, streamName, config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder()
                                                                                    .controllerURI(PRAVEGA.getControllerURI())
                                                                                    .build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(scopeName, controller, connectionFactory);

        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl(scopeName, controller, clientFactory);
        String groupName = "testReaderOffline-group";
        groupManager.createReaderGroup(groupName, ReaderGroupConfig.builder().disableAutomaticCheckpoints()
                                                                 .stream(scopeName + "/" + streamName).build());

        final ReaderGroup readerGroup = groupManager.getReaderGroup(groupName);

        // create a reader
        @Cleanup
        EventStreamReader<String> reader1 = clientFactory.createReader("reader1", groupName, new JavaSerializer<>(),
                                                                       ReaderConfig.builder().build());

        EventRead<String> eventRead = reader1.readNextEvent(100);
        assertNull("Event read should be null since no events are written", eventRead.getEvent());

        @Cleanup
        EventStreamReader<String> reader2 = clientFactory.createReader("reader2", groupName, new JavaSerializer<>(),
                                                                       ReaderConfig.builder().build());

        //make reader1 offline
        readerGroup.readerOffline("reader1", null);

        // write events into the stream.
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new JavaSerializer<>(),
                                                                           EventWriterConfig.builder().build());
        writer.writeEvent("0", "data1").get();
        writer.writeEvent("0", "data2").get();

        eventRead = reader2.readNextEvent(10000);
        assertEquals("data1", eventRead.getEvent());
    }

    @Test(timeout = 30000)
    public void testDeleteReaderGroup() throws Exception {
        StreamConfiguration config = getStreamConfig();
        LocalController controller = (LocalController) PRAVEGA.getLocalController();
        String streamName = "testDeleteReaderGroup";
        controller.createScope("test").get();
        controller.createStream("test", streamName, config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder()
                                                                                    .controllerURI(PRAVEGA.getControllerURI())
                                                                                    .build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl("test", controller, connectionFactory);

        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl("test", controller, clientFactory);
        // Create a ReaderGroup
        String groupName = "testDeleteReaderGroup-group";
        groupManager.createReaderGroup(groupName, ReaderGroupConfig.builder().disableAutomaticCheckpoints()
                                                                 .stream("test/" + streamName).build());
        // Create a Reader
        EventStreamReader<String> reader = clientFactory.createReader("reader1", groupName, serializer, ReaderConfig.builder().build());

        // Write events into the stream.
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, serializer, EventWriterConfig.builder().build());
        writer.writeEvent("0", "data1").get();

        EventRead<String> eventRead = reader.readNextEvent(10000);
        assertEquals("data1", eventRead.getEvent());

        // Close the reader, this internally invokes ReaderGroup#readerOffline
        reader.close();

        //delete the readerGroup.
        groupManager.deleteReaderGroup(groupName);

        // create a new readerGroup with the same name.
        groupManager.createReaderGroup(groupName, ReaderGroupConfig.builder().disableAutomaticCheckpoints()
                                                                 .stream("test/" + streamName).build());

        reader = clientFactory.createReader("reader1", groupName, new JavaSerializer<>(),
                                                                       ReaderConfig.builder().build());
        eventRead = reader.readNextEvent(10000);
        assertEquals("data1", eventRead.getEvent());
    }

    @Test(timeout = 30000)
    public void testCreateSubscriberReaderGroup() throws InterruptedException, ExecutionException {
        StreamConfiguration config = getStreamConfig();
        LocalController controller = (LocalController) PRAVEGA.getLocalController();
        String streamName = "testCreateSubscriberReaderGroup";
        controller.createScope("test").get();
        controller.createStream("test", streamName, config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder()
                .controllerURI(PRAVEGA.getControllerURI())
                .build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl("test", controller, connectionFactory);

        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl("test", controller, clientFactory);

        // Create a ReaderGroup
        String groupName = "testCreateSubscriberReaderGroup-group";
        groupManager.createReaderGroup(groupName, ReaderGroupConfig.builder().disableAutomaticCheckpoints()
                                                                .stream("test/" + streamName)
                                                                .retentionType(ReaderGroupConfig.StreamDataRetention.MANUAL_RELEASE_AT_USER_STREAMCUT).build());

        List<String> subs = controller.listSubscribers("test", streamName).get();
        assertTrue("Subscriber list does not contain required reader group", subs.contains("test/" + groupName));
    }

    @Test(timeout = 30000)
    public void testResetSubscriberToNonSubscriberReaderGroup() throws InterruptedException, ExecutionException {
        StreamConfiguration config = getStreamConfig();
        LocalController controller = (LocalController) PRAVEGA.getLocalController();
        String streamName = "testResetSubscriberToNonSubscriberReaderGroup";
        controller.createScope("test").get();
        controller.createStream("test", streamName, config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder()
                .controllerURI(PRAVEGA.getControllerURI())
                .build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl("test", controller, connectionFactory);

        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl("test", controller, clientFactory);

        // Create a ReaderGroup
        String group = "testResetSubscriberToNonSubscriberReaderGroup-group";
        groupManager.createReaderGroup(group, ReaderGroupConfig.builder().disableAutomaticCheckpoints()
                .stream("test/" + streamName).retentionType(ReaderGroupConfig.StreamDataRetention.MANUAL_RELEASE_AT_USER_STREAMCUT).build());

        List<String> subs = controller.listSubscribers("test", streamName).get();
        assertTrue("Subscriber list does not contain required reader group", subs.contains("test/" + group));

        ReaderGroup subGroup = groupManager.getReaderGroup(group);
        subGroup.resetReaderGroup(ReaderGroupConfig.builder().disableAutomaticCheckpoints().stream("test/" + streamName).build());

        subs = controller.listSubscribers("test", streamName).get();
        assertFalse("Subscriber list contains required reader group", subs.contains("test/" + group));
    }

    @Test(timeout = 30000)
    public void testResetNonSubscriberToSubscriberReaderGroup() throws InterruptedException, ExecutionException {
        StreamConfiguration config = getStreamConfig();
        LocalController controller = (LocalController) PRAVEGA.getLocalController();
        String streamName = "testResetNonSubscriberToSubscriberReaderGroup";
        controller.createScope("test").get();
        controller.createStream("test", streamName, config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder()
                .controllerURI(PRAVEGA.getControllerURI())
                .build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl("test", controller, connectionFactory);

        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl("test", controller, clientFactory);

        String group = "testResetNonSubscriberToSubscriberReaderGroup-group";
        // Create a ReaderGroup
        groupManager.createReaderGroup(group, ReaderGroupConfig.builder().disableAutomaticCheckpoints()
                .stream("test/" + streamName).build());

        List<String> subs = controller.listSubscribers("test", streamName).get();
        assertFalse("Subscriber list contains required reader group", subs.contains("test/" + group));

        ReaderGroup subGroup = groupManager.getReaderGroup(group);
        subGroup.resetReaderGroup(ReaderGroupConfig.builder().disableAutomaticCheckpoints().stream("test/" + streamName)
                .retentionType(ReaderGroupConfig.StreamDataRetention.MANUAL_RELEASE_AT_USER_STREAMCUT).build());

        subs = controller.listSubscribers("test", streamName).get();
        assertTrue("Subscriber list does not contain required reader group", subs.contains("test/" + group));
    }

    @Test(timeout = 30000)
    public void testLaggingResetReaderGroup() throws Exception {
        StreamConfiguration config = getStreamConfig();
        LocalController controller = (LocalController) PRAVEGA.getLocalController();
        controller.createScope("test").get();
        controller.createStream("test", "testLaggingResetReaderGroup", config).get();
        controller.createStream("test", "testLaggingResetReaderGroup2", config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder()
                .controllerURI(PRAVEGA.getControllerURI())
                .build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl("test", controller, connectionFactory);

        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl("test", controller, clientFactory);

        UUID rgId = UUID.randomUUID();
        ReaderGroupConfig rgConf = ReaderGroupConfig.builder().disableAutomaticCheckpoints()
                .stream("test/testLaggingResetReaderGroup").retentionType(ReaderGroupConfig.StreamDataRetention.NONE).build();
        rgConf = ReaderGroupConfig.cloneConfig(rgConf, rgId, 0L);

        // Create a ReaderGroup
        groupManager.createReaderGroup("testLaggingResetReaderGroup-group", rgConf);

        ReaderGroupConfig updateConf = ReaderGroupConfig.builder().disableAutomaticCheckpoints()
                .stream("test/testLaggingResetReaderGroup2").retentionType(ReaderGroupConfig.StreamDataRetention.NONE).build();
        updateConf = ReaderGroupConfig.cloneConfig(updateConf, rgId, 0L);

        // Update from the controller end
        controller.updateReaderGroup("test", "testLaggingResetReaderGroup-group", updateConf).join();
        ReaderGroup group = groupManager.getReaderGroup("testLaggingResetReaderGroup-group");
        // Reset from client end
        group.resetReaderGroup(ReaderGroupConfig.builder().disableAutomaticCheckpoints().stream("test/testLaggingResetReaderGroup").build());
    }

    @Test(timeout = 30000)
    public void testMultiScopeReaderGroup() throws Exception {
        LocalController controller = (LocalController) PRAVEGA.getLocalController();

        // Config of two streams with same name and different scopes.
        String defaultScope = "test";
        String scopeA = "scopeA";
        String scopeB = "scopeB";
        String streamName = "testMultiScopeReaderGroup";

        // Create Scopes
        controller.createScope(defaultScope).join();
        controller.createScope(scopeA).join();
        controller.createScope(scopeB).join();

        // Create Streams.
        controller.createStream(scopeA, streamName, getStreamConfig()).join();
        controller.createStream(scopeB, streamName, getStreamConfig()).join();

        // Create ReaderGroup and reader.
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder()
                                                                                    .controllerURI(PRAVEGA.getControllerURI())
                                                                                    .build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(defaultScope, controller, connectionFactory);

        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl(defaultScope, controller, clientFactory);
        String groupName = "testMultiScopeReaderGroup-group";
        groupManager.createReaderGroup(groupName, ReaderGroupConfig.builder()
                                                                 .disableAutomaticCheckpoints()
                                                                 .stream(Stream.of(scopeA, streamName))
                                                                 .stream(Stream.of(scopeB, streamName))
                                                                 .build());

        ReaderGroup readerGroup = groupManager.getReaderGroup(groupName);
        @Cleanup
        EventStreamReader<String> reader1 = clientFactory.createReader("reader1", groupName, new JavaSerializer<>(),
                                                                       ReaderConfig.builder().build());

        // Read empty stream.
        EventRead<String> eventRead = reader1.readNextEvent(100);
        assertNull("Event read should be null since no events are written", eventRead.getEvent());

        // Write to scopeA stream.
        writeTestEvent(scopeA, streamName, 0);
        eventRead = reader1.readNextEvent(10000);
        assertEquals("0", eventRead.getEvent());

        // Write to scopeB stream.
        writeTestEvent(scopeB, streamName, 1);
        eventRead = reader1.readNextEvent(10000);
        assertEquals("1", eventRead.getEvent());

        // Verify ReaderGroup.getStreamNames().
        Set<String> managedStreams = readerGroup.getStreamNames();
        assertTrue(managedStreams.contains(Stream.of(scopeA, streamName).getScopedName()));
        assertTrue(managedStreams.contains(Stream.of(scopeB, streamName).getScopedName()));
    }

    @Test(timeout = 30000)
    public void testGenerateStreamCuts() throws Exception {
        String streamName = "testGenerateStreamCuts";
        final Stream stream = Stream.of(SCOPE, streamName);
        final String group = "testGenerateStreamCuts-group";

        createScope(SCOPE);
        createStream(SCOPE, streamName, ScalingPolicy.fixed(1));

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory
            .withScope(SCOPE, ClientConfig.builder().controllerURI(PRAVEGA.getControllerURI()).build());
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, serializer,
                                                                           EventWriterConfig.builder().build());
        //Prep the stream with data.
        //1.Write events with event size of 30
        writer.writeEvent(randomKeyGenerator.get(), getEventData.apply(1)).join();
        writer.writeEvent(randomKeyGenerator.get(), getEventData.apply(2)).join();
        writer.writeEvent(randomKeyGenerator.get(), getEventData.apply(3)).join();
        writer.writeEvent(randomKeyGenerator.get(), getEventData.apply(4)).join();

        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope(SCOPE, PRAVEGA.getControllerURI());
        groupManager.createReaderGroup(group, ReaderGroupConfig
                .builder().disableAutomaticCheckpoints().groupRefreshTimeMillis(1000)
                .stream(stream)
                .build());

        ReaderGroup readerGroup = groupManager.getReaderGroup(group);

        //Create a reader
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader("readerId", group, serializer,
                                                                      ReaderConfig.builder().build());

        readAndVerify(reader, 1);
        @Cleanup("shutdown")
        InlineExecutor backgroundExecutor = new InlineExecutor();
        CompletableFuture<Map<Stream, StreamCut>> sc = readerGroup.generateStreamCuts(backgroundExecutor);
        // The reader group state will be updated after 1 second.
        TimeUnit.SECONDS.sleep(1);
        EventRead<String> data = reader.readNextEvent(15000);
        assertTrue(Futures.await(sc)); // wait until the streamCut is obtained.

        //expected segment 0 offset is 30L.
        Map<Segment, Long> expectedOffsetMap = ImmutableMap.of(getSegment(streamName, 0, 0), 30L);
        Map<Stream, StreamCut> scMap = sc.join();

        assertEquals("StreamCut for a single stream expected", 1, scMap.size());
        assertEquals("StreamCut pointing ot offset 30L expected", new StreamCutImpl(stream, expectedOffsetMap), scMap.get(stream));
    }

    @Test(timeout = 30000)
    public void testReaderOfflineWithSilentCheckpoint() throws Exception {
        String streamName = "testReaderOfflineWithSilentCheckpoint";
        final Stream stream = Stream.of(SCOPE, streamName);
        final String group = "testReaderOfflineWithSilentCheckpoint-group";

        @Cleanup("shutdown")
        InlineExecutor backgroundExecutor = new InlineExecutor();

        createScope(SCOPE);
        createStream(SCOPE, streamName, ScalingPolicy.fixed(1));

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(SCOPE, ClientConfig.builder().controllerURI(PRAVEGA.getControllerURI()).build());
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, serializer,
                                                                           EventWriterConfig.builder().build());
        //Prep the stream with data.
        //1.Write events with event size of 30
        writer.writeEvent(randomKeyGenerator.get(), getEventData.apply(1)).join();
        writer.writeEvent(randomKeyGenerator.get(), getEventData.apply(2)).join();
        writer.writeEvent(randomKeyGenerator.get(), getEventData.apply(3)).join();
        writer.writeEvent(randomKeyGenerator.get(), getEventData.apply(4)).join();

        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope(SCOPE, PRAVEGA.getControllerURI());
        groupManager.createReaderGroup(group, ReaderGroupConfig
                .builder().disableAutomaticCheckpoints().groupRefreshTimeMillis(1000)
                .stream(stream)
                .build());

        ReaderGroup readerGroup = groupManager.getReaderGroup(group);

        //Create a reader
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader("readerId", group, serializer,
                                                                      ReaderConfig.builder().build());

        //2. Read an event.
        readAndVerify(reader, 1);

        //3. Trigger a checkpoint and verify it is completed.
        CompletableFuture<Checkpoint> checkpoint = readerGroup.initiateCheckpoint("chk1", backgroundExecutor);
        // The reader group state will be updated after 1 second.
        TimeUnit.SECONDS.sleep(1);
        EventRead<String> data = reader.readNextEvent(15000);
        assertTrue(data.isCheckpoint());
        readAndVerify(reader, 2);
        assertTrue("Checkpointing should complete successfully", Futures.await(checkpoint));

        //4. GenerateStreamCuts and validate the offset of stream cut.
        CompletableFuture<Map<Stream, StreamCut>> sc = readerGroup.generateStreamCuts(backgroundExecutor);
        // The reader group state will be updated after 1 second.
        TimeUnit.SECONDS.sleep(1);
        data = reader.readNextEvent(15000);
        assertTrue("StreamCut generation should complete successfully", Futures.await(sc));
        //expected segment 0 offset is 60L, since 2 events are read.
        Map<Segment, Long> expectedOffsetMap = ImmutableMap.of(getSegment(streamName, 0, 0), 60L);
        Map<Stream, StreamCut> scMap = sc.join();
        assertEquals("StreamCut for a single stream expected", 1, scMap.size());
        assertEquals("StreamCut pointing ot offset 30L expected", new StreamCutImpl(stream, expectedOffsetMap),
                     scMap.get(stream));

        //5. Invoke readerOffline with last position as null. The newer readers should start reading
        //from the last checkpointed position
        readerGroup.readerOffline("readerId", null);
        @Cleanup
        EventStreamReader<String> reader1 = clientFactory.createReader("readerId", group, serializer,
                                                                       ReaderConfig.builder().build());
        readAndVerify(reader1, 2);
    }

    @Test(timeout = 40000)
    public void testGenerateStreamCutsWithScaling() throws Exception {
        String streamName = "testGenerateStreamCutsWithScaling";
        final Stream stream = Stream.of(SCOPE, streamName);
        final String group = "testGenerateStreamCutsWithScaling-group";

        createScope(SCOPE);
        createStream(SCOPE, streamName, ScalingPolicy.fixed(2));

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(SCOPE, ClientConfig.builder().controllerURI(PRAVEGA.getControllerURI()).build());
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, serializer,
                                                                           EventWriterConfig.builder().build());
        //Prep the stream with data.
        //1.Write 2 events with event size of 30 to Segment 0.
        writer.writeEvent(keyGenerator.apply("0.1"), getEventData.apply(0)).join();
        writer.writeEvent(keyGenerator.apply("0.1"), getEventData.apply(0)).join();
        //2. Write 2 events with event size of 30 to Segment 1.
        writer.writeEvent(keyGenerator.apply("0.9"), getEventData.apply(1)).join();
        writer.writeEvent(keyGenerator.apply("0.9"), getEventData.apply(1)).join();

        //3. Manually scale stream. Split Segment 0 to Segment 2, Segment 3
        Map<Double, Double> newKeyRanges = new HashMap<>();
        newKeyRanges.put(0.0, 0.25);
        newKeyRanges.put(0.25, 0.5);
        newKeyRanges.put(0.5, 1.0);
        scaleStream(streamName, newKeyRanges);

        //4. Write events to segment 2
        writer.writeEvent(keyGenerator.apply("0.1"), getEventData.apply(2));
        //5. Write events to segment 3
        writer.writeEvent(keyGenerator.apply("0.3"), getEventData.apply(3));
        //6. Write events to Segment 1.
        writer.writeEvent(keyGenerator.apply("0.9"), getEventData.apply(1));

        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope(SCOPE, PRAVEGA.getControllerURI());
        groupManager.createReaderGroup(group, ReaderGroupConfig
                .builder().disableAutomaticCheckpoints().groupRefreshTimeMillis(200)
                .stream(stream)
                .build());

        ReaderGroup readerGroup = groupManager.getReaderGroup(group);

        //7. Create two readers and read 1 event from both the readers
        @Cleanup
        EventStreamReader<String> reader1 = clientFactory.createReader("reader1", group, serializer,
                                                                      ReaderConfig.builder().build());

        @Cleanup
        EventStreamReader<String> reader2 = clientFactory.createReader("reader2", group, serializer,
                                                                       ReaderConfig.builder().build());

        //8. Read 1 event from both the readers.
        String reader1Event = reader1.readNextEvent(15000).getEvent();
        String reader2Event = reader2.readNextEvent(15000).getEvent();

        //9. Read all events from segment 0.
        if (reader1Event.equalsIgnoreCase(getEventData.apply(0))) {
           assertEquals(getEventData.apply(0), reader1.readNextEvent(15000).getEvent());
           assertEquals(getEventData.apply(1), reader2Event);
           readAndVerify(reader2, 1);
        } else {
           assertEquals(getEventData.apply(1), reader1.readNextEvent(15000).getEvent());
           assertEquals(getEventData.apply(0), reader2Event);
           readAndVerify(reader2, 0);
        }

        //Readers see the empty segments
        EventRead<String> data = reader2.readNextEvent(100);
        assertNull(data.getEvent());
        data = reader1.readNextEvent(100);
        assertNull(data.getEvent());
        
        @Cleanup("shutdown")
        InlineExecutor backgroundExecutor = new InlineExecutor();
        readerGroup.initiateCheckpoint("cp1", backgroundExecutor);
        data = reader1.readNextEvent(5000);
        assertEquals("cp1", data.getCheckpointName());
        data = reader2.readNextEvent(5000);
        assertEquals("cp1", data.getCheckpointName());
        
        //New segments are available to read
        reader1Event = reader1.readNextEvent(5000).getEvent();
        assertNotNull(reader1Event);
        reader2Event = reader2.readNextEvent(5000).getEvent();
        assertNotNull(reader2Event);
        
        //10. Generate StreamCuts
        CompletableFuture<Map<Stream, StreamCut>> sc = readerGroup.generateStreamCuts(backgroundExecutor);
        // The reader group state will be updated after 1 second.
        TimeUnit.SECONDS.sleep(1);
        
        reader1Event = reader1.readNextEvent(500).getEvent();
        reader2Event = reader2.readNextEvent(500).getEvent();
        
        //11 Validate the StreamCut generated.
        assertTrue(Futures.await(sc)); // wait until the streamCut is obtained.

        Set<Segment> expectedSegments = ImmutableSet.<Segment>builder()
                .add(getSegment(streamName, 4, 1)) // 1 event read from segment 1
                .add(getSegment(streamName, 2, 1)) // 1 event read from segment 2 or 3.
                .add(getSegment(streamName, 3, 1))
                .build();
        Map<Stream, StreamCut> scMap = sc.join();
        assertEquals("StreamCut for a single stream expected", 1, scMap.size());
        assertEquals(expectedSegments, scMap.get(stream).asImpl().getPositions().keySet());
    }

    private StreamConfiguration getStreamConfig() {
        return StreamConfiguration.builder()
                                  .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 2))
                                  .build();
    }

    private void writeTestEvent(String scope, String streamName, int eventId) {
        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, ClientConfig.builder().controllerURI(PRAVEGA.getControllerURI()).build());
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new JavaSerializer<>(), EventWriterConfig.builder().build());

        writer.writeEvent( "0", Integer.toString(eventId)).join();
    }

    @Test(timeout = 60000, expected = ObjectClosedException.class)
    public void testRGManagerCallWhenExecutorisNotAvailable() throws Exception {
        StreamConfiguration config = getStreamConfig();
        LocalController controller = (LocalController) PRAVEGA.getLocalController();
        String streamName = "testDeleteReaderGroup";
        controller.createScope("test").get();
        controller.createStream("test", streamName, config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder()
                .controllerURI(PRAVEGA.getControllerURI())
                .build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl("test", controller, connectionFactory);

        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl("test", controller, clientFactory);
        // Create a ReaderGroup
        String groupName = "testDeleteReaderGroup-group";
        groupManager.createReaderGroup(groupName, ReaderGroupConfig.builder().disableAutomaticCheckpoints()
                .stream("test/" + streamName).build());
        // Create a Reader
        EventStreamReader<String> reader = clientFactory.createReader("reader2", groupName, serializer, ReaderConfig.builder().build());

        // Write events into the stream.
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, serializer, EventWriterConfig.builder().build());
        writer.writeEvent("0", "data1").get();

        EventRead<String> eventRead = reader.readNextEvent(10000);
        eventRead.getEvent();

        // Close the reader, this internally invokes ReaderGroup#readerOffline
        reader.close();

        // close the ReaderGroupManager
        groupManager.close();
        
        // try to get the readerGroup, this should throw an exception as we have added a check to verify if the executor is available.
        groupManager.getReaderGroup(groupName);
    }
}

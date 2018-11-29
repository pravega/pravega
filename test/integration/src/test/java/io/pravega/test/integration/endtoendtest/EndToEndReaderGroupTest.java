/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.endtoendtest;

import com.google.common.collect.ImmutableMap;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
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
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.server.eventProcessor.LocalController;
import io.pravega.test.common.InlineExecutor;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
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
        LocalController controller = (LocalController) controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope("test").get();
        controller.createStream("test", "test", config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder()
                                                                                    .controllerURI(URI.create("tcp://" + serviceHost))
                                                                                    .build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl("test", controller, connectionFactory);

        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl("test", controller, clientFactory,
                                                                     connectionFactory);
        groupManager.createReaderGroup("group", ReaderGroupConfig.builder().disableAutomaticCheckpoints()
                                                                 .stream("test/test").build());

        final ReaderGroup readerGroup = groupManager.getReaderGroup("group");

        // create a reader
        @Cleanup
        EventStreamReader<String> reader1 = clientFactory.createReader("reader1", "group", new JavaSerializer<>(),
                                                                       ReaderConfig.builder().build());

        EventRead<String> eventRead = reader1.readNextEvent(100);
        assertNull("Event read should be null since no events are written", eventRead.getEvent());

        @Cleanup
        EventStreamReader<String> reader2 = clientFactory.createReader("reader2", "group", new JavaSerializer<>(),
                                                                       ReaderConfig.builder().build());

        //make reader1 offline
        readerGroup.readerOffline("reader1", null);

        // write events into the stream.
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter("test", new JavaSerializer<>(),
                                                                           EventWriterConfig.builder().build());
        writer.writeEvent("0", "data1").get();
        writer.writeEvent("0", "data2").get();

        eventRead = reader2.readNextEvent(10000);
        assertEquals("data1", eventRead.getEvent());
    }

    @Test(timeout = 30000)
    public void testMultiScopeReaderGroup() throws Exception {
        LocalController controller = (LocalController) controllerWrapper.getController();

        // Config of two streams with same name and different scopes.
        String defaultScope = "test";
        String scopeA = "scopeA";
        String scopeB = "scopeB";
        String streamName = "test";

        // Create Scopes
        controllerWrapper.getControllerService().createScope(defaultScope).get();
        controllerWrapper.getControllerService().createScope(scopeA).get();
        controllerWrapper.getControllerService().createScope(scopeB).get();

        // Create Streams.
        controller.createStream(scopeA, streamName, getStreamConfig()).get();
        controller.createStream(scopeB, streamName, getStreamConfig()).get();

        // Create ReaderGroup and reader.
        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder()
                                                                                    .controllerURI(URI.create("tcp://" + serviceHost))
                                                                                    .build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(streamName, controller, connectionFactory);

        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl(defaultScope, controller, clientFactory,
                                                                     connectionFactory);
        groupManager.createReaderGroup("group", ReaderGroupConfig.builder()
                                                                 .disableAutomaticCheckpoints()
                                                                 .stream(Stream.of(scopeA, streamName))
                                                                 .stream(Stream.of(scopeB, streamName))
                                                                 .build());

        ReaderGroup readerGroup = groupManager.getReaderGroup("group");
        @Cleanup
        EventStreamReader<String> reader1 = clientFactory.createReader("reader1", "group", new JavaSerializer<>(),
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
        final Stream stream = Stream.of(SCOPE, STREAM);
        final String group = "group";

        createScope(SCOPE);
        createStream(SCOPE, STREAM, ScalingPolicy.fixed(1));

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(SCOPE, ClientConfig.builder().controllerURI(controllerURI).build());
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(STREAM, serializer,
                                                                           EventWriterConfig.builder().build());
        //Prep the stream with data.
        //1.Write events with event size of 30
        writer.writeEvent(randomKeyGenerator.get(), getEventData.apply(1)).join();
        writer.writeEvent(randomKeyGenerator.get(), getEventData.apply(2)).join();
        writer.writeEvent(randomKeyGenerator.get(), getEventData.apply(3)).join();
        writer.writeEvent(randomKeyGenerator.get(), getEventData.apply(4)).join();

        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope(SCOPE, controllerURI);
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
        Map<Segment, Long> expectedOffsetMap = ImmutableMap.of(getSegment(0, 0), 30L);
        Map<Stream, StreamCut> scMap = sc.join();

        assertEquals("StreamCut for a single stream expected", 1, scMap.size());
        assertEquals("StreamCut pointing ot offset 30L expected", new StreamCutImpl(stream, expectedOffsetMap), scMap.get(stream));
    }

    @Test(timeout = 30000)
    public void testReaderOfflineWithSilentCheckpoint() throws Exception {
        final Stream stream = Stream.of(SCOPE, STREAM);
        final String group = "group";

        @Cleanup("shutdown")
        InlineExecutor backgroundExecutor = new InlineExecutor();

        createScope(SCOPE);
        createStream(SCOPE, STREAM, ScalingPolicy.fixed(1));

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(SCOPE, ClientConfig.builder().controllerURI(controllerURI).build());
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(STREAM, serializer,
                                                                           EventWriterConfig.builder().build());
        //Prep the stream with data.
        //1.Write events with event size of 30
        writer.writeEvent(randomKeyGenerator.get(), getEventData.apply(1)).join();
        writer.writeEvent(randomKeyGenerator.get(), getEventData.apply(2)).join();
        writer.writeEvent(randomKeyGenerator.get(), getEventData.apply(3)).join();
        writer.writeEvent(randomKeyGenerator.get(), getEventData.apply(4)).join();

        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope(SCOPE, controllerURI);
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
        Map<Segment, Long> expectedOffsetMap = ImmutableMap.of(getSegment(0, 0), 60L);
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
        final Stream stream = Stream.of(SCOPE, STREAM);
        final String group = "group";

        createScope(SCOPE);
        createStream(SCOPE, STREAM, ScalingPolicy.fixed(2));

        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(SCOPE, ClientConfig.builder().controllerURI(controllerURI).build());
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(STREAM, serializer,
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
        scaleStream(STREAM, newKeyRanges);

        //4. Write events to segment 2
        writer.writeEvent(keyGenerator.apply("0.1"), getEventData.apply(2));
        //5. Write events to segment 3
        writer.writeEvent(keyGenerator.apply("0.3"), getEventData.apply(3));
        //6. Write events to Segment 1.
        writer.writeEvent(keyGenerator.apply("0.9"), getEventData.apply(1));

        @Cleanup
        ReaderGroupManager groupManager = ReaderGroupManager.withScope(SCOPE, controllerURI);
        groupManager.createReaderGroup(group, ReaderGroupConfig
                .builder().disableAutomaticCheckpoints().groupRefreshTimeMillis(1000)
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
           readAndVerify(reader1, 2, 3);
        } else {
           assertEquals(getEventData.apply(0), reader2.readNextEvent(15000).getEvent());
           readAndVerify(reader2, 2, 3);
        }

        //10. Generate StreamCuts
        @Cleanup("shutdown")
        InlineExecutor backgroundExecutor = new InlineExecutor();
        CompletableFuture<Map<Stream, StreamCut>> sc = readerGroup.generateStreamCuts(backgroundExecutor);
        // The reader group state will be updated after 1 second.
        TimeUnit.SECONDS.sleep(1);

        //11. Fetch the next events from the reader. Before the reader returns the next events both the readers update its current position.
        EventRead<String> data = reader2.readNextEvent(5000);
        data = reader1.readNextEvent(15000);

        //10 Validate the StreamCut generated.
        assertTrue(Futures.await(sc)); // wait until the streamCut is obtained.
        //expected segment 0 offset is 30L.
        Map<Segment, Long> expectedOffsetMap = ImmutableMap.<Segment, Long>builder()
                .put(getSegment(1, 0), 30L) // 1 event read from segment 1
                .put(getSegment(2, 1), 30L) // 1 event read from segment 2 and 3.
                .put(getSegment(3, 1), 30L)
                .build();
        Map<Stream, StreamCut> scMap = sc.join();
        assertEquals("StreamCut for a single stream expected", 1, scMap.size());
        assertEquals(new StreamCutImpl(stream, expectedOffsetMap), scMap.get(stream));
    }

    private StreamConfiguration getStreamConfig() {
        return StreamConfiguration.builder()
                                  .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 2))
                                  .build();
    }

    private void writeTestEvent(String scope, String streamName, int eventId) {
        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, ClientConfig.builder().controllerURI(controllerURI).build());
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new JavaSerializer<>(), EventWriterConfig.builder().build());

        writer.writeEvent( "0", Integer.toString(eventId)).join();
    }

}

/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.endtoendtest;

import io.pravega.client.ClientConfig;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.controller.server.eventProcessor.LocalController;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.InlineExecutor;
import lombok.Cleanup;
import org.junit.Test;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class EndToEndCBRTest extends AbstractEndToEndTest {

    private static final long CLOCK_ADVANCE_INTERVAL = 60 * 1000000000L;

    @Test
    public void testReaderGroupAutoRetention() throws Exception {
        String scope = "test";
        String streamName = "test";
        String groupName = "group";
        StreamConfiguration config = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .retentionPolicy(RetentionPolicy.builder().retentionType(RetentionPolicy.RetentionType.TIME).retentionParam(1L).retentionMax(Long.MAX_VALUE).build())
                .build();
        LocalController controller = (LocalController) controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope(scope).get();
        controller.createStream(scope, streamName, config).get();
        Stream stream = Stream.of(scope, streamName);
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder()
                .controllerURI(URI.create("tcp://" + serviceHost))
                .build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory);

        // write events
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, serializer,
                EventWriterConfig.builder().build());
        writer.writeEvent("1", "e1").join();
        writer.writeEvent("2", "e2").join();

        // Create a ReaderGroup
        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl(scope, controller, clientFactory);
        groupManager.createReaderGroup(groupName, ReaderGroupConfig
                .builder().disableAutomaticCheckpoints()
                .retentionType(ReaderGroupConfig.StreamDataRetention.AUTOMATIC_RELEASE_AT_LAST_CHECKPOINT)
                .stream(stream).build());

        // Create a Reader
        AtomicLong clock = new AtomicLong();
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader("reader1", groupName, serializer,
                ReaderConfig.builder().build(), clock::get,
                clock::get);
        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        EventRead<String> read = reader.readNextEvent(60000);
        assertEquals("e1", read.getEvent());

        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        @Cleanup("shutdown")
        final InlineExecutor backgroundExecutor = new InlineExecutor();
        ReaderGroup readerGroup = groupManager.getReaderGroup(groupName);
        CompletableFuture<Checkpoint> checkpoint = readerGroup.initiateCheckpoint("Checkpoint", backgroundExecutor);
        assertFalse(checkpoint.isDone());
        read = reader.readNextEvent(60000);
        assertTrue(read.isCheckpoint());
        assertEquals("Checkpoint", read.getCheckpointName());
        assertNull(read.getEvent());

        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        read = reader.readNextEvent(60000);
        assertEquals("e2", read.getEvent());
        Checkpoint cpResult = checkpoint.get(5, TimeUnit.SECONDS);
        assertTrue(checkpoint.isDone());
        assertEquals("Checkpoint", cpResult.getName());
        read = reader.readNextEvent(100);
        assertNull(read.getEvent());
        assertFalse(read.isCheckpoint());

        AssertExtensions.assertEventuallyEquals(true, () -> controller.getSegmentsAtTime(new StreamImpl(scope, streamName), 0L)
                .join().values().stream().anyMatch(off -> off > 0), 30 * 1000L);

        groupManager.createReaderGroup("group2", ReaderGroupConfig.builder().disableAutomaticCheckpoints().stream(NameUtils.getScopedStreamName(scope, streamName)).build());
        EventStreamReader<String> reader2 = clientFactory.createReader("reader2", "group2", serializer, ReaderConfig.builder().build());
        EventRead<String> eventRead2 = reader2.readNextEvent(10000);
        assertEquals("e2", eventRead2.getEvent());
    }

    @Test
    public void testReaderGroupManualRetention() throws Exception {
        String scope = "test";
        String streamName = "test";
        String groupName = "group";
        StreamConfiguration config = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .retentionPolicy(RetentionPolicy.builder().retentionType(RetentionPolicy.RetentionType.TIME).retentionParam(1L).retentionMax(Long.MAX_VALUE).build())
                .build();
        LocalController controller = (LocalController) controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope(scope).get();
        controller.createStream(scope, streamName, config).get();
        Stream stream = Stream.of(scope, streamName);
        @Cleanup
        ConnectionFactory connectionFactory = new SocketConnectionFactoryImpl(ClientConfig.builder()
                .controllerURI(URI.create("tcp://" + serviceHost))
                .build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory);

        // write events
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, serializer,
                EventWriterConfig.builder().build());
        writer.writeEvent("1", "e1").join();
        writer.writeEvent("2", "e2").join();

        // Create a ReaderGroup
        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl(scope, controller, clientFactory);
        groupManager.createReaderGroup(groupName, ReaderGroupConfig
                .builder().disableAutomaticCheckpoints()
                .retentionType(ReaderGroupConfig.StreamDataRetention.MANUAL_RELEASE_AT_USER_STREAMCUT)
                .stream(stream).build());

        System.out.println("Sub list: " + controller.listSubscribers(scope, streamName).get());

        // Create a Reader
        AtomicLong clock = new AtomicLong();
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader("reader1", groupName, serializer,
                ReaderConfig.builder().build(), clock::get,
                clock::get);
        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        EventRead<String> read = reader.readNextEvent(60000);
        assertEquals("e1", read.getEvent());

        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        @Cleanup("shutdown")
        final InlineExecutor backgroundExecutor = new InlineExecutor();
        ReaderGroup readerGroup = groupManager.getReaderGroup(groupName);
        CompletableFuture<Map<Stream, StreamCut>> streamCuts = readerGroup.generateStreamCuts(backgroundExecutor);
        assertFalse(streamCuts.isDone());
        read = reader.readNextEvent(60000);
        assertEquals("e2", read.getEvent());

        clock.addAndGet(CLOCK_ADVANCE_INTERVAL);
        read = reader.readNextEvent(60000);
        assertNull(read.getEvent());
        Map<Stream, StreamCut> scResult = streamCuts.get(5, TimeUnit.SECONDS);
        assertTrue(streamCuts.isDone());

        System.out.println("StreamCut: " + scResult);

        readerGroup.updateRetentionStreamCut(scResult);

        System.out.println("Segments: " + controller.getSegmentsAtTime(stream, 0L).join());
        AssertExtensions.assertEventuallyEquals(true, () -> controller.getSegmentsAtTime(stream, 0L)
                .join().values().stream().anyMatch(off -> off > 0), 30 * 1000L);

        groupManager.createReaderGroup("group2", ReaderGroupConfig.builder().disableAutomaticCheckpoints().stream(NameUtils.getScopedStreamName(scope, streamName)).build());
        EventStreamReader<String> reader2 = clientFactory.createReader("reader2", "group2", serializer, ReaderConfig.builder().build());
        EventRead<String> eventRead2 = reader2.readNextEvent(10000);
        assertEquals("e2", eventRead2.getEvent());
    }
}

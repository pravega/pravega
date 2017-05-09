/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration;

import io.pravega.service.contracts.StreamSegmentStore;
import io.pravega.service.server.host.handler.PravegaConnectionListener;
import io.pravega.service.server.store.ServiceBuilder;
import io.pravega.service.server.store.ServiceBuilderConfig;
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
import io.pravega.client.stream.Sequence;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.mock.MockClientFactory;
import io.pravega.client.stream.mock.MockStreamManager;
import io.pravega.test.common.InlineExecutor;
import io.pravega.test.common.TestUtils;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import java.util.Collections;
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
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store);
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(scope, endpoint, port);
        MockClientFactory clientFactory = streamManager.getClientFactory();
        ReaderGroupConfig groupConfig = ReaderGroupConfig.builder().startingPosition(Sequence.MIN_VALUE).build();
        streamManager.createScope(scope);
        streamManager.createStream(scope, streamName, StreamConfiguration.builder()
                                                                         .scope(scope)
                                                                         .streamName(streamName)
                                                                         .scalingPolicy(ScalingPolicy.fixed(1))
                                                                         .build());
        ReaderGroup readerGroup = streamManager.createReaderGroup(readerGroupName, groupConfig,
                                                                  Collections.singleton(streamName));
        JavaSerializer<String> serializer = new JavaSerializer<>();
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
        CompletableFuture<Checkpoint> checkpoint = readerGroup.initiateCheckpoint("Checkpoint", new InlineExecutor());
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
        readerGroup.resetReadersToCheckpoint(cpResult);
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
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store);
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(scope, endpoint, port);
        MockClientFactory clientFactory = streamManager.getClientFactory();
        ReaderGroupConfig groupConfig = ReaderGroupConfig.builder().startingPosition(Sequence.MIN_VALUE).build();
        streamManager.createScope(scope);
        streamManager.createStream(scope, streamName, StreamConfiguration.builder()
                                                                         .scope(scope)
                                                                         .streamName(streamName)
                                                                         .scalingPolicy(ScalingPolicy.fixed(1))
                                                                         .build());
        ReaderGroup readerGroup = streamManager.createReaderGroup(readerGroupName, groupConfig,
                                                                  Collections.singleton(streamName));
        JavaSerializer<String> serializer = new JavaSerializer<>();
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
        CompletableFuture<Checkpoint> checkpoint = readerGroup.initiateCheckpoint("Checkpoint", new InlineExecutor());
        assertFalse(checkpoint.isDone());
        EventRead<String> read = reader1.readNextEvent(60000);
        assertTrue(read.isCheckpoint());
        assertEquals("Checkpoint", read.getCheckpointName());
        assertNull(read.getEvent());
        read = reader2.readNextEvent(60000);
        assertTrue(read.isCheckpoint());
        assertEquals("Checkpoint", read.getCheckpointName());
        assertNull(read.getEvent());
        
        Checkpoint cpResult = checkpoint.get(5, TimeUnit.SECONDS);
        assertTrue(checkpoint.isDone());
        assertEquals("Checkpoint", cpResult.getName());
    }
}

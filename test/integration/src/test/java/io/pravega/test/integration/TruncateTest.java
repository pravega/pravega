/**
 * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration;

import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentMetadataClient;
import io.pravega.client.segment.impl.SegmentMetadataClientFactory;
import io.pravega.client.segment.impl.SegmentMetadataClientFactoryImpl;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.TruncatedDataException;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.stream.mock.MockClientFactory;
import io.pravega.client.stream.mock.MockController;
import io.pravega.client.stream.mock.MockStreamManager;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.Cleanup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TruncateTest {

    private static final String SCOPE = "Scope";
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
    
    @Test
    public void appendThroughStreamingClient() throws InterruptedException, ExecutionException, TimeoutException, TruncatedDataException, ReinitializationRequiredException {
        String endpoint = "localhost";
        String streamName = "abc";
        int port = TestUtils.getAvailableListenPort();
        String testString = "Hello world\n";
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store);
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager(SCOPE, endpoint, port);
        @Cleanup
        MockClientFactory clientFactory = streamManager.getClientFactory();
        streamManager.createScope(SCOPE);
        streamManager.createStream(SCOPE, streamName, null);
        Serializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<String> producer = clientFactory.createEventWriter(streamName, serializer, EventWriterConfig.builder().build());
        Future<Void> ack = producer.writeEvent(testString);
        ack.get(5, TimeUnit.SECONDS);
        
        SegmentMetadataClientFactory metadataClientFactory = new SegmentMetadataClientFactoryImpl(new MockController(endpoint, port, streamManager.getConnectionFactory()), streamManager.getConnectionFactory());
        SegmentMetadataClient metadataClient = metadataClientFactory.createSegmentMetadataClient(new Segment(SCOPE, streamName, 0), "");
        assertEquals(0, metadataClient.getSegmentInfo().getStartingOffset());
        long writeOffset = metadataClient.getSegmentInfo().getWriteOffset();
        assertEquals(writeOffset, metadataClient.fetchCurrentSegmentLength());
        assertTrue(metadataClient.getSegmentInfo().getWriteOffset() > testString.length());
        metadataClient.truncateSegment(writeOffset);
        assertEquals(writeOffset, metadataClient.getSegmentInfo().getStartingOffset());
        assertEquals(writeOffset, metadataClient.getSegmentInfo().getWriteOffset());
        assertEquals(writeOffset, metadataClient.fetchCurrentSegmentLength());
        
        ack = producer.writeEvent(testString);
        ack.get(5, TimeUnit.SECONDS);
        
        streamManager.createReaderGroup("ReaderGroup", ReaderGroupConfig.builder().disableAutomaticCheckpoints().stream(new StreamImpl(SCOPE, streamName)).build());
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader("reader", "ReaderGroup", serializer, ReaderConfig.builder().build());
        AssertExtensions.assertThrows(TruncatedDataException.class, () -> reader.readNextEvent(2000));
        EventRead<String> event = reader.readNextEvent(2000);
        assertEquals(testString, event.getEvent());
        event = reader.readNextEvent(100);
        assertEquals(null, event.getEvent());
    }
    
}

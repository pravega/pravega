/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;


import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.segment.impl.ConditionalOutputStreamFactory;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentInputStreamFactory;
import io.pravega.client.segment.impl.SegmentMetadataClientFactory;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentOutputStreamFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;

import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import lombok.val;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ClientFactoryTest {

    @Mock
    private ConnectionFactory connectionFactory;
    @Mock
    private Controller controllerClient;
    @Mock
    private SegmentInputStreamFactory inFactory;
    @Mock
    private SegmentOutputStreamFactory outFactory;
    @Mock
    private ConditionalOutputStreamFactory condFactory;
    @Mock
    private SegmentMetadataClientFactory metaFactory;

    @Test
    public void testCloseWithExternalController() {
        ClientFactoryImpl clientFactory = new ClientFactoryImpl("scope", controllerClient);
        clientFactory.close();
        verify(controllerClient, times(1)).close();
    }

    @Test
    public void testCloseWithExternalControllerConnectionFactory() {
        ClientFactoryImpl clientFactory = new ClientFactoryImpl("scope", controllerClient, connectionFactory);
        clientFactory.close();
        verify(connectionFactory, times(1)).close();
        verify(controllerClient, times(1)).close();
    }

    @Test
    public void testEventWriter() {
        String scope = "scope";
        String stream = "stream1";
        // setup mocks
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(scope, controllerClient, connectionFactory, inFactory, outFactory, condFactory, metaFactory);
        NavigableMap<Double, SegmentWithRange> segments = new TreeMap<>();
        Segment segment = new Segment(scope, stream, 0L);
        segments.put(1.0, new SegmentWithRange(segment, 0.0, 1.0));
        StreamSegments currentSegments = new StreamSegments(segments, "");
        SegmentOutputStream outStream = mock(SegmentOutputStream.class);
        when(controllerClient.getCurrentSegments(scope, stream))
                .thenReturn(CompletableFuture.completedFuture(currentSegments));
        when(outFactory.createOutputStreamForSegment(eq(segment), any(), any(), any())).thenReturn(outStream);

        EventWriterConfig writerConfig = EventWriterConfig.builder().build();
        EventStreamWriter<String> writer = clientFactory.createEventWriter(stream, new JavaSerializer<String>(), writerConfig);
        assertEquals(writerConfig, writer.getConfig());
    }

    @Test(expected = IllegalStateException.class)
    public void testEventWriterSealedStream() {
        String scope = "scope";
        String stream = "stream1";
        // setup mocks
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(scope, controllerClient, connectionFactory, inFactory, outFactory, condFactory, metaFactory);
        StreamSegments currentSegments = new StreamSegments(new TreeMap<>(), "");
        when(controllerClient.getCurrentSegments(scope, stream))
                .thenReturn(CompletableFuture.completedFuture(currentSegments));

        EventWriterConfig writerConfig = EventWriterConfig.builder().build();
        EventStreamWriter<String> writer = clientFactory.createEventWriter(stream, new JavaSerializer<String>(), writerConfig);
        assertEquals(writerConfig, writer.getConfig());
    }

    @Test
    public void testTxnWriter() {
        ClientFactoryImpl clientFactory = new ClientFactoryImpl("scope", controllerClient, connectionFactory);
        EventWriterConfig writerConfig = EventWriterConfig.builder().build();
        val txnWriter = clientFactory.createTransactionalEventWriter("writer1", "stream1", new JavaSerializer<String>(), writerConfig);
        assertEquals(writerConfig, txnWriter.getConfig());
        val txnWriter2 = clientFactory.createTransactionalEventWriter( "stream1", new JavaSerializer<String>(), writerConfig);
        assertEquals(writerConfig, txnWriter2.getConfig());
    }
    
}

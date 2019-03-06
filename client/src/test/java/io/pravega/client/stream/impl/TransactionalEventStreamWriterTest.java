/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentOutputStreamFactory;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.client.stream.impl.EventStreamWriterTest.FakeSegmentOutputStream;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

public class TransactionalEventStreamWriterTest extends ThreadPooledTestSuite {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(10);

    private StreamSegments getSegments(Segment segment) {
        NavigableMap<Double, SegmentWithRange> segments = new TreeMap<>();
        segments.put(1.0, new SegmentWithRange(segment, 0.0, 1.0));
        return new StreamSegments(segments, "");
    }
    
    private CompletableFuture<StreamSegments> getSegmentsFuture(Segment segment) {
        return CompletableFuture.completedFuture(getSegments(segment));
    }
    
    @Test
    public void testTxn() throws TxnFailedException {
        String scope = "scope";
        String streamName = "stream";
        StreamImpl stream = new StreamImpl(scope, streamName);
        Segment segment = new Segment(scope, streamName, 0);
        UUID txid = UUID.randomUUID();
        EventWriterConfig config = EventWriterConfig.builder().transactionTimeoutTime(0).build();
        SegmentOutputStreamFactory streamFactory = Mockito.mock(SegmentOutputStreamFactory.class);
        Controller controller = Mockito.mock(Controller.class);
        Mockito.when(controller.getCurrentSegments(scope, streamName)).thenReturn(getSegmentsFuture(segment));
        FakeSegmentOutputStream outputStream = new FakeSegmentOutputStream(segment);
        FakeSegmentOutputStream bad = new FakeSegmentOutputStream(segment);
        Mockito.when(controller.createTransaction(stream, 0))
               .thenReturn(CompletableFuture.completedFuture(new TxnSegments(getSegments(segment), txid)));
        Mockito.when(streamFactory.createOutputStreamForTransaction(eq(segment), eq(txid), any(), any()))
                .thenReturn(outputStream);
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment), any(), any(), any())).thenReturn(bad);

        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        TransactionalEventStreamWriter<String> writer = new TransactionalEventStreamWriterImpl<>(stream, "id", controller, streamFactory, serializer,
                config);
        Transaction<String> txn = writer.beginTxn();
        txn.writeEvent("Foo");
        assertTrue(bad.unacked.isEmpty());
        assertEquals(1, outputStream.unacked.size());
        outputStream.unacked.get(0).getAckFuture().complete(null);
        txn.flush();
        assertTrue(bad.unacked.isEmpty());
        assertTrue(outputStream.unacked.isEmpty());
    }

    @Test
    public void testTxnFailed() {
        String scope = "scope";
        String streamName = "stream";
        StreamImpl stream = new StreamImpl(scope, streamName);
        Segment segment = new Segment(scope, streamName, 0);
        UUID txid = UUID.randomUUID();
        EventWriterConfig config = EventWriterConfig.builder().transactionTimeoutTime(0).build();
        SegmentOutputStreamFactory streamFactory = Mockito.mock(SegmentOutputStreamFactory.class);
        Controller controller = Mockito.mock(Controller.class);
        Mockito.when(controller.getCurrentSegments(scope, streamName)).thenReturn(getSegmentsFuture(segment));
        FakeSegmentOutputStream outputStream = new FakeSegmentOutputStream(segment);
        FakeSegmentOutputStream bad = new FakeSegmentOutputStream(segment);
        Mockito.when(controller.createTransaction(stream, 0))
               .thenReturn(CompletableFuture.completedFuture(new TxnSegments(getSegments(segment), txid)));
        Mockito.when(streamFactory.createOutputStreamForTransaction(eq(segment), eq(txid), any(), any()))
                .thenReturn(outputStream);
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment), any(), any(), any())).thenReturn(bad);

        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        TransactionalEventStreamWriter<String> writer = new TransactionalEventStreamWriterImpl<>(stream, "id", controller, streamFactory, serializer,
                config);
        Transaction<String> txn = writer.beginTxn();
        outputStream.invokeSealedCallBack();
        try {
            txn.writeEvent("Foo");
        } catch (TxnFailedException e) {
            // Expected
        }
        assertTrue(bad.unacked.isEmpty());
        assertEquals(1, outputStream.unacked.size());
    }

}

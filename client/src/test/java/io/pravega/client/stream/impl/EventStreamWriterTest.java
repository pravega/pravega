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

import io.pravega.client.segment.impl.EndOfSegmentException;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentOutputStreamFactory;
import io.pravega.client.segment.impl.SegmentSealedException;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.client.stream.mock.MockSegmentIoStreams;
import io.pravega.common.Exceptions;
import io.pravega.common.util.ReusableLatch;
import io.pravega.test.common.Async;
import io.pravega.test.common.InlineExecutor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

public class EventStreamWriterTest {

    @Test
    public void testWrite() {
        String scope = "scope";
        String streamName = "stream";
        StreamImpl stream = new StreamImpl(scope, streamName);
        Segment segment = new Segment(scope, streamName, 0);
        EventWriterConfig config = EventWriterConfig.builder().build();
        SegmentOutputStreamFactory streamFactory = Mockito.mock(SegmentOutputStreamFactory.class);
        Controller controller = Mockito.mock(Controller.class);
        Mockito.when(controller.getCurrentSegments(scope, streamName)).thenReturn(getSegmentsFuture(segment));
        MockSegmentIoStreams outputStream = new MockSegmentIoStreams(segment);
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment), any(), any())).thenReturn(outputStream);
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream, controller, streamFactory,
                new JavaSerializer<>(), config, new InlineExecutor());
        writer.writeEvent("Foo");
        writer.writeEvent("Bar");
        writer.close();
        try {
            writer.writeEvent("fail");
            fail();
        } catch (IllegalStateException e) {
            // expected.
        }
    }

    private StreamSegments getSegments(Segment segment) {
        NavigableMap<Double, Segment> segments = new TreeMap<>();
        segments.put(1.0, segment);
        return new StreamSegments(segments);
    }
    
    private CompletableFuture<StreamSegments> getSegmentsFuture(Segment segment) {
        return CompletableFuture.completedFuture(getSegments(segment));
    }
    
    private CompletableFuture<StreamSegmentsWithPredecessors> getReplacement(Segment old, Segment repacement) {
        Map<SegmentWithRange, List<Integer>> segments = new HashMap<>();
        segments.put(new SegmentWithRange(repacement, 0, 1), Collections.singletonList(old.getSegmentNumber()));
        return CompletableFuture.completedFuture(new StreamSegmentsWithPredecessors(segments));
    }

    @Test
    public void testFailOnClose() throws SegmentSealedException {
        String scope = "scope";
        String streamName = "stream";
        StreamImpl stream = new StreamImpl(scope, streamName);
        Segment segment = new Segment(scope, streamName, 0);
        EventWriterConfig config = EventWriterConfig.builder().build();
        SegmentOutputStreamFactory streamFactory = Mockito.mock(SegmentOutputStreamFactory.class);
        Controller controller = Mockito.mock(Controller.class);
        Mockito.when(controller.getCurrentSegments(scope, streamName)).thenReturn(getSegmentsFuture(segment));
        SegmentOutputStream outputStream = Mockito.mock(SegmentOutputStream.class);
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment), any(), any())).thenReturn(outputStream);
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream, controller, streamFactory,
                new JavaSerializer<>(), config, new InlineExecutor());
        Mockito.doThrow(new RuntimeException("Intentional exception")).when(outputStream).close();
        writer.writeEvent("Foo");
        writer.writeEvent("Bar");
        try {
            writer.close();
            fail();
        } catch (RuntimeException e) {
            // expected.
        }
        try {
            writer.writeEvent("fail");
            fail();
        } catch (IllegalStateException e) {
            // expected
        }
    }

    @NotThreadSafe
    @RequiredArgsConstructor
    private static final class FakeSegmentOutputStream implements SegmentOutputStream {
        private final Segment segment;
        private Consumer<Segment> callBackForSealed;
        private final ArrayList<PendingEvent> writes = new ArrayList<>();

        private void invokeSealedCallBack() {
            if (callBackForSealed != null) {
                callBackForSealed.accept(segment);
            }
        }

        @Override
        public void write(PendingEvent event) {
            writes.add(event);
        }

        @Override
        public void close() throws SegmentSealedException {
        }

        @Override
        public void flush() throws SegmentSealedException  {
            writes.clear();
        }

        @Override
        public List<PendingEvent> getUnackedEventsOnSeal() {
            return Collections.unmodifiableList(writes);
        }

        @Override
        public String getSegmentName() {
            return segment.getScopedName();
        }

    }

    @NotThreadSafe
    @RequiredArgsConstructor
    private static final class SealedSegmentOutputStream implements SegmentOutputStream {
        private final Segment segment;
        private Consumer<Segment> callBackForSealed;
        private final ArrayList<PendingEvent> writes = new ArrayList<>();
        private ReusableLatch flushLatch = new ReusableLatch();
        private void invokeSealedCallBack() {
            if (callBackForSealed != null) {
                callBackForSealed.accept(segment);
            }
        }

        private void releaseFlush() {
            flushLatch.release();
        }

        @Override
        public void write(PendingEvent event) {
            writes.add(event);
        }

        @Override
        public void close() throws SegmentSealedException {
            flush();
        }

        @Override
        public void flush() throws SegmentSealedException {
            //flushLatch is used to simulate a blocking Flush(). .
            Exceptions.handleInterrupted(() -> flushLatch.await());
            throw new SegmentSealedException(segment.toString());
        }

        @Override
        public List<PendingEvent> getUnackedEventsOnSeal() {
            return Collections.unmodifiableList(writes);
        }

        @Override
        public String getSegmentName() {
            return segment.getScopedName();
        }

    }

    @Test
    public void testEndOfSegment() {
        String scope = "scope";
        String streamName = "stream";
        String routingKey = "RoutingKey";
        StreamImpl stream = new StreamImpl(scope, streamName);
        Segment segment1 = new Segment(scope, streamName, 0);
        Segment segment2 = new Segment(scope, streamName, 1);
        EventWriterConfig config = EventWriterConfig.builder().build();
        SegmentOutputStreamFactory streamFactory = Mockito.mock(SegmentOutputStreamFactory.class);
        Controller controller = Mockito.mock(Controller.class);

        FakeSegmentOutputStream outputStream1 = new FakeSegmentOutputStream(segment1);
        FakeSegmentOutputStream outputStream2 = new FakeSegmentOutputStream(segment2);

        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment1), any(), any())).thenAnswer(i -> {
            outputStream1.callBackForSealed = i.getArgument(1);
            return outputStream1;
        });
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment2), any(), any())).thenAnswer(i -> {
            outputStream2.callBackForSealed = i.getArgument(1);
            return outputStream2;
        });

        JavaSerializer<String> serializer = new JavaSerializer<>();
        Mockito.when(controller.getCurrentSegments(scope, streamName))
               .thenReturn(getSegmentsFuture(segment1))
               .thenReturn(getSegmentsFuture(segment2));
        @Cleanup
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream, controller, streamFactory, serializer,
                config, new InlineExecutor());

        writer.writeEvent(routingKey, "Foo");

        Mockito.when(controller.getCurrentSegments(scope, streamName)).thenReturn(getSegmentsFuture(segment2));
        Mockito.when(controller.getSuccessors(segment1)).thenReturn(getReplacement(segment1, segment2));

        //invoke the sealed callback invocation simulating a netty call back with segment sealed exception.
        outputStream1.invokeSealedCallBack();

        writer.writeEvent(routingKey, "Bar");
        Mockito.verify(controller, Mockito.times(1)).getCurrentSegments(any(), any());

        assertEquals(2, outputStream2.getUnackedEventsOnSeal().size());
        assertEquals("Foo", serializer.deserialize(outputStream2.getUnackedEventsOnSeal().get(0).getData()));
        assertEquals("Bar", serializer.deserialize(outputStream2.getUnackedEventsOnSeal().get(1).getData()));
    }

    @Test
    public void testEndOfSegmentBackgroundRefresh() {
        String scope = "scope";
        String streamName = "stream";
        String routingKey = "RoutingKey";
        StreamImpl stream = new StreamImpl(scope, streamName);
        Segment segment1 = new Segment(scope, streamName, 0);
        Segment segment2 = new Segment(scope, streamName, 1);
        EventWriterConfig config = EventWriterConfig.builder().build();
        SegmentOutputStreamFactory streamFactory = Mockito.mock(SegmentOutputStreamFactory.class);
        Controller controller = Mockito.mock(Controller.class);

        FakeSegmentOutputStream outputStream1 = new FakeSegmentOutputStream(segment1);
        FakeSegmentOutputStream outputStream2 = new FakeSegmentOutputStream(segment2);

        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment1), any(), any()))
                .thenAnswer(i -> {
                    outputStream1.callBackForSealed = i.getArgument(1);
                    return outputStream1;
                });
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment2), any(), any()))
                .thenAnswer(i -> {
                    outputStream2.callBackForSealed = i.getArgument(1);
                    return outputStream2;
                });

        JavaSerializer<String> serializer = new JavaSerializer<>();
        Mockito.when(controller.getCurrentSegments(scope, streamName))
                .thenReturn(getSegmentsFuture(segment1))
                .thenReturn(getSegmentsFuture(segment2));
        @Cleanup
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream, controller, streamFactory, serializer,
                config, new InlineExecutor());

        writer.writeEvent(routingKey, "Foo");

        Mockito.when(controller.getCurrentSegments(scope, streamName)).thenReturn(getSegmentsFuture(segment2));
        Mockito.when(controller.getSuccessors(segment1)).thenReturn(getReplacement(segment1, segment2));

        writer.writeEvent(routingKey, "Bar");
        Mockito.verify(controller, Mockito.times(1)).getCurrentSegments(any(), any());

        assertEquals(2, outputStream1.getUnackedEventsOnSeal().size());
        assertEquals("Foo", serializer.deserialize(outputStream1.getUnackedEventsOnSeal().get(0).getData()));
        assertEquals("Bar", serializer.deserialize(outputStream1.getUnackedEventsOnSeal().get(1).getData()));

        outputStream1.invokeSealedCallBack(); // simulate a segment sealed callback.
        writer.writeEvent(routingKey, "TestData");
        //This time the actual handleLogSealed is invoked and the resend method resends data to outputStream2.
        assertEquals(3, outputStream2.getUnackedEventsOnSeal().size());
        assertEquals("Foo", serializer.deserialize(outputStream2.getUnackedEventsOnSeal().get(0).getData()));
        assertEquals("Bar", serializer.deserialize(outputStream2.getUnackedEventsOnSeal().get(1).getData()));
        assertEquals("TestData", serializer.deserialize(outputStream2.getUnackedEventsOnSeal().get(2).getData()));

    }

    @Test
    @Ignore
    public void testNoNextSegment() {
        fail();
    }

    @Test
    public void testTxn() throws TxnFailedException {
        String scope = "scope";
        String streamName = "stream";
        StreamImpl stream = new StreamImpl(scope, streamName);
        Segment segment = new Segment(scope, streamName, 0);
        UUID txid = UUID.randomUUID();
        EventWriterConfig config = EventWriterConfig.builder().build();
        SegmentOutputStreamFactory streamFactory = Mockito.mock(SegmentOutputStreamFactory.class);
        Controller controller = Mockito.mock(Controller.class);
        Mockito.when(controller.getCurrentSegments(scope, streamName)).thenReturn(getSegmentsFuture(segment));
        FakeSegmentOutputStream outputStream = new FakeSegmentOutputStream(segment);
        FakeSegmentOutputStream bad = new FakeSegmentOutputStream(segment);
        Mockito.when(controller.createTransaction(stream, 0, 0, 0))
               .thenReturn(CompletableFuture.completedFuture(new TxnSegments(getSegments(segment), txid)));
        Mockito.when(streamFactory.createOutputStreamForTransaction(eq(segment), eq(txid), any(), any()))
                .thenReturn(outputStream);
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment), any(), any())).thenReturn(bad);

        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream, controller, streamFactory, serializer,
                config, new InlineExecutor());
        Transaction<String> txn = writer.beginTxn(0, 0, 0);
        txn.writeEvent("Foo");
        Mockito.verify(controller).getCurrentSegments(any(), any());
        assertTrue(bad.getUnackedEventsOnSeal().isEmpty());
        assertEquals(1, outputStream.getUnackedEventsOnSeal().size());
        outputStream.getUnackedEventsOnSeal().get(0).getAckFuture().complete(true);
        txn.flush();
        assertTrue(bad.getUnackedEventsOnSeal().isEmpty());
        assertTrue(outputStream.getUnackedEventsOnSeal().isEmpty());
    }

    @Test
    public void testTxnFailed() {
        String scope = "scope";
        String streamName = "stream";
        StreamImpl stream = new StreamImpl(scope, streamName);
        Segment segment = new Segment(scope, streamName, 0);
        UUID txid = UUID.randomUUID();
        EventWriterConfig config = EventWriterConfig.builder().build();
        SegmentOutputStreamFactory streamFactory = Mockito.mock(SegmentOutputStreamFactory.class);
        Controller controller = Mockito.mock(Controller.class);
        Mockito.when(controller.getCurrentSegments(scope, streamName)).thenReturn(getSegmentsFuture(segment));
        FakeSegmentOutputStream outputStream = new FakeSegmentOutputStream(segment);
        FakeSegmentOutputStream bad = new FakeSegmentOutputStream(segment);
        Mockito.when(controller.createTransaction(stream, 0, 0, 0))
               .thenReturn(CompletableFuture.completedFuture(new TxnSegments(getSegments(segment), txid)));
        Mockito.when(streamFactory.createOutputStreamForTransaction(eq(segment), eq(txid), any(), any()))
                .thenReturn(outputStream);
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment), any(), any())).thenReturn(bad);

        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream, controller, streamFactory, serializer,
                config, new InlineExecutor());
        Transaction<String> txn = writer.beginTxn(0, 0, 0);
        outputStream.invokeSealedCallBack();
        try {
            txn.writeEvent("Foo");
        } catch (TxnFailedException e) {
            // Expected
        }
        Mockito.verify(controller).getCurrentSegments(any(), any());
        assertTrue(bad.getUnackedEventsOnSeal().isEmpty());
        assertEquals(1, outputStream.getUnackedEventsOnSeal().size());
    }

    @Test
    public void testFlush() {
        String scope = "scope";
        String streamName = "stream";
        StreamImpl stream = new StreamImpl(scope, streamName);
        Segment segment = new Segment(scope, streamName, 0);
        EventWriterConfig config = EventWriterConfig.builder().build();
        SegmentOutputStreamFactory streamFactory = Mockito.mock(SegmentOutputStreamFactory.class);
        Controller controller = Mockito.mock(Controller.class);
        FakeSegmentOutputStream outputStream = new FakeSegmentOutputStream(segment);
        Mockito.when(controller.getCurrentSegments(scope, streamName)).thenReturn(getSegmentsFuture(segment));
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment), any(), any())).thenReturn(outputStream);

        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream, controller, streamFactory, serializer,
                config, new InlineExecutor());
        writer.writeEvent("Foo");
        Mockito.verify(controller).getCurrentSegments(any(), any());
        assertTrue(outputStream.getUnackedEventsOnSeal().size() > 0);
        writer.flush();
        assertTrue(outputStream.getUnackedEventsOnSeal().isEmpty());
    }

    @Test
    public void testSegmentSealedInFlush() throws EndOfSegmentException {
        String scope = "scope";
        String streamName = "stream";
        StreamImpl stream = new StreamImpl(scope, streamName);
        Segment segment1 = new Segment(scope, streamName, 0);
        Segment segment2 = new Segment(scope, streamName, 1);
        EventWriterConfig config = EventWriterConfig.builder().build();

        SegmentOutputStreamFactory streamFactory = Mockito.mock(SegmentOutputStreamFactory.class);
        Controller controller = Mockito.mock(Controller.class);
        FakeSegmentOutputStream outputStream = new FakeSegmentOutputStream(segment1);
        Mockito.when(controller.getCurrentSegments(scope, streamName))
               .thenReturn(getSegmentsFuture(segment1));
        Mockito.when(controller.getSuccessors(segment1)).thenReturn(getReplacement(segment1, segment2));
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment1), any(), any()))
                .thenAnswer(i -> {
                    outputStream.callBackForSealed = i.getArgument(1);
                    return outputStream;
                });
        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream, controller, streamFactory, serializer,
                config, new InlineExecutor());
        writer.writeEvent("Foo");
        Mockito.verify(controller).getCurrentSegments(any(), any());
        assertTrue(outputStream.getUnackedEventsOnSeal().size() > 0);

        MockSegmentIoStreams outputStream2 = new MockSegmentIoStreams(segment2);
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment2), any(), any())).thenReturn(outputStream2);
        outputStream.invokeSealedCallBack();

        writer.flush();

        Mockito.verify(controller, Mockito.times(1)).getCurrentSegments(any(), any());
        assertTrue(outputStream2.fetchCurrentStreamLength() > 0);
        assertEquals(serializer.serialize("Foo"), outputStream2.read());
    }

    @Test
    public void testRetryFlushSegmentSealed() throws EndOfSegmentException {
        String scope = "scope";
        String streamName = "stream";
        StreamImpl stream = new StreamImpl(scope, streamName);
        Segment segment1 = new Segment(scope, streamName, 0);
        Segment segment2 = new Segment(scope, streamName, 1);
        EventWriterConfig config = EventWriterConfig.builder().build();

        SegmentOutputStreamFactory streamFactory = Mockito.mock(SegmentOutputStreamFactory.class);
        Controller controller = Mockito.mock(Controller.class);
        SealedSegmentOutputStream outputStream = new SealedSegmentOutputStream(segment1);
        Mockito.when(controller.getCurrentSegments(scope, streamName))
                .thenReturn(getSegmentsFuture(segment1));
        Mockito.when(controller.getSuccessors(segment1)).thenReturn(getReplacement(segment1, segment2));
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment1), any(), any()))
                .thenAnswer(i -> {
                    outputStream.callBackForSealed = i.getArgument(1);
                    return outputStream;
                });
        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream, controller, streamFactory, serializer,
                config, new InlineExecutor());
        writer.writeEvent("Foo");
        Mockito.verify(controller).getCurrentSegments(any(), any());
        assertTrue(outputStream.getUnackedEventsOnSeal().size() > 0);

        MockSegmentIoStreams outputStream2 = new MockSegmentIoStreams(segment2);
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment2), any(), any())).thenReturn(outputStream2);

        Async.testBlocking(() -> {
            writer.flush(); // blocking on flush.
        }, () -> {
            outputStream.releaseFlush(); // trigger release with a segmentSealedException.
            outputStream.invokeSealedCallBack(); // trigger Sealed Segment call back.
        });

        Mockito.verify(controller, Mockito.times(1)).getCurrentSegments(any(), any());
        assertTrue(outputStream2.fetchCurrentStreamLength() > 0);
        assertEquals(serializer.serialize("Foo"), outputStream2.read());
    }

    @Test
    public void testRetryCloseSegmentSealed() throws EndOfSegmentException {
        String scope = "scope";
        String streamName = "stream";
        StreamImpl stream = new StreamImpl(scope, streamName);
        Segment segment1 = new Segment(scope, streamName, 0);
        Segment segment2 = new Segment(scope, streamName, 1);
        EventWriterConfig config = EventWriterConfig.builder().build();

        SegmentOutputStreamFactory streamFactory = Mockito.mock(SegmentOutputStreamFactory.class);
        Controller controller = Mockito.mock(Controller.class);
        SealedSegmentOutputStream outputStream = new SealedSegmentOutputStream(segment1);
        Mockito.when(controller.getCurrentSegments(scope, streamName))
                .thenReturn(getSegmentsFuture(segment1));
        Mockito.when(controller.getSuccessors(segment1)).thenReturn(getReplacement(segment1, segment2));
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment1), any(), any()))
                .thenAnswer(i -> {
                    outputStream.callBackForSealed = i.getArgument(1);
                    return outputStream;
                });
        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream, controller, streamFactory, serializer,
                config, new InlineExecutor());
        writer.writeEvent("Foo");
        Mockito.verify(controller).getCurrentSegments(any(), any());
        assertTrue(outputStream.getUnackedEventsOnSeal().size() > 0);

        MockSegmentIoStreams outputStream2 = new MockSegmentIoStreams(segment2);
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment2), any(), any())).thenReturn(outputStream2);

        Async.testBlocking(() -> {
            writer.close(); // closed invokes flush internally; this call is blocking on flush.
        }, () -> {
            outputStream.releaseFlush(); // trigger release with a segmentSealedException.
            outputStream.invokeSealedCallBack(); // trigger Sealed Segment call back.
        });

        Mockito.verify(controller, Mockito.times(1)).getCurrentSegments(any(), any());
        assertTrue(outputStream2.fetchCurrentStreamLength() > 0);
        assertTrue(outputStream2.isClosed());
        //the connection to outputStream is closed with the failConnection during SegmentSealed Callback.
        assertEquals(serializer.serialize("Foo"), outputStream2.read());
    }

    @Test
    public void testSegmentSealedInClose() throws EndOfSegmentException {
        String scope = "scope";
        String streamName = "stream";
        StreamImpl stream = new StreamImpl(scope, streamName);
        Segment segment1 = new Segment(scope, streamName, 0);
        Segment segment2 = new Segment(scope, streamName, 1);
        EventWriterConfig config = EventWriterConfig.builder().build();

        SegmentOutputStreamFactory streamFactory = Mockito.mock(SegmentOutputStreamFactory.class);
        Controller controller = Mockito.mock(Controller.class);
        FakeSegmentOutputStream outputStream1 = new FakeSegmentOutputStream(segment1);
        Mockito.when(controller.getCurrentSegments(scope, streamName))
                .thenReturn(getSegmentsFuture(segment1));
        Mockito.when(controller.getSuccessors(segment1)).thenReturn(getReplacement(segment1, segment2));
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment1), any(), any()))
                .thenAnswer(i -> {
                    outputStream1.callBackForSealed = i.getArgument(1);
                    return outputStream1;
                });
        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream, controller, streamFactory, serializer,
                config, new InlineExecutor());
        writer.writeEvent("Foo");
        Mockito.verify(controller).getCurrentSegments(any(), any());
        assertTrue(outputStream1.getUnackedEventsOnSeal().size() > 0);

        MockSegmentIoStreams outputStream2 = new MockSegmentIoStreams(segment2);
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment2), any(), any())).thenReturn(outputStream2);
        outputStream1.invokeSealedCallBack();

        writer.close();

        Mockito.verify(controller, Mockito.times(1)).getCurrentSegments(any(), any());
        assertTrue(outputStream2.fetchCurrentStreamLength() > 0);
        assertEquals(serializer.serialize("Foo"), outputStream2.read());
    }

    @Test
    public void testSegmentSealedInSegmentSealed() {
        String scope = "scope";
        String streamName = "stream";
        String routingKey = "RoutingKey";
        StreamImpl stream = new StreamImpl(scope, streamName);
        Segment segment1 = new Segment(scope, streamName, 0);
        Segment segment2 = new Segment(scope, streamName, 1);
        Segment segment3 = new Segment(scope, streamName, 2);
        EventWriterConfig config = EventWriterConfig.builder().build();

        SegmentOutputStreamFactory streamFactory = Mockito.mock(SegmentOutputStreamFactory.class);
        Controller controller = Mockito.mock(Controller.class);
        FakeSegmentOutputStream outputStream1 = new FakeSegmentOutputStream(segment1);
        FakeSegmentOutputStream outputStream2 = new FakeSegmentOutputStream(segment2);
        FakeSegmentOutputStream outputStream3 = new FakeSegmentOutputStream(segment3);
        Mockito.when(controller.getCurrentSegments(scope, streamName))
               .thenReturn(getSegmentsFuture(segment1));
        Mockito.when(controller.getSuccessors(segment1)).thenReturn(getReplacement(segment1, segment2));
        Mockito.when(controller.getSuccessors(segment2)).thenReturn(getReplacement(segment2, segment3));

        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment1), any(), any()))
                .thenAnswer(i -> {
                    outputStream1.callBackForSealed = i.getArgument(1);
                    return outputStream1;
                });
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment2), any(), any()))
                .thenAnswer(i -> {
                    outputStream2.callBackForSealed = i.getArgument(1);
                    return outputStream2;
                });
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment3), any(), any()))
                .thenAnswer(i -> {
                    outputStream3.callBackForSealed = i.getArgument(1);
                    return outputStream3;
                });
        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream, controller, streamFactory, serializer,
                config, new InlineExecutor());
        writer.writeEvent(routingKey, "Foo");
        Mockito.verify(controller).getCurrentSegments(any(), any());
        assertEquals(1, outputStream1.getUnackedEventsOnSeal().size());
        assertTrue(outputStream2.getUnackedEventsOnSeal().isEmpty());

        outputStream1.invokeSealedCallBack();
        outputStream2.invokeSealedCallBack();

        writer.writeEvent(routingKey, "Bar");

        Mockito.verify(controller, Mockito.times(1)).getCurrentSegments(any(), any());

        assertEquals(1, outputStream2.getUnackedEventsOnSeal().size());
        assertEquals("Foo", serializer.deserialize(outputStream2.getUnackedEventsOnSeal().get(0).getData()));
        assertEquals(2, outputStream3.getUnackedEventsOnSeal().size());
        assertEquals("Foo", serializer.deserialize(outputStream3.getUnackedEventsOnSeal().get(0).getData()));
        assertEquals("Bar", serializer.deserialize(outputStream3.getUnackedEventsOnSeal().get(1).getData()));
    }
}

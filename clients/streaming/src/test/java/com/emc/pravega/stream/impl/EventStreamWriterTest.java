/**
 *
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.TxnFailedException;
import com.emc.pravega.stream.impl.segment.EndOfSegmentException;
import com.emc.pravega.stream.impl.segment.SegmentOutputStream;
import com.emc.pravega.stream.impl.segment.SegmentOutputStreamFactory;
import com.emc.pravega.stream.impl.segment.SegmentSealedException;
import com.emc.pravega.stream.mock.MockSegmentIoStreams;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Cleanup;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
        Mockito.when(controller.getCurrentSegments(scope, streamName)).thenReturn(getSegment(segment));
        MockSegmentIoStreams outputStream = new MockSegmentIoStreams(segment);
        Mockito.when(streamFactory.createOutputStreamForSegment(segment)).thenReturn(outputStream);
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream,
                                                                       controller,
                                                                       streamFactory,
                                                                       new JavaSerializer<>(),
                                                                       config);
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

    private CompletableFuture<StreamSegments> getSegment(Segment segment) {
        NavigableMap<Double, Segment> segments = new TreeMap<>();
        segments.put(1.0, segment);
        return CompletableFuture.completedFuture(new StreamSegments(segments));
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
        Mockito.when(controller.getCurrentSegments(scope, streamName)).thenReturn(getSegment(segment));
        SegmentOutputStream outputStream = Mockito.mock(SegmentOutputStream.class);
        Mockito.when(streamFactory.createOutputStreamForSegment(segment)).thenReturn(outputStream);
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream,
                                                                       controller,
                                                                       streamFactory,
                                                                       new JavaSerializer<>(),
                                                                       config);
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
    private static final class FakeSegmentOutputStream implements SegmentOutputStream {
        private final ArrayList<PendingEvent> writes = new ArrayList<>();
        private boolean sealed = false;

        @Override
        public void write(PendingEvent event) throws SegmentSealedException {
            writes.add(event);
            if (sealed) {
                throw new SegmentSealedException();
            }
        }

        @Override
        public void close() throws SegmentSealedException {
            if (sealed) {
                throw new SegmentSealedException();
            }
            flush();
        }

        @Override
        public void flush() throws SegmentSealedException {
            if (sealed) {
                throw new SegmentSealedException();
            }
            writes.clear();
        }

        @Override
        public List<PendingEvent> getUnackedEvents() {
            return Collections.unmodifiableList(writes);
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
        FakeSegmentOutputStream outputStream1 = new FakeSegmentOutputStream();
        FakeSegmentOutputStream outputStream2 = new FakeSegmentOutputStream();
        Mockito.when(streamFactory.createOutputStreamForSegment(segment1)).thenReturn(outputStream1);
        Mockito.when(streamFactory.createOutputStreamForSegment(segment2)).thenReturn(outputStream2);

        JavaSerializer<String> serializer = new JavaSerializer<>();
        Mockito.when(controller.getCurrentSegments(scope, streamName))
               .thenReturn(getSegment(segment1))
               .thenReturn(getSegment(segment2));
        @Cleanup
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream,
                                                                       controller,
                                                                       streamFactory,
                                                                       serializer,
                                                                       config);

        writer.writeEvent(routingKey, "Foo");
        outputStream1.sealed = true;

        Mockito.when(controller.getCurrentSegments(scope, streamName)).thenReturn(getSegment(segment2));
        writer.writeEvent(routingKey, "Bar");
        Mockito.verify(controller, Mockito.times(2)).getCurrentSegments(Mockito.any(), Mockito.any());

        assertEquals(2, outputStream2.getUnackedEvents().size());
        assertEquals("Foo", serializer.deserialize(outputStream2.getUnackedEvents().get(0).getData()));
        assertEquals("Bar", serializer.deserialize(outputStream2.getUnackedEvents().get(1).getData()));
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
        Mockito.when(controller.getCurrentSegments(scope, streamName)).thenReturn(getSegment(segment));
        FakeSegmentOutputStream outputStream = new FakeSegmentOutputStream();
        FakeSegmentOutputStream bad = new FakeSegmentOutputStream();
        Mockito.when(controller.createTransaction(stream, 0, 0, 0)).thenReturn(CompletableFuture.completedFuture(txid));
        Mockito.when(streamFactory.createOutputStreamForTransaction(segment, txid)).thenReturn(outputStream);
        Mockito.when(streamFactory.createOutputStreamForSegment(segment)).thenReturn(bad);

        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream,
                                                                       controller,
                                                                       streamFactory,
                                                                       serializer,
                                                                       config);
        Transaction<String> txn = writer.beginTxn(0, 0, 0);
        txn.writeEvent("Foo");
        Mockito.verify(controller).getCurrentSegments(Mockito.any(), Mockito.any());
        assertTrue(bad.getUnackedEvents().isEmpty());
        assertEquals(1, outputStream.getUnackedEvents().size());
        txn.flush();
        assertTrue(bad.getUnackedEvents().isEmpty());
        assertTrue(outputStream.getUnackedEvents().isEmpty());
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
        Mockito.when(controller.getCurrentSegments(scope, streamName)).thenReturn(getSegment(segment));
        FakeSegmentOutputStream outputStream = new FakeSegmentOutputStream();
        FakeSegmentOutputStream bad = new FakeSegmentOutputStream();
        Mockito.when(controller.createTransaction(stream, 0, 0, 0)).thenReturn(CompletableFuture.completedFuture(txid));
        Mockito.when(streamFactory.createOutputStreamForTransaction(segment, txid)).thenReturn(outputStream);
        Mockito.when(streamFactory.createOutputStreamForSegment(segment)).thenReturn(bad);

        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream,
                                                                       controller,
                                                                       streamFactory,
                                                                       serializer,
                                                                       config);
        Transaction<String> txn = writer.beginTxn(0, 0, 0);
        outputStream.sealed = true;
        try {
            txn.writeEvent("Foo");
            fail();
        } catch (TxnFailedException e) {
            // Expected
        }
        Mockito.verify(controller).getCurrentSegments(Mockito.any(), Mockito.any());
        assertTrue(bad.getUnackedEvents().isEmpty());
        assertEquals(1, outputStream.getUnackedEvents().size());
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
        FakeSegmentOutputStream outputStream = new FakeSegmentOutputStream();
        Mockito.when(controller.getCurrentSegments(scope, streamName)).thenReturn(getSegment(segment));
        Mockito.when(streamFactory.createOutputStreamForSegment(segment)).thenReturn(outputStream);

        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream,
                                                                       controller,
                                                                       streamFactory,
                                                                       serializer,
                                                                       config);
        writer.writeEvent("Foo");
        Mockito.verify(controller).getCurrentSegments(Mockito.any(), Mockito.any());
        assertTrue(outputStream.getUnackedEvents().size() > 0);
        writer.flush();
        assertTrue(outputStream.getUnackedEvents().isEmpty());
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
        FakeSegmentOutputStream outputStream = new FakeSegmentOutputStream();
        Mockito.when(controller.getCurrentSegments(scope, streamName))
               .thenReturn(getSegment(segment1))
               .thenReturn(getSegment(segment2));
        Mockito.when(streamFactory.createOutputStreamForSegment(segment1)).thenReturn(outputStream);
        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream,
                                                                       controller,
                                                                       streamFactory,
                                                                       serializer,
                                                                       config);
        writer.writeEvent("Foo");
        Mockito.verify(controller).getCurrentSegments(Mockito.any(), Mockito.any());
        assertTrue(outputStream.getUnackedEvents().size() > 0);
        outputStream.sealed = true;

        MockSegmentIoStreams outputStream2 = new MockSegmentIoStreams(segment2);
        Mockito.when(streamFactory.createOutputStreamForSegment(segment2)).thenReturn(outputStream2);
        writer.flush();

        Mockito.verify(controller, Mockito.times(2)).getCurrentSegments(Mockito.any(), Mockito.any());
        assertTrue(outputStream2.fetchCurrentStreamLength() > 0);
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
        FakeSegmentOutputStream outputStream1 = new FakeSegmentOutputStream();
        Mockito.when(controller.getCurrentSegments(scope, streamName))
               .thenReturn(getSegment(segment1))
               .thenReturn(getSegment(segment2));
        Mockito.when(streamFactory.createOutputStreamForSegment(segment1)).thenReturn(outputStream1);
        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream,
                                                                       controller,
                                                                       streamFactory,
                                                                       serializer,
                                                                       config);
        writer.writeEvent("Foo");
        Mockito.verify(controller).getCurrentSegments(Mockito.any(), Mockito.any());
        assertTrue(outputStream1.getUnackedEvents().size() > 0);
        outputStream1.sealed = true;

        MockSegmentIoStreams outputStream2 = new MockSegmentIoStreams(segment2);
        Mockito.when(streamFactory.createOutputStreamForSegment(segment2)).thenReturn(outputStream2);
        writer.close();

        Mockito.verify(controller, Mockito.times(2)).getCurrentSegments(Mockito.any(), Mockito.any());
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
        FakeSegmentOutputStream outputStream1 = new FakeSegmentOutputStream();
        FakeSegmentOutputStream outputStream2 = new FakeSegmentOutputStream();
        FakeSegmentOutputStream outputStream3 = new FakeSegmentOutputStream();
        Mockito.when(controller.getCurrentSegments(scope, streamName))
               .thenReturn(getSegment(segment1))
               .thenReturn(getSegment(segment2))
               .thenReturn(getSegment(segment3));
        
        Mockito.when(streamFactory.createOutputStreamForSegment(segment1)).thenReturn(outputStream1);
        Mockito.when(streamFactory.createOutputStreamForSegment(segment2)).thenReturn(outputStream2);
        Mockito.when(streamFactory.createOutputStreamForSegment(segment3)).thenReturn(outputStream3);
        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream,
                                                                       controller,
                                                                       streamFactory,
                                                                       serializer,
                                                                       config);
        writer.writeEvent(routingKey, "Foo");
        Mockito.verify(controller).getCurrentSegments(Mockito.any(), Mockito.any());
        assertEquals(1, outputStream1.getUnackedEvents().size());
        assertTrue(outputStream2.getUnackedEvents().isEmpty());

        outputStream1.sealed = true;
        outputStream2.sealed = true;

        writer.writeEvent(routingKey, "Bar");

        Mockito.verify(controller, Mockito.times(3)).getCurrentSegments(Mockito.any(), Mockito.any());

        assertEquals(1, outputStream2.getUnackedEvents().size());
        assertEquals("Foo", serializer.deserialize(outputStream2.getUnackedEvents().get(0).getData()));
        assertEquals(2, outputStream3.getUnackedEvents().size());
        assertEquals("Foo", serializer.deserialize(outputStream3.getUnackedEvents().get(0).getData()));
        assertEquals("Bar", serializer.deserialize(outputStream3.getUnackedEvents().get(1).getData()));
    }
}

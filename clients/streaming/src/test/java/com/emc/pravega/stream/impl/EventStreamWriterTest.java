/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.impl.segment.SegmentOutputStream;
import com.emc.pravega.stream.impl.segment.SegmentOutputStreamFactory;
import com.emc.pravega.stream.impl.segment.SegmentSealedException;
import com.emc.pravega.stream.mock.MockSegmentIoStreams;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class EventStreamWriterTest {

    @Test
    public void testWrite() throws SegmentSealedException {
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
                                                                       new EventRouter(stream, controller),
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
                                                                       new EventRouter(stream, controller),
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

    @Test
    public void testEndOfSegment() throws SegmentSealedException {
        String scope = "scope";
        String streamName = "stream";
        StreamImpl stream = new StreamImpl(scope, streamName);
        Segment segment1 = new Segment(scope, streamName, 0);
        Segment segment2 = new Segment(scope, streamName, 1);
        EventWriterConfig config = EventWriterConfig.builder().build();
        SegmentOutputStreamFactory streamFactory = Mockito.mock(SegmentOutputStreamFactory.class);
        Controller controller = Mockito.mock(Controller.class);
        Mockito.when(controller.getCurrentSegments(scope, streamName)).thenReturn(getSegment(segment1)).thenReturn(getSegment(segment2));
        SegmentOutputStream outputStream1 = Mockito.mock(SegmentOutputStream.class);
        Mockito.when(streamFactory.createOutputStreamForSegment(segment1)).thenReturn(outputStream1);       
        EventRouter router = Mockito.mock(EventRouter.class);
        @Cleanup
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream,
                                                                       controller,
                                                                       streamFactory,
                                                                       router,
                                                                       new JavaSerializer<>(),
                                                                       config);
        Mockito.when(router.getSegmentForEvent(null)).thenReturn(segment1).thenReturn(segment2);
        Mockito.doThrow(new SegmentSealedException()).when(outputStream1).write(Mockito.any());
        
        MockSegmentIoStreams outputStream2 = new MockSegmentIoStreams(segment2);
        Mockito.when(streamFactory.createOutputStreamForSegment(segment2)).thenReturn(outputStream2);     
        writer.writeEvent("Foo");
        Mockito.verify(controller, Mockito.times(2)).getCurrentSegments(Mockito.any(), Mockito.any());
        Mockito.verify(router, Mockito.times(2)).getSegmentForEvent(null);
        Mockito.verifyNoMoreInteractions(controller, router, outputStream1);
        assertTrue(outputStream2.fetchCurrentStreamLength() > 0);
    }

    @Test
    public void testNoNextSegment() {
        fail();
    }

    @Test
    public void testDataRetransmitted() {
        fail();
    }

    @Test
    public void testSegmentSealedInFlush() {
        fail();
    }

    @Test
    public void testSegmentSealedInClose() {
        fail();
    }

    @Test
    public void testSegmentSealedInSegmentSealed() {
        fail();
    }

    @Test
    public void testSegmentSealedInTx() {
        fail();
    }

    @Test
    public void testAcking() {
        fail();
    }

    @Test
    public void testFlush() {
        fail();
    }
}

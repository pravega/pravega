/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.client.stream.impl;

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentOutputStreamFactory;
import io.pravega.client.stream.AckFuture;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.mock.MockSegmentIoStreams;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IdempotentEventStreamWriterTest {

    @Test
    public void testWrite() throws InterruptedException, ExecutionException {
        String scope = "scope";
        String streamName = "stream";
        UUID writerId = UUID.randomUUID();
        StreamImpl stream = new StreamImpl(scope, streamName);
        Segment segment = new Segment(scope, streamName, 0);
        EventWriterConfig config = EventWriterConfig.builder().build();
        SegmentOutputStreamFactory streamFactory = Mockito.mock(SegmentOutputStreamFactory.class);
        Controller controller = Mockito.mock(Controller.class);
        Mockito.when(controller.getCurrentSegments(scope, streamName)).thenReturn(getSegmentsFuture(segment));
        MockSegmentIoStreams outputStream = new MockSegmentIoStreams(segment);
        Mockito.when(streamFactory.createOutputStreamForSegment(writerId, segment)).thenReturn(outputStream);
        IdempotentEventStreamWriterImpl<String> writer = new IdempotentEventStreamWriterImpl<>(stream, writerId,
                                                                                               controller,
                                                                                               streamFactory,
                                                                                               new JavaSerializer<>(),
                                                                                               config);
        AckFuture fooFuture = writer.writeEvent("Foo", 1, "Foo");
        AckFuture barFuture = writer.writeEvent("Bar", 2, "Bar");
        assertTrue(fooFuture.isDone());
        assertTrue(barFuture.isDone());
        assertTrue(fooFuture.get());
        assertTrue(barFuture.get());
        writer.close();
    }
    
    private CompletableFuture<StreamSegments> getSegmentsFuture(Segment segment) {
        NavigableMap<Double, Segment> segments = new TreeMap<>();
        segments.put(1.0, segment);
        return CompletableFuture.completedFuture(new StreamSegments(segments));
    }
    
    @Test
    public void testSequenceGoesBackwards() throws InterruptedException, ExecutionException {
        String scope = "scope";
        String streamName = "stream";
        UUID writerId = UUID.randomUUID();
        StreamImpl stream = new StreamImpl(scope, streamName);
        Segment segment = new Segment(scope, streamName, 0);
        EventWriterConfig config = EventWriterConfig.builder().build();
        SegmentOutputStreamFactory streamFactory = Mockito.mock(SegmentOutputStreamFactory.class);
        Controller controller = Mockito.mock(Controller.class);
        Mockito.when(controller.getCurrentSegments(scope, streamName)).thenReturn(getSegmentsFuture(segment));
        MockSegmentIoStreams outputStream = new MockSegmentIoStreams(segment);
        Mockito.when(streamFactory.createOutputStreamForSegment(writerId, segment)).thenReturn(outputStream);
        IdempotentEventStreamWriterImpl<String> writer = new IdempotentEventStreamWriterImpl<>(stream, writerId,
                                                                                               controller,
                                                                                               streamFactory,
                                                                                               new JavaSerializer<>(),
                                                                                               config);
        AckFuture fooFuture = writer.writeEvent("Foo", 2, "Foo");
        AckFuture barFuture = writer.writeEvent("Bar", 1, "Bar");
        assertTrue(fooFuture.isDone());
        assertTrue(barFuture.isDone());
        assertTrue(fooFuture.get());
        assertFalse(barFuture.get());
        writer.close();
    }
    
    @Test
    public void testNewClientWithLowerSequence() throws InterruptedException, ExecutionException  {
        String scope = "scope";
        String streamName = "stream";
        UUID writerId = UUID.randomUUID();
        StreamImpl stream = new StreamImpl(scope, streamName);
        Segment segment = new Segment(scope, streamName, 0);
        EventWriterConfig config = EventWriterConfig.builder().build();
        SegmentOutputStreamFactory streamFactory = Mockito.mock(SegmentOutputStreamFactory.class);
        Controller controller = Mockito.mock(Controller.class);
        Mockito.when(controller.getCurrentSegments(scope, streamName)).thenReturn(getSegmentsFuture(segment));
        MockSegmentIoStreams outputStream = new MockSegmentIoStreams(segment);
        Mockito.when(streamFactory.createOutputStreamForSegment(writerId, segment)).thenReturn(outputStream);
        IdempotentEventStreamWriterImpl<String> writer = new IdempotentEventStreamWriterImpl<>(stream, writerId,
                                                                                               controller,
                                                                                               streamFactory,
                                                                                               new JavaSerializer<>(),
                                                                                               config);
        AckFuture fooFuture = writer.writeEvent("Foo", 2, "Foo");
        assertTrue(fooFuture.isDone());
        assertTrue(fooFuture.get());
        writer = new IdempotentEventStreamWriterImpl<>(stream, writerId, controller, streamFactory,
                new JavaSerializer<>(), config);
        AckFuture barFuture = writer.writeEvent("Bar", 1, "Bar");
        assertTrue(barFuture.isDone());
        assertFalse(barFuture.get());
        writer.close();
    }
    
}

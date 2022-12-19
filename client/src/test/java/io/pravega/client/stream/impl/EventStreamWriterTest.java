/**
 * Copyright Pravega Authors.
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.segment.impl.EndOfSegmentException;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentOutputStreamFactory;
import io.pravega.client.segment.impl.SegmentSealedException;
import io.pravega.client.segment.impl.SegmentTruncatedException;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.mock.MockSegmentIoStreams;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.common.util.ReusableLatch;
import io.pravega.shared.protocol.netty.ConnectionFailedException;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.CollectingExecutor;
import io.pravega.test.common.LeakDetectorTestSuite;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.junit.Test;
import org.mockito.Mockito;

import static io.pravega.test.common.AssertExtensions.assertBlocks;
import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

public class EventStreamWriterTest extends LeakDetectorTestSuite {
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
        MockSegmentIoStreams outputStream = new MockSegmentIoStreams(segment, null);
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment), any(), any(), any())).thenReturn(outputStream);
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream, "id", controller, streamFactory,
                new JavaSerializer<>(), config, executorService(), executorService(), null);
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

    @Test
    public void testWriteEvents() {
        String scope = "scope1";
        String streamName = "stream1";
        StreamImpl stream = new StreamImpl(scope, streamName);
        Segment segment = new Segment(scope, streamName, 0);
        EventWriterConfig config = EventWriterConfig.builder().build();
        SegmentOutputStreamFactory streamFactory = Mockito.mock(SegmentOutputStreamFactory.class);
        Controller controller = Mockito.mock(Controller.class);
        Mockito.when(controller.getCurrentSegments(scope, streamName)).thenReturn(getSegmentsFuture(segment));
        MockSegmentIoStreams outputStream = new MockSegmentIoStreams(segment, null);
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment), any(), any(), any())).thenReturn(outputStream);
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream, "id", controller, streamFactory,
                new JavaSerializer<>(), config, executorService(), executorService(), null);
        writer.writeEvents("1", Lists.newArrayList("Foo", "Bar")).join();
        writer.writeEvent("1", "Foo2").join();
        writer.close();
    }

    private StreamSegments getSegments(Segment segment) {
        NavigableMap<Double, SegmentWithRange> segments = new TreeMap<>();
        segments.put(1.0, new SegmentWithRange(segment, 0.0, 1.0));
        return new StreamSegments(segments);
    }
    
    private CompletableFuture<StreamSegments> getSegmentsFuture(Segment segment) {
        return CompletableFuture.completedFuture(getSegments(segment));
    }
    
    private CompletableFuture<StreamSegmentsWithPredecessors> getReplacement(Segment old, Segment repacement) {
        Map<SegmentWithRange, List<Long>> segments = new HashMap<>();
        segments.put(new SegmentWithRange(repacement, 0, 1), Collections.singletonList(old.getSegmentId()));
        return CompletableFuture.completedFuture(new StreamSegmentsWithPredecessors(segments, ""));
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
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment), any(), any(), any())).thenReturn(outputStream);
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream, "id", controller, streamFactory,
                new JavaSerializer<>(), config, executorService(), executorService(), null);
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
    public static final class FakeSegmentOutputStream implements SegmentOutputStream {
        final ArrayList<PendingEvent> unacked = new ArrayList<>();
        private final Segment segment;
        private Consumer<Segment> callBackForSealed;
        private final ArrayList<PendingEvent> acked = new ArrayList<>();
        private boolean sealed = false;
 
        private ByteBuffer getAcked(int index) {
            PendingEvent event = acked.get(index);
            if (event == null) {
                return null;
            }
            return event.getData().slice().skipBytes(WireCommands.TYPE_PLUS_LENGTH_SIZE).nioBuffer();
        }
        
        private ByteBuffer getUnacked(int index) {
            PendingEvent event = unacked.get(index);
            if (event == null) {
                return null;
            }
            return event.getData().slice().skipBytes(WireCommands.TYPE_PLUS_LENGTH_SIZE).nioBuffer();
        }
        
        void invokeSealedCallBack() {
            if (callBackForSealed != null) {
                callBackForSealed.accept(segment);
            }
        }

        @Override
        public void write(PendingEvent event) {
            unacked.add(event);
        }

        @Override
        public void close() throws SegmentSealedException {
            flush();
        }

        @Override
        public void flush() throws SegmentSealedException  {
            if (!sealed) {
                acked.addAll(unacked);
                unacked.clear();
            } else {
                throw new SegmentSealedException("Segment already sealed");
            }
        }

        @Override
        public void flushAsync() {
            //Noting to do.
        }

        @Override
        public List<PendingEvent> getUnackedEventsOnSeal() {
            sealed = true;
            return Collections.unmodifiableList(unacked);
        }

        @Override
        public String getSegmentName() {
            return segment.getScopedName();
        }

        @Override
        public long getLastObservedWriteOffset() {
            long result = 0;
            for (PendingEvent value : unacked) {
                result += value.getData().capacity();
            }
            return result;
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
        public void flushAsync() {
            //Noting to do.
        }

        @Override
        public List<PendingEvent> getUnackedEventsOnSeal() {
            return Collections.unmodifiableList(writes);
        }

        @Override
        public String getSegmentName() {
            return segment.getScopedName();
        }

        @Override
        public long getLastObservedWriteOffset() {
            return -1;
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

        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment1), any(), any(), any())).thenAnswer(i -> {
            outputStream1.callBackForSealed = i.getArgument(1);
            return outputStream1;
        });
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment2), any(), any(), any())).thenAnswer(i -> {
            outputStream2.callBackForSealed = i.getArgument(1);
            return outputStream2;
        });

        JavaSerializer<String> serializer = new JavaSerializer<>();
        Mockito.when(controller.getCurrentSegments(scope, streamName))
               .thenReturn(getSegmentsFuture(segment1))
               .thenReturn(getSegmentsFuture(segment2));
        @Cleanup
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream, "id", controller, streamFactory, serializer,
                config, executorService(), executorService(), null);

        writer.writeEvent(routingKey, "Foo");

        Mockito.when(controller.getCurrentSegments(scope, streamName)).thenReturn(getSegmentsFuture(segment2));
        Mockito.when(controller.getSuccessors(segment1)).thenReturn(getReplacement(segment1, segment2));

        //invoke the sealed callback invocation simulating a netty call back with segment sealed exception.
        outputStream1.invokeSealedCallBack();

        writer.writeEvent(routingKey, "Bar");
        Mockito.verify(controller, Mockito.times(1)).getCurrentSegments(any(), any());

        assertEquals(2, outputStream2.acked.size());
        assertEquals(1, outputStream2.unacked.size());
        assertEquals("Foo", serializer.deserialize(outputStream2.getAcked(0)));
        assertEquals("Bar", serializer.deserialize(outputStream2.getUnacked(0)));
    }

    @Test
    public void testResendFailedUponStreamSealed() {
        String scope = "scope";
        String streamName = "stream";
        String routingKey = "RoutingKey";
        StreamImpl stream = new StreamImpl(scope, streamName);
        Segment segment1 = new Segment(scope, streamName, 0);
        EventWriterConfig config = EventWriterConfig.builder().build();
        SegmentOutputStreamFactory streamFactory = Mockito.mock(SegmentOutputStreamFactory.class);
        Controller controller = Mockito.mock(Controller.class);

        FakeSegmentOutputStream outputStream1 = new FakeSegmentOutputStream(segment1);
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment1), any(), any(), any())).thenAnswer(i -> {
            outputStream1.callBackForSealed = i.getArgument(1);
            return outputStream1;
        });

        JavaSerializer<String> serializer = new JavaSerializer<>();
        val empty = CompletableFuture.completedFuture(new StreamSegments(new TreeMap<>()));

        Mockito.when(controller.getCurrentSegments(scope, streamName))
                .thenReturn(getSegmentsFuture(segment1))
                .thenReturn(empty);
        @Cleanup
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream, "id", controller, streamFactory, serializer,
                config, executorService(), executorService(), null);
        //When stream is not seal then we are able to write to the stream.
        writer.writeEvent(routingKey, "Foo");
        writer.writeEvent(routingKey, "Bar");

        Mockito.when(controller.getCurrentSegments(scope, streamName)).thenReturn(empty);
        val noFutures = CompletableFuture.completedFuture(new StreamSegmentsWithPredecessors(new HashMap<>(), ""));
        Mockito.when(controller.getSuccessors(segment1)).thenReturn(noFutures);

        assertEquals(2, outputStream1.unacked.size());
        assertEquals(0, outputStream1.acked.size());
        val pendingEvents1 = outputStream1.getUnackedEventsOnSeal();
        //invoke the sealed callback invocation simulating a netty call back with segment sealed exception.
        outputStream1.sealed = true;
        outputStream1.invokeSealedCallBack();

        // If stream is sealed then all the pending event should complete exceptionally.
        pendingEvents1.forEach(p -> p.getAckFuture().completeExceptionally(new IllegalStateException()));
        assertThrows("Stream should throw IllegalStateException", () -> writer.writeEvent(routingKey, "foo"), e -> e.getClass().equals(IllegalStateException.class));
        assertThrows("Stream should be sealed", () -> writer.writeEvent(routingKey, "Bar"), e -> e.getMessage().contains("sealed"));
        writer.close();
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

        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment1), any(), any(), any()))
                .thenAnswer(i -> {
                    outputStream1.callBackForSealed = i.getArgument(1);
                    return outputStream1;
                });
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment2), any(), any(), any()))
                .thenAnswer(i -> {
                    outputStream2.callBackForSealed = i.getArgument(1);
                    return outputStream2;
                });

        JavaSerializer<String> serializer = new JavaSerializer<>();
        Mockito.when(controller.getCurrentSegments(scope, streamName))
                .thenReturn(getSegmentsFuture(segment1))
                .thenReturn(getSegmentsFuture(segment2));
        @Cleanup
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream, "id", controller, streamFactory, serializer,
                config, executorService(), executorService(), null);

        writer.writeEvent(routingKey, "Foo");

        Mockito.when(controller.getCurrentSegments(scope, streamName)).thenReturn(getSegmentsFuture(segment2));
        Mockito.when(controller.getSuccessors(segment1)).thenReturn(getReplacement(segment1, segment2));

        writer.writeEvent(routingKey, "Bar");
        Mockito.verify(controller, Mockito.times(1)).getCurrentSegments(any(), any());

        assertEquals(2, outputStream1.unacked.size());
        assertEquals("Foo", serializer.deserialize(outputStream1.getUnacked(0)));
        assertEquals("Bar", serializer.deserialize(outputStream1.getUnacked(1)));

        outputStream1.invokeSealedCallBack(); // simulate a segment sealed callback.
        writer.writeEvent(routingKey, "TestData");
        //This time the actual handleLogSealed is invoked and the resend method resends data to outputStream2.
        assertEquals(3, outputStream2.acked.size());
        assertEquals("Foo", serializer.deserialize(outputStream2.getAcked(0)));
        assertEquals("Bar", serializer.deserialize(outputStream2.getAcked(1)));
        assertEquals(1, outputStream2.unacked.size());
        assertEquals("TestData", serializer.deserialize(outputStream2.getUnacked(0)));

    }

    @Test
    public void testNoNextSegment() {
        String scope = "scope";
        String streamName = "stream";
        String routingKey = "RoutingKey";
        StreamImpl stream = new StreamImpl(scope, streamName);
        Segment segment1 = new Segment(scope, streamName, 0);
        EventWriterConfig config = EventWriterConfig.builder().build();
        SegmentOutputStreamFactory streamFactory = Mockito.mock(SegmentOutputStreamFactory.class);
        Controller controller = Mockito.mock(Controller.class);

        FakeSegmentOutputStream outputStream1 = new FakeSegmentOutputStream(segment1);

        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment1), any(), any(), any())).thenAnswer(i -> {
            outputStream1.callBackForSealed = i.getArgument(1);
            return outputStream1;
        });

        JavaSerializer<String> serializer = new JavaSerializer<>();
        val empty = CompletableFuture.completedFuture(new StreamSegments(new TreeMap<>()));
        Mockito.when(controller.getCurrentSegments(scope, streamName))
               .thenReturn(getSegmentsFuture(segment1))
               .thenReturn(empty);
        @Cleanup
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream, "id", controller, streamFactory, serializer,
                config, executorService(), executorService(), null);

        writer.writeEvent(routingKey, "Foo");

        Mockito.when(controller.getCurrentSegments(scope, streamName)).thenReturn(empty);
        val noFutures = CompletableFuture.completedFuture(new StreamSegmentsWithPredecessors(new HashMap<>(), ""));
        Mockito.when(controller.getSuccessors(segment1)).thenReturn(noFutures);

        //invoke the sealed callback invocation simulating a netty call back with segment sealed exception.
        outputStream1.sealed = true;
        assertBlocks(() -> writer.flush(), () -> outputStream1.invokeSealedCallBack());

        assertThrows("Stream should be sealed", () -> writer.writeEvent(routingKey, "Bar"), e -> e.getMessage().contains("sealed"));
        writer.close();
    }
    
    @Test
    public void testNoNextSegmentMidClose() {
        String scope = "scope";
        String streamName = "stream";
        String routingKey = "RoutingKey";
        StreamImpl stream = new StreamImpl(scope, streamName);
        Segment segment1 = new Segment(scope, streamName, 0);
        EventWriterConfig config = EventWriterConfig.builder().build();
        SegmentOutputStreamFactory streamFactory = Mockito.mock(SegmentOutputStreamFactory.class);
        Controller controller = Mockito.mock(Controller.class);

        FakeSegmentOutputStream outputStream1 = new FakeSegmentOutputStream(segment1);

        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment1), any(), any(), any())).thenAnswer(i -> {
            outputStream1.callBackForSealed = i.getArgument(1);
            return outputStream1;
        });

        JavaSerializer<String> serializer = new JavaSerializer<>();
        val empty = CompletableFuture.completedFuture(new StreamSegments(new TreeMap<>()));
        Mockito.when(controller.getCurrentSegments(scope, streamName))
               .thenReturn(getSegmentsFuture(segment1))
               .thenReturn(empty);
        @Cleanup
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream, "id", controller, streamFactory, serializer,
                config, executorService(), executorService(), null);

        CompletableFuture<Void> future = writer.writeEvent(routingKey, "Foo");

        Mockito.when(controller.getCurrentSegments(scope, streamName)).thenReturn(empty);
        val noFutures = CompletableFuture.completedFuture(new StreamSegmentsWithPredecessors(new HashMap<>(), ""));
        Mockito.when(controller.getSuccessors(segment1)).thenReturn(noFutures);

        //invoke the sealed callback invocation simulating a netty call back with segment sealed exception.
        outputStream1.sealed = true;
        assertBlocks(() -> writer.close(), () -> outputStream1.invokeSealedCallBack());
        assertTrue(future.isDone());
        assertTrue(future.isCompletedExceptionally());
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
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment), any(), any(), any())).thenReturn(outputStream);

        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream, "id", controller, streamFactory, serializer,
                config, executorService(), executorService(), null);
        writer.writeEvent("Foo");
        Mockito.verify(controller).getCurrentSegments(any(), any());
        assertTrue(outputStream.unacked.size() > 0);
        writer.flush();
        assertTrue(outputStream.unacked.isEmpty());
    }
    
    @Test
    public void testSealInvokesFlush() {
        String scope = "scope";
        String streamName = "stream";
        StreamImpl stream = new StreamImpl(scope, streamName);
        Segment segment1 = new Segment(scope, streamName, 0);
        Segment segment2 = new Segment(scope, streamName, 1);
        EventWriterConfig config = EventWriterConfig.builder().build();

        SegmentOutputStreamFactory streamFactory = Mockito.mock(SegmentOutputStreamFactory.class);
        Controller controller = Mockito.mock(Controller.class);
        FakeSegmentOutputStream outputStream1 = new FakeSegmentOutputStream(segment1);
        FakeSegmentOutputStream outputStream2 = new FakeSegmentOutputStream(segment2);
        Mockito.when(controller.getCurrentSegments(scope, streamName))
                .thenReturn(getSegmentsFuture(segment1));
        Mockito.when(controller.getSuccessors(segment1)).thenReturn(getReplacement(segment1, segment2));
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment1), any(), any(), any())).thenAnswer(i -> {
            outputStream1.callBackForSealed = i.getArgument(1);
            return outputStream1;
        });
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment2), any(), any(), any())).thenAnswer(i -> {
            outputStream2.callBackForSealed = i.getArgument(1);
            return outputStream2;
        });
        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream, "id", controller, streamFactory, serializer,
                config, executorService(), executorService(), null);
        writer.writeEvent("Foo");
        Mockito.verify(controller).getCurrentSegments(any(), any());
        assertEquals(1, outputStream1.unacked.size());
        assertEquals(0, outputStream1.acked.size());
        outputStream1.invokeSealedCallBack();
        assertEquals(0, outputStream2.unacked.size());
        assertEquals(2, outputStream2.acked.size());
    }

    @Test
    public void testSealInvokesFlushError() throws SegmentSealedException {
        String scope = "scope";
        String streamName = "stream";
        StreamImpl stream = new StreamImpl(scope, streamName);
        Segment segment1 = new Segment(scope, streamName, 0);
        Segment segment2 = new Segment(scope, streamName, 1);
        EventWriterConfig config = EventWriterConfig.builder().build();
        // Setup Mocks.
        SegmentOutputStreamFactory streamFactory = Mockito.mock(SegmentOutputStreamFactory.class);
        Controller controller = Mockito.mock(Controller.class);
        FakeSegmentOutputStream outputStream1 = new FakeSegmentOutputStream(segment1);
        SegmentOutputStream outputStream2 = Mockito.mock(SegmentOutputStream.class);
        // Simulate a connection failure for inflight event re-write on segment2
        RetriesExhaustedException connectionFailure = new RetriesExhaustedException(new ConnectionFailedException("Connection drop"));
        Mockito.doAnswer(args -> {
            PendingEvent event = args.getArgument(0);
            if (event.getAckFuture() != null) {
                event.getAckFuture().completeExceptionally(connectionFailure);
            }
            return null;
        }).when(outputStream2).write(any());
        // A flush will throw a RetriesExhausted exception.
        Mockito.doThrow(connectionFailure).when(outputStream2).flush();

        // Enable mocks for the controller calls.
        Mockito.when(controller.getCurrentSegments(scope, streamName))
                .thenReturn(getSegmentsFuture(segment1));
        // Successor of segment1 is segment2
        Mockito.when(controller.getSuccessors(segment1)).thenReturn(getReplacement(segment1, segment2));
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment1), any(), any(), any())).thenAnswer(i -> {
            outputStream1.callBackForSealed = i.getArgument(1);
            return outputStream1;
        });
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment2), any(), any(), any())).thenReturn(outputStream2);

        // Start of test
        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream, "id", controller, streamFactory, serializer,
                config, executorService(), executorService(), null);
        // Write an event to the stream.
        CompletableFuture<Void> writerFuture = writer.writeEvent("Foo");
        Mockito.verify(controller).getCurrentSegments(any(), any());
        assertEquals(1, outputStream1.unacked.size());
        assertEquals(0, outputStream1.acked.size());
        // Simulate segment1 being sealed, now the writer will fetch its successor, segment2, from the Controller
        outputStream1.invokeSealedCallBack();
        // Verify that the inflight event which is written to segment2 due to sealed segment fails incase of a connection failure.
        AssertExtensions.assertThrows(RetriesExhaustedException.class, () -> Futures.getThrowingException(writerFuture));
        // Verify that a flush() does indicate this failure.
        AssertExtensions.assertThrows(RetriesExhaustedException.class, () -> writer.flush());
    }

    @Test
    public void testSegmentSealedInFlush() throws EndOfSegmentException, SegmentTruncatedException {
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
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment1), any(), any(), any()))
                .thenAnswer(i -> {
                    outputStream.callBackForSealed = i.getArgument(1);
                    return outputStream;
                });
        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream, "id", controller, streamFactory, serializer,
                config, executorService(), executorService(), null);
        writer.writeEvent("Foo");
        Mockito.verify(controller).getCurrentSegments(any(), any());
        assertTrue(outputStream.unacked.size() > 0);

        MockSegmentIoStreams outputStream2 = new MockSegmentIoStreams(segment2, null);
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment2), any(), any(), any())).thenReturn(outputStream2);
        outputStream.invokeSealedCallBack();

        writer.flush();

        Mockito.verify(controller, Mockito.times(1)).getCurrentSegments(any(), any());
        assertTrue(outputStream2.fetchCurrentSegmentLength().join() > 0);
        assertEquals(serializer.serialize("Foo"), outputStream2.read());
    }

    @Test
    public void testRetryFlushSegmentSealed() throws EndOfSegmentException, SegmentTruncatedException {
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
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment1), any(), any(), any()))
                .thenAnswer(i -> {
                    outputStream.callBackForSealed = i.getArgument(1);
                    return outputStream;
                });
        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream, "id", controller, streamFactory, serializer,
                config, executorService(), executorService(), null);
        writer.writeEvent("Foo");
        Mockito.verify(controller).getCurrentSegments(any(), any());
        assertTrue(outputStream.getUnackedEventsOnSeal().size() > 0);

        MockSegmentIoStreams outputStream2 = new MockSegmentIoStreams(segment2, null);
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment2), any(), any(), any())).thenReturn(outputStream2);

        AssertExtensions.assertBlocks(() -> {
            writer.flush(); // blocking on flush.
        }, () -> {
            outputStream.releaseFlush(); // trigger release with a segmentSealedException.
            outputStream.invokeSealedCallBack(); // trigger Sealed Segment call back.
        });

        Mockito.verify(controller, Mockito.times(1)).getCurrentSegments(any(), any());
        assertTrue(outputStream2.fetchCurrentSegmentLength().join() > 0);
        assertEquals(serializer.serialize("Foo"), outputStream2.read());
    }

    /**
     * This tests lock release ordering. If a write happens after a flush starts, it should block on the flush while the seal proceeds.
     */
    @Test(timeout = 10000)
    public void testWriteBetweenSeals() throws EndOfSegmentException, SegmentTruncatedException {
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
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment1), any(), any(), any()))
                .thenAnswer(i -> {
                    outputStream.callBackForSealed = i.getArgument(1);
                    return outputStream;
                });
        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream, "id", controller, streamFactory, serializer,
                config, executorService(), executorService(), null);
        writer.writeEvent("Foo");
        Mockito.verify(controller).getCurrentSegments(any(), any());

        MockSegmentIoStreams outputStream2 = new MockSegmentIoStreams(segment2, null);
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment2), any(), any(), any())).thenReturn(outputStream2);

        AssertExtensions.assertBlocks(() -> {
            writer.flush(); // blocking on flush.
        }, () -> {
            AssertExtensions.assertBlocks(() -> {
                writer.writeEvent("foo");
            }, () -> {
                outputStream.releaseFlush(); // trigger release with a segmentSealedException.
                outputStream.invokeSealedCallBack(); // trigger Sealed Segment call back.
            });
        });

        Mockito.verify(controller, Mockito.times(1)).getCurrentSegments(any(), any());
        assertTrue(outputStream2.fetchCurrentSegmentLength().join() > 0);
        assertEquals(serializer.serialize("Foo"), outputStream2.read());
    }
    
    @Test
    public void testRetryCloseSegmentSealed() throws EndOfSegmentException, SegmentTruncatedException {
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
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment1), any(), any(), any()))
                .thenAnswer(i -> {
                    outputStream.callBackForSealed = i.getArgument(1);
                    return outputStream;
                });
        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream, "id", controller, streamFactory, serializer,
                config, executorService(), executorService(), null);
        writer.writeEvent("Foo");
        Mockito.verify(controller).getCurrentSegments(any(), any());
        assertTrue(outputStream.getUnackedEventsOnSeal().size() > 0);

        MockSegmentIoStreams outputStream2 = new MockSegmentIoStreams(segment2, null);
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment2), any(), any(), any())).thenReturn(outputStream2);

        AssertExtensions.assertBlocks(() -> {
            writer.close(); // closed invokes flush internally; this call is blocking on flush.
        }, () -> {
            outputStream.releaseFlush(); // trigger release with a segmentSealedException.
            outputStream.invokeSealedCallBack(); // trigger Sealed Segment call back.
        });

        Mockito.verify(controller, Mockito.times(1)).getCurrentSegments(any(), any());
        assertTrue(outputStream2.fetchCurrentSegmentLength().join() > 0);
        assertTrue(outputStream2.isClosed());
        //the connection to outputStream is closed with the failConnection during SegmentSealed Callback.
        assertEquals(serializer.serialize("Foo"), outputStream2.read());
    }

    @Test
    public void testSegmentSealedInClose() throws EndOfSegmentException, SegmentTruncatedException {
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
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment1), any(), any(), any()))
                .thenAnswer(i -> {
                    outputStream1.callBackForSealed = i.getArgument(1);
                    return outputStream1;
                });
        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream, "id", controller, streamFactory, serializer,
                config, executorService(), executorService(), null);
        writer.writeEvent("Foo");
        Mockito.verify(controller).getCurrentSegments(any(), any());
        assertTrue(outputStream1.unacked.size() > 0);

        MockSegmentIoStreams outputStream2 = new MockSegmentIoStreams(segment2, null);
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment2), any(), any(), any())).thenReturn(outputStream2);
        outputStream1.invokeSealedCallBack();

        writer.close();

        Mockito.verify(controller, Mockito.times(1)).getCurrentSegments(any(), any());
        assertTrue(outputStream2.fetchCurrentSegmentLength().join() > 0);
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

        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment1), any(), any(), any()))
                .thenAnswer(i -> {
                    outputStream1.callBackForSealed = i.getArgument(1);
                    return outputStream1;
                });
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment2), any(), any(), any()))
                .thenAnswer(i -> {
                    outputStream2.callBackForSealed = i.getArgument(1);
                    return outputStream2;
                });
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment3), any(), any(), any()))
                .thenAnswer(i -> {
                    outputStream3.callBackForSealed = i.getArgument(1);
                    return outputStream3;
                });
        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream, "id", controller, streamFactory, serializer,
                config, executorService(), executorService(), null);
        writer.writeEvent(routingKey, "Foo");
        Mockito.verify(controller).getCurrentSegments(any(), any());
        assertEquals(1, outputStream1.unacked.size());
        assertTrue(outputStream2.getUnackedEventsOnSeal().isEmpty());

        outputStream1.invokeSealedCallBack();
        outputStream2.invokeSealedCallBack();

        writer.writeEvent(routingKey, "Bar");

        Mockito.verify(controller, Mockito.times(1)).getCurrentSegments(any(), any());

        assertEquals(0, outputStream2.acked.size());
        assertEquals(2, outputStream2.unacked.size());
        assertEquals("Foo", serializer.deserialize(outputStream2.getUnacked(0)));
        assertEquals(3, outputStream3.acked.size());
        assertEquals(1, outputStream3.unacked.size());
        assertEquals("Foo", serializer.deserialize(outputStream3.getAcked(0)));
        assertEquals("Bar", serializer.deserialize(outputStream3.getUnacked(0)));
    }
    
    @Test
    public void testNoteTime() {
        String scope = "scope";
        String streamName = "stream";
        StreamImpl stream = new StreamImpl(scope, streamName);
        Segment segment1 = new Segment(scope, streamName, 0);
        EventWriterConfig config = EventWriterConfig.builder().build();

        SegmentOutputStream mockOutputStream = Mockito.mock(SegmentOutputStream.class);       
        SegmentOutputStreamFactory streamFactory = Mockito.mock(SegmentOutputStreamFactory.class);
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment1), any(), any(), any())).thenReturn(mockOutputStream);
        Controller controller = Mockito.mock(Controller.class);
        Mockito.when(controller.getCurrentSegments(scope, streamName)).thenReturn(getSegmentsFuture(segment1));
        
        Mockito.when(mockOutputStream.getLastObservedWriteOffset()).thenReturn(1111L);
        
        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream, "id", controller, streamFactory, serializer,
                config, executorService(), executorService(), null);
        writer.noteTime(123);
        
        ImmutableMap<Segment, Long> expectedOffsets = ImmutableMap.of(segment1, 1111L);
        Mockito.verify(controller).noteTimestampFromWriter(eq("id"), eq(stream), eq(123L), eq(new WriterPosition(expectedOffsets)));
    }
    
    @Test
    public void testNoteTimeFail() {
        String scope = "scope";
        String streamName = "stream";
        StreamImpl stream = new StreamImpl(scope, streamName);
        Segment segment1 = new Segment(scope, streamName, 0);
        EventWriterConfig config = EventWriterConfig.builder().automaticallyNoteTime(true).build();

        SegmentOutputStream mockOutputStream = Mockito.mock(SegmentOutputStream.class);       
        SegmentOutputStreamFactory streamFactory = Mockito.mock(SegmentOutputStreamFactory.class);
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment1), any(), any(), any())).thenReturn(mockOutputStream);
        Controller controller = Mockito.mock(Controller.class);
        Mockito.when(controller.getCurrentSegments(scope, streamName)).thenReturn(getSegmentsFuture(segment1));

        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream, "id", controller, streamFactory, serializer,
                config, executorService(), executorService(), null);
        AssertExtensions.assertThrows(IllegalStateException.class, () -> writer.noteTime(123));       
    }
    
    @Test
    public void testAutoNoteTime() {
        String scope = "scope";
        String streamName = "stream";
        StreamImpl stream = new StreamImpl(scope, streamName);
        Segment segment1 = new Segment(scope, streamName, 0);
        EventWriterConfig config = EventWriterConfig.builder().automaticallyNoteTime(true).build();

        SegmentOutputStream mockOutputStream = Mockito.mock(SegmentOutputStream.class);       
        SegmentOutputStreamFactory streamFactory = Mockito.mock(SegmentOutputStreamFactory.class);
        Mockito.when(streamFactory.createOutputStreamForSegment(eq(segment1), any(), any(), any())).thenReturn(mockOutputStream);
        Controller controller = Mockito.mock(Controller.class);
        Mockito.when(controller.getCurrentSegments(scope, streamName)).thenReturn(getSegmentsFuture(segment1));
        
        Mockito.when(mockOutputStream.getLastObservedWriteOffset()).thenReturn(1111L);
        
        CollectingExecutor executor = new CollectingExecutor();
        
        JavaSerializer<String> serializer = new JavaSerializer<>();
        @Cleanup
        EventStreamWriter<String> writer = new EventStreamWriterImpl<>(stream, "id", controller, streamFactory, serializer,
                config, executor, executor, null);
        
        List<Runnable> tasks = executor.getScheduledTasks();
        assertEquals(1, tasks.size());
        tasks.get(0).run();
        
        ImmutableMap<Segment, Long> expectedOffsets = ImmutableMap.of(segment1, 1111L);
        Mockito.verify(controller).noteTimestampFromWriter(eq("id"), eq(stream), Mockito.anyLong(), eq(new WriterPosition(expectedOffsets)));
    }
    
}

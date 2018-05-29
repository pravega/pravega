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

import com.google.common.collect.ImmutableMap;
import io.pravega.client.segment.impl.AsyncSegmentEventReader;
import io.pravega.client.segment.impl.AsyncSegmentEventReaderFactory;
import io.pravega.client.segment.impl.EndOfSegmentException;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentMetadataClient;
import io.pravega.client.segment.impl.SegmentMetadataClientFactory;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentSealedException;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import lombok.SneakyThrows;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Stubber;

import static com.google.common.base.Preconditions.checkState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class EventStreamReaderTest {
    private static final Segment S0 = Segment.fromScopedName("Foo/Bar/0");
    private static final Segment S1 = Segment.fromScopedName("Foo/Bar/1");

    private final Consumer<Segment> segmentSealedCallback = segment -> { };
    private final EventWriterConfig writerConfig = EventWriterConfig.builder().build();

    // region Lifecycle

    @Test
    public void testClose() throws Exception {
        Context context = new Context();
        EventStreamReaderImpl<byte[]> reader = context.createEventStreamReader();

        // add a segment reader to ensure that it is closed
        AsyncSegmentEventReader sr0 = context.createSegmentReader(S0);
        context.acquire(S0);
        // read an event to ensure that the segment reader is acquired
        doReturn(result(value(1))).doReturn(result(eos())).when(sr0).readAsync();
        reader.readNextEvent(0);
        // verify close
        reader.close();
        verify(context.groupState).readerShutdown(any());
        verify(sr0).close();
        verify(context.groupState).close();

    }
    // endregion
//
//    @Test(timeout = 10000)
//    public void testEndOfSegmentWithoutSuccessors() throws SegmentSealedException, ReinitializationRequiredException {
//        AtomicLong clock = new AtomicLong();
//        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
//        ReaderGroupStateManager groupState = mock(ReaderGroupStateManager.class);
//        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(segmentStreamFactory, segmentStreamFactory,
//                                                                           new ByteArraySerializer(), groupState,
//                                                                           clock::get,
//                                                                           ReaderConfig.builder().build());
//        Segment segment = Segment.fromScopedName("Foo/Bar/0");
//        Mockito.when(groupState.acquireNewSegmentsIfNeeded(0L))
//               .thenReturn(ImmutableMap.of(segment, 0L))
//               .thenReturn(Collections.emptyMap());
//        SegmentOutputStream stream = segmentStreamFactory.createOutputStreamForSegment(segment, segmentSealedCallback, writerConfig, "");
//        ByteBuffer buffer = writeInt(stream, 1);
//        EventRead<byte[]> read = reader.readNextEvent(0);
//        byte[] event = read.getEvent();
//        assertEquals(buffer, ByteBuffer.wrap(event));
//        read = reader.readNextEvent(0);
//        assertNull(read.getEvent());
//        read = reader.readNextEvent(0);
//        assertNull(read.getEvent());
//        assertEquals(0, reader.getReaders().size());
//        reader.close();
//    }

//    @SuppressWarnings("unchecked")
//    @Test(timeout = 10000)
//    public void testReadWithEndOfSegmentException() throws Exception {
//        AtomicLong clock = new AtomicLong();
//        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
//
//        //Prep the mocks.
//        ReaderGroupStateManager groupState  = mock(ReaderGroupStateManager.class);
//
//        //Mock for the two SegmentInputStreams.
//        Segment segment = Segment.fromScopedName("Foo/Bar/0");
//        SegmentInputStream segmentInputStream1 = mock(SegmentInputStream.class);
//        Mockito.when(segmentInputStream1.read(anyLong())).thenThrow(new EndOfSegmentException(EndOfSegmentException.ErrorType.END_OFFSET_REACHED));
//        Mockito.when(segmentInputStream1.getSegmentId()).thenReturn(segment);
//
//        SegmentInputStream segmentInputStream2 = mock(SegmentInputStream.class);
//        SegmentOutputStream stream = segmentStreamFactory.createOutputStreamForSegment(segment, segmentSealedCallback, writerConfig, "");
//        ByteBuffer buffer = writeInt(stream, 1);
//        Mockito.when(segmentInputStream2.read(anyLong())).thenReturn(buffer);
//        Mockito.when(segmentInputStream2.getSegmentId()).thenReturn(Segment.fromScopedName("Foo/test/0"));
//        Mockito.when(segmentInputStream2.getOffset()).thenReturn(10L);
//
//        SegmentInputStreamFactory inputStreamFactory = mock(SegmentInputStreamFactory.class);
//        Mockito.when(inputStreamFactory.createInputStreamForSegment(any(Segment.class), anyLong())).thenReturn(segmentInputStream1);
//        Mockito.when(orderer.nextSegment(any(List.class))).thenReturn(segmentInputStream1).thenReturn(segmentInputStream2);
//
//
//        @Cleanup
//        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(inputStreamFactory, segmentStreamFactory,
//                new ByteArraySerializer(), groupState,
//                 clock::get,
//                ReaderConfig.builder().build());
//
//        EventRead<byte[]> event = reader.readNextEvent(100L);
//        //Validate that segmentInputStream1.close() is invoked on reaching endOffset.
//        verify(segmentInputStream1, Mockito.times(1)).close();
//        //Validate that groupState.handleEndOfSegment method is invoked.
//        verify(groupState, Mockito.times(1)).handleEndOfSegment(segment, false);
//    }

    @Test
    public void testReadNextEvent() throws Exception {
        Context context = new Context();
        EventStreamReaderImpl<byte[]> reader = context.createEventStreamReader();

        // provide a segment to be read
        AsyncSegmentEventReader sr0 = context.createSegmentReader(S0);
        AsyncSegmentEventReader sr1 = context.createSegmentReader(S1);
        context.acquire(S0);

        final CompletableFuture<ByteBuffer> r1 = new CompletableFuture<>();
        final CompletableFuture<ByteBuffer> r2 = new CompletableFuture<>();
        final CompletableFuture<ByteBuffer> r3 = result(value(2));

        doReturn(r1).doReturn(r2).doReturn(result(eos())).when(sr0).readAsync();
        doReturn(r3).doReturn(result(eos())).when(sr1).readAsync();

        // async value
        context.whenPolled(() -> r1.complete(value(1)));
        assertEquals(value(1), ByteBuffer.wrap(reader.readNextEvent(0).getEvent()));

        // timeout
        assertNull(reader.readNextEvent(0).getEvent());

        // end-of-segment (leading to timeout due to lack of acquired successor)
        r2.completeExceptionally(eos());
        assertNull(reader.readNextEvent(0).getEvent());
        verify(sr0).close();

        // value from next segment
        context.acquire(S1);
        assertEquals(value(2), ByteBuffer.wrap(reader.readNextEvent(0).getEvent()));
    }

//
//    @Test(timeout = 10000)
//    public void testReleaseSegment() throws SegmentSealedException, ReinitializationRequiredException {
//        AtomicLong clock = new AtomicLong();
//        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
//        ReaderGroupStateManager groupState = mock(ReaderGroupStateManager.class);
//        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(segmentStreamFactory, segmentStreamFactory,
//                                                                           new ByteArraySerializer(), groupState,
//                                                                           clock::get,
//                                                                           ReaderConfig.builder().build());
//        Segment segment1 = Segment.fromScopedName("Foo/Bar/0");
//        Segment segment2 = Segment.fromScopedName("Foo/Bar/1");
//        Mockito.when(groupState.acquireNewSegmentsIfNeeded(0L))
//               .thenReturn(ImmutableMap.of(segment1, 0L, segment2, 0L))
//               .thenReturn(Collections.emptyMap());
//        SegmentOutputStream stream1 = segmentStreamFactory.createOutputStreamForSegment(segment1, segmentSealedCallback, writerConfig, "");
//        SegmentOutputStream stream2 = segmentStreamFactory.createOutputStreamForSegment(segment2, segmentSealedCallback, writerConfig, "");
//        writeInt(stream1, 1);
//        writeInt(stream2, 2);
//        reader.readNextEvent(0);
//        List<SegmentInputStream> readers = reader.getReaders();
//        assertEquals(2, readers.size());
//        Assert.assertEquals(segment1, readers.get(0).getSegmentId());
//        Assert.assertEquals(segment2, readers.get(1).getSegmentId());
//
//        Mockito.when(groupState.getCheckpoint()).thenReturn("checkpoint");
//        assertTrue(reader.readNextEvent(0).isCheckpoint());
//        Mockito.when(groupState.getCheckpoint()).thenReturn(null);
//        Mockito.when(groupState.findSegmentToReleaseIfRequired()).thenReturn(segment2);
//        Mockito.when(groupState.releaseSegment(Mockito.eq(segment2), anyLong(), anyLong())).thenReturn(true);
//        assertFalse(reader.readNextEvent(0).isCheckpoint());
//        verify(groupState).releaseSegment(Mockito.eq(segment2), anyLong(), anyLong());
//        readers = reader.getReaders();
//        assertEquals(1, readers.size());
//        Assert.assertEquals(segment1, readers.get(0).getSegmentId());
//        reader.close();
//    }

    private ByteBuffer writeInt(SegmentOutputStream stream, int value) throws SegmentSealedException {
        ByteBuffer buffer = ByteBuffer.allocate(4).putInt(value);
        buffer.flip();
        stream.write(new PendingEvent(null, buffer, new CompletableFuture<Void>()));
        return buffer;
    }
//
//    @SuppressWarnings("unused")
//    @Test(timeout = 10000)
//    public void testAcquireSegment() throws SegmentSealedException, ReinitializationRequiredException {
//        AtomicLong clock = new AtomicLong();
//        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
//        ReaderGroupStateManager groupState = mock(ReaderGroupStateManager.class);
//        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(segmentStreamFactory, segmentStreamFactory,
//                                                                           new ByteArraySerializer(), groupState,
//                                                                           clock::get,
//                                                                           ReaderConfig.builder().build());
//        Segment segment1 = Segment.fromScopedName("Foo/Bar/0");
//        Segment segment2 = Segment.fromScopedName("Foo/Bar/1");
//        Mockito.when(groupState.acquireNewSegmentsIfNeeded(0L))
//               .thenReturn(ImmutableMap.of(segment1, 0L))
//               .thenReturn(ImmutableMap.of(segment2, 0L))
//               .thenReturn(Collections.emptyMap());
//        SegmentOutputStream stream1 = segmentStreamFactory.createOutputStreamForSegment(segment1, segmentSealedCallback, writerConfig, "");
//        SegmentOutputStream stream2 = segmentStreamFactory.createOutputStreamForSegment(segment2, segmentSealedCallback, writerConfig, "");
//        writeInt(stream1, 1);
//        writeInt(stream1, 2);
//        writeInt(stream2, 3);
//        writeInt(stream2, 4);
//        reader.readNextEvent(0);
//        List<SegmentInputStream> readers = reader.getReaders();
//        assertEquals(1, readers.size());
//        Assert.assertEquals(segment1, readers.get(0).getSegmentId());
//
//        reader.readNextEvent(0);
//        readers = reader.getReaders();
//        assertEquals(2, readers.size());
//        Assert.assertEquals(segment1, readers.get(0).getSegmentId());
//        Assert.assertEquals(segment2, readers.get(1).getSegmentId());
//        reader.close();
//    }
//
//    @Test
//    public void testEventPointer() throws SegmentSealedException, NoSuchEventException, ReinitializationRequiredException {
//        AtomicLong clock = new AtomicLong();
//        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
//        ReaderGroupStateManager groupState = mock(ReaderGroupStateManager.class);
//        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(segmentStreamFactory, segmentStreamFactory,
//                                                                           new ByteArraySerializer(), groupState,
//                                                                           clock::get,
//                                                                           ReaderConfig.builder().build());
//        Segment segment = Segment.fromScopedName("Foo/Bar/0");
//        Mockito.when(groupState.acquireNewSegmentsIfNeeded(0L)).thenReturn(ImmutableMap.of(segment, 0L)).thenReturn(Collections.emptyMap());
//        SegmentOutputStream stream = segmentStreamFactory.createOutputStreamForSegment(segment, segmentSealedCallback, writerConfig, "");
//        ByteBuffer buffer1 = writeInt(stream, 1);
//        ByteBuffer buffer2 = writeInt(stream, 2);
//        ByteBuffer buffer3 = writeInt(stream, 3);
//        EventRead<byte[]> event1 = reader.readNextEvent(0);
//        EventRead<byte[]> event2 = reader.readNextEvent(0);
//        EventRead<byte[]> event3 = reader.readNextEvent(0);
//        assertEquals(buffer1, ByteBuffer.wrap(event1.getEvent()));
//        assertEquals(buffer2, ByteBuffer.wrap(event2.getEvent()));
//        assertEquals(buffer3, ByteBuffer.wrap(event3.getEvent()));
//        assertNull(reader.readNextEvent(0).getEvent());
//        assertEquals(buffer1, ByteBuffer.wrap(reader.fetchEvent(event1.getEventPointer())));
//        assertEquals(buffer3, ByteBuffer.wrap(reader.fetchEvent(event3.getEventPointer())));
//        assertEquals(buffer2, ByteBuffer.wrap(reader.fetchEvent(event2.getEventPointer())));
//        reader.close();
//    }
//
//    @Test(timeout = 10000)
//    public void testCheckpoint() throws SegmentSealedException, ReinitializationRequiredException {
//        AtomicLong clock = new AtomicLong();
//        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
//        ReaderGroupStateManager groupState = mock(ReaderGroupStateManager.class);
//        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(segmentStreamFactory, segmentStreamFactory,
//                                                                           new ByteArraySerializer(), groupState,
//                                                                           clock::get,
//                                                                           ReaderConfig.builder().build());
//        Segment segment = Segment.fromScopedName("Foo/Bar/0");
//        Mockito.when(groupState.acquireNewSegmentsIfNeeded(0L)).thenReturn(ImmutableMap.of(segment, 0L)).thenReturn(Collections.emptyMap());
//        SegmentOutputStream stream = segmentStreamFactory.createOutputStreamForSegment(segment, segmentSealedCallback, writerConfig, "");
//        ByteBuffer buffer = writeInt(stream, 1);
//        Mockito.when(groupState.getCheckpoint()).thenReturn("Foo").thenReturn(null);
//        EventRead<byte[]> eventRead = reader.readNextEvent(0);
//        assertTrue(eventRead.isCheckpoint());
//        assertNull(eventRead.getEvent());
//        assertEquals("Foo", eventRead.getCheckpointName());
//        assertEquals(buffer, ByteBuffer.wrap(reader.readNextEvent(0).getEvent()));
//        assertNull(reader.readNextEvent(0).getEvent());
//        reader.close();
//    }
//
//    @Test(timeout = 10000)
//    public void testRestore() throws SegmentSealedException, ReinitializationRequiredException {
//        AtomicLong clock = new AtomicLong();
//        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
//        ReaderGroupStateManager groupState = mock(ReaderGroupStateManager.class);
//        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(segmentStreamFactory, segmentStreamFactory,
//                                                                           new ByteArraySerializer(), groupState,
//                                                                           clock::get,
//                                                                           ReaderConfig.builder().build());
//        Segment segment = Segment.fromScopedName("Foo/Bar/0");
//        Mockito.when(groupState.acquireNewSegmentsIfNeeded(0L)).thenReturn(ImmutableMap.of(segment, 0L)).thenReturn(Collections.emptyMap());
//        SegmentOutputStream stream = segmentStreamFactory.createOutputStreamForSegment(segment, segmentSealedCallback, writerConfig, "");
//        ByteBuffer buffer = writeInt(stream, 1);
//        Mockito.when(groupState.getCheckpoint()).thenThrow(new ReinitializationRequiredException());
//        try {
//            reader.readNextEvent(0);
//            fail();
//        } catch (ReinitializationRequiredException e) {
//            // expected
//        }
//        assertTrue(reader.getReaders().isEmpty());
//        reader.close();
//    }
//
//    @Test(timeout = 10000)
//    public void testDataTruncated() throws SegmentSealedException, ReinitializationRequiredException {
//        AtomicLong clock = new AtomicLong();
//        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
//        ReaderGroupStateManager groupState = mock(ReaderGroupStateManager.class);
//        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(segmentStreamFactory, segmentStreamFactory,
//                                                                           new ByteArraySerializer(), groupState,
//                                                                           clock::get,
//                                                                           ReaderConfig.builder().build());
//        Segment segment = Segment.fromScopedName("Foo/Bar/0");
//        Mockito.when(groupState.acquireNewSegmentsIfNeeded(0L))
//               .thenReturn(ImmutableMap.of(segment, 0L))
//               .thenReturn(Collections.emptyMap());
//        SegmentOutputStream stream = segmentStreamFactory.createOutputStreamForSegment(segment, segmentSealedCallback,
//                                                                                       writerConfig, "");
//        SegmentMetadataClient metadataClient = segmentStreamFactory.createSegmentMetadataClient(segment, "");
//        ByteBuffer buffer1 = writeInt(stream, 1);
//        ByteBuffer buffer2 = writeInt(stream, 2);
//        ByteBuffer buffer3 = writeInt(stream, 3);
//        long length = metadataClient.fetchCurrentSegmentLength();
//        assertEquals(0, length % 3);
//        EventRead<byte[]> event1 = reader.readNextEvent(0);
//        assertEquals(buffer1, ByteBuffer.wrap(event1.getEvent()));
//        metadataClient.truncateSegment(segment, length / 3);
//        assertEquals(buffer2, ByteBuffer.wrap(reader.readNextEvent(0).getEvent()));
//        metadataClient.truncateSegment(segment, length);
//        ByteBuffer buffer4 = writeInt(stream, 4);
//        AssertExtensions.assertThrows(TruncatedDataException.class, () -> reader.readNextEvent(0));
//        assertEquals(buffer4, ByteBuffer.wrap(reader.readNextEvent(0).getEvent()));
//        assertNull(reader.readNextEvent(0).getEvent());
//        AssertExtensions.assertThrows(NoSuchEventException.class, () -> reader.fetchEvent(event1.getEventPointer()));
//        reader.close();
//    }
//

    /**
     * Creates a mock segment event reader for the given segment.
     *
     * @param segment the segment to be associated with the reader.
     * @param addToFactory the mock factory to add the reader to.
     * @return the mock reader.
     */
    @Deprecated
    private AsyncSegmentEventReader mockSegmentEventReader(Segment segment, AsyncSegmentEventReaderFactory addToFactory) {
        AsyncSegmentEventReader r = mock(AsyncSegmentEventReader.class);
        doReturn(segment).when(r).getSegmentId();
        doReturn(r).when(addToFactory).createEventReaderForSegment(segment, anyLong(), anyInt());
        return r;
    }

    private ByteBuffer value(int value) {
        ByteBuffer buffer = ByteBuffer.allocate(4).putInt(value);
        buffer.flip();
        return buffer;
    }

    private EndOfSegmentException eos() {
        return new EndOfSegmentException();
    }

    private CompletableFuture<ByteBuffer> result(ByteBuffer buffer) {
        return CompletableFuture.completedFuture(buffer);
    }

    private CompletableFuture<ByteBuffer> result(Exception e) {
        CompletableFuture promise = new CompletableFuture<>();
        promise.completeExceptionally(e);
        return promise;
    }

    private static class Context implements AsyncSegmentEventReaderFactory, SegmentMetadataClientFactory {

        final AtomicLong clock = new AtomicLong();
        final ReaderGroupStateManager groupState = mock(ReaderGroupStateManager.class);
        @SuppressWarnings("unchecked")
        final BlockingQueue<EventStreamReaderImpl.AcquiredReader> readCompletionQueue = spy(new LinkedBlockingQueue());
        private final Map<Segment, AsyncSegmentEventReader> readers = new HashMap<>();
        private final Map<Segment, SegmentMetadataClient> metadataClients = new HashMap<>();

        public Context() {
            doReturn("reader-1").when(groupState).getReaderId();
        }

        @Override
        public AsyncSegmentEventReader createEventReaderForSegment(Segment segment, long endOffset, int bufferSize) {
            checkState(readers.containsKey(segment));
            return readers.get(segment);
        }

        @Override
        public SegmentMetadataClient createSegmentMetadataClient(Segment segment, String delegationToken) {
            checkState(metadataClients.containsKey(segment));
            return metadataClients.get(segment);
        }

        /**
         * Creates a mock segment event reader for the given segment.
         *
         * @param segment the segment to be associated with the reader.
         * @return the mock reader.
         */
        public AsyncSegmentEventReader createSegmentReader(Segment segment) {
            return createSegmentReader(segment, 0L, Long.MAX_VALUE);
        }

        /**
         * Creates a mock segment event reader for the given segment.
         *
         * @param segment the segment to be associated with the reader.
         * @return the mock reader.
         */
        public AsyncSegmentEventReader createSegmentReader(Segment segment, long startOffset, long endOffset) {
            AsyncSegmentEventReader reader = mock(AsyncSegmentEventReader.class);
            doReturn(segment).when(reader).getSegmentId();
            doReturn(startOffset).when(reader).getOffset();
            // TODO support endOffset
            doAnswer(i -> {
                doReturn(true).when(reader).isClosed();
                return null;
            }).when(reader).close();
            readers.put(segment, reader);
            return reader;
        }

        /**
         * Creates a mock metadata client for the given segment.
         * @param segment the segment to be associated with the metadata client.
         * @return the mock metadata client.
         */
        public SegmentMetadataClient createSegmentMetadataClient(Segment segment) {
            SegmentMetadataClient client = mock(SegmentMetadataClient.class);
            metadataClients.put(segment, client);
            return client;
        }

        /**
         * Creates an event stream reader for test purposes.
         * @return the reader.
         */
        public EventStreamReaderImpl<byte[]> createEventStreamReader() {
            EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(this, this,
                    new ByteArraySerializer(), groupState, clock::get, ReaderConfig.builder().build(), readCompletionQueue);
            return reader;
        }

        /**
         * Convenience method for acquiring a sequence of segments (one per call to {@code acquireNewSegmentsIfNeeded}).
         * Be sure to create the associated reader for a given segment before invoking this method.
         *
         * @param segment a segment to acquire.
         * @param extraSegments a sequence of subsequent segments to acquire.
         */
        @SneakyThrows
        public void acquire(Segment segment, Segment... extraSegments) {
            Stubber stub = doReturn(acquired(segment));
            for (Segment s : extraSegments) {
                stub = stub.doReturn(acquired(s));
            }
            stub.doReturn(Collections.emptyMap()).when(this.groupState).acquireNewSegmentsIfNeeded(0L);
        }

        private Map<Segment, Long> acquired(Segment s) {
            checkState(readers.containsKey(s));
            return ImmutableMap.of(s, readers.get(s).getOffset());
        }

        /**
         * Registers a callback handler for the {@code poll} method of the {@code readCompletionQueue}.
         * The callback will be invoked before the real {@code poll} method.  Subsequent call will
         * directly invoke the real method.
         */
        @SneakyThrows
        public void whenPolled(Runnable callable) {
            doAnswer(i -> {
                callable.run();
                return i.callRealMethod();
            }).doCallRealMethod().when(readCompletionQueue).poll(anyLong(), any());
        }

        /**
         * Registers a callback handler for the {@code poll} method of the {@code readCompletionQueue}.
         * The callback will be invoked in lieu of the real {@code poll} method.
         */
        @SneakyThrows
        public void onPoll(Callable<EventStreamReaderImpl.AcquiredReader> callable) {
            doAnswer(i -> callable.call()).doCallRealMethod().when(readCompletionQueue).poll(anyLong(), any());
        }
    }
}

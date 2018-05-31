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
import io.pravega.client.segment.impl.NoSuchEventException;
import io.pravega.client.segment.impl.NoSuchSegmentException;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentMetadataClient;
import io.pravega.client.segment.impl.SegmentMetadataClientFactory;
import io.pravega.client.segment.impl.SegmentTruncatedException;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.ReaderConfig;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.test.common.AssertExtensions;
import lombok.SneakyThrows;
import org.junit.Test;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.Stubber;

import static com.google.common.base.Preconditions.checkState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class EventStreamReaderTest {
    private static final Segment S0 = Segment.fromScopedName("Foo/Bar/0");
    private static final Segment S1 = Segment.fromScopedName("Foo/Bar/1");

    private static final int INT_EVENT_LENGTH = WireCommands.TYPE_PLUS_LENGTH_SIZE + Integer.BYTES;
    private static final long EVENT_POINTER_0 = 42L * INT_EVENT_LENGTH;

    @Test
    public void testClose() throws Exception {
        Context context = new Context();
        EventStreamReaderImpl<byte[]> reader = context.createEventStreamReader();

        // assign a segment and then read an event to ensure that a reader is acquired and closed
        AsyncSegmentEventReader sr0 = context.registerSegmentReader(S0);
        context.acquire(S0);
        doAnswer(readResult(value(1))).doAnswer(readResult(eos())).when(sr0).readAsync();
        reader.readNextEvent(0);

        // verify close
        reader.close();
        verify(context.groupState).readerShutdown(any());
        verify(sr0).close();
        verify(context.groupState).close();
    }

    @Test
    public void testReadNextEvent() throws Exception {
        Context context = new Context();
        EventStreamReaderImpl<byte[]> reader = context.createEventStreamReader();

        // provide a segment to be read
        AsyncSegmentEventReader sr0 = context.registerSegmentReader(S0);
        AsyncSegmentEventReader sr1 = context.registerSegmentReader(S1);
        context.acquire(S0);

        final CompletableFuture<ByteBuffer> r1 = new CompletableFuture<>();
        final CompletableFuture<ByteBuffer> r2 = new CompletableFuture<>();
        final CompletableFuture<ByteBuffer> r3 = CompletableFuture.completedFuture(value(2));

        doAnswer(readResult(r1)).doAnswer(readResult(r2)).doAnswer(readResult(eos())).when(sr0).readAsync();
        doAnswer(readResult(r3)).doAnswer(readResult(eos())).when(sr1).readAsync();

        // async value
        context.whenPolled(() -> r1.complete(value(1)));
        EventRead<byte[]> eventRead = reader.readNextEvent(0);
        assertEquals(value(1), ByteBuffer.wrap(eventRead.getEvent()));
        assertEquals(new EventPointerImpl(S0, 0, INT_EVENT_LENGTH), eventRead.getEventPointer().asImpl());
        assertEquals(Collections.singletonMap(S0, (long) INT_EVENT_LENGTH), eventRead.getPosition().asImpl().getOwnedSegmentsWithOffsets());

        // timeout
        assertTrue(isTimeout(reader.readNextEvent(0)));

        // end-of-segment (leading to timeout due to lack of acquired successor)
        r2.completeExceptionally(eos());
        assertTrue(isTimeout(reader.readNextEvent(0)));
        verify(context.groupState).handleEndOfSegment(S0, true);
        verify(sr0).close();

        // value from next segment
        context.acquire(S1);
        assertEquals(value(2), ByteBuffer.wrap(reader.readNextEvent(0).getEvent()));
    }

    @Test
    public void testReadPosition() throws Exception {
        Context context = new Context();
        EventStreamReaderImpl<byte[]> reader = context.createEventStreamReader();
        AsyncSegmentEventReader sr0 = context.registerSegmentReader(S0);
        AsyncSegmentEventReader sr1 = context.registerSegmentReader(S1);
        context.acquire(S0, S1);

        // observable position should advance on calls to readNextEvent (not the underlying async reader)
        doAnswer(readResult(value(0))).doAnswer(readResult(eos())).when(sr0).readAsync();
        doAnswer(readResult(value(1))).doAnswer(readResult(eos())).when(sr1).readAsync();
        EventRead<byte[]> eventRead = reader.readNextEvent(0);
        assertEquals(new EventPointerImpl(S0, 0, INT_EVENT_LENGTH), eventRead.getEventPointer().asImpl());
        assert INT_EVENT_LENGTH == sr1.getOffset();
        assertEquals(ImmutableMap.of(S0, (long) INT_EVENT_LENGTH, S1, 0L), eventRead.getPosition().asImpl().getOwnedSegmentsWithOffsets());
    }

    @Test
    public void testAcquire() throws Exception {
        Context context = new Context();
        EventStreamReaderImpl<byte[]> reader = context.createEventStreamReader();

        // acquire a segment without completing the read (to avoid mutating the initial state)
        AsyncSegmentEventReader sr0 = context.registerSegmentReader(S0, EVENT_POINTER_0, EVENT_POINTER_0 + INT_EVENT_LENGTH);
        context.acquire(S0);
        final CompletableFuture<ByteBuffer> r1 = new CompletableFuture<>();
        doAnswer(readResult(r1)).when(sr0).readAsync();
        assertTrue("expected a timeout", isTimeout(reader.readNextEvent(0)));

        // verify the acquired reader state
        assertEquals(1, reader.getReaders().size());
        EventStreamReaderImpl.AcquiredReader acquiredReader = reader.getReaders().get(0);
        assertEquals(EVENT_POINTER_0, acquiredReader.getReadOffset());
        assertFalse(acquiredReader.outstandingRead.isDone());
    }

    @Test
    public void testRelease() throws Exception {
        Context context = new Context();
        EventStreamReaderImpl<byte[]> reader = context.createEventStreamReader();
        AsyncSegmentEventReader sr0 = context.registerSegmentReader(S0);
        context.acquire(S0);

        // schedule an async read to a segment
        final CompletableFuture<ByteBuffer> r1 = new CompletableFuture<>();
        doAnswer(readResult(r1)).doAnswer(readResult(eos())).when(sr0).readAsync();
        assertTrue("expected a timeout", isTimeout(reader.readNextEvent(0)));

        // use a checkpoint to schedule a release
        context.checkpoint(null); // use a checkpoint to schedule a release
        assertTrue("expected a checkpoint", reader.readNextEvent(0).isCheckpoint());

        // schedule the release of the segment and verify that the released position is accurate
        // note: the completion state of the async read should not affect the position
        r1.complete(value(1));
        assert reader.getQueue().size() == 1;
        context.release(S0, pos -> {
            assertEquals("expected the release position to be invariant to the completion state", 0L, pos.longValue());
            return true;
        });
        assertTrue("expected a timeout", isTimeout(reader.readNextEvent(0)));
        verify(context.groupState).releaseSegment(any(), anyLong(), anyLong());
        assertEquals(0, reader.getReaders().size());
    }

    @Test
    public void testEventPointer() throws Exception {
        Context context = new Context();
        EventStreamReaderImpl<byte[]> reader = context.createEventStreamReader();
        AsyncSegmentEventReader sr0;
        EventPointerInternal pointer = new EventPointerImpl(S0, EVENT_POINTER_0, INT_EVENT_LENGTH);

        // verify reader initialization
        sr0 = context.registerSegmentReader(S0);
        doAnswer(readResult(value(1))).when(sr0).readAsync();
        reader.fetchEvent(pointer);
        verify(sr0).setOffset(pointer.getEventStartOffset());

        // verify value
        sr0 = context.registerSegmentReader(S0);
        doAnswer(readResult(value(1))).when(sr0).readAsync();
        assertEquals(value(1), ByteBuffer.wrap(reader.fetchEvent(pointer)));
        verify(sr0).close();

        // verify exception handling
        sr0 = context.registerSegmentReader(S0);
        doAnswer(readResult(eos())).when(sr0).readAsync();
        AssertExtensions.assertThrows(NoSuchEventException.class, () -> reader.fetchEvent(pointer));
        verify(sr0).close();
        sr0 = context.registerSegmentReader(S0);
        doAnswer(readResult(new NoSuchSegmentException(S0.getScopedName()))).when(sr0).readAsync();
        AssertExtensions.assertThrows(NoSuchEventException.class, () -> reader.fetchEvent(pointer));
        verify(sr0).close();
        sr0 = context.registerSegmentReader(S0);
        doAnswer(readResult(new SegmentTruncatedException(S0.getScopedName()))).when(sr0).readAsync();
        AssertExtensions.assertThrows(NoSuchEventException.class, () -> reader.fetchEvent(pointer));
        verify(sr0).close();
        sr0 = context.registerSegmentReader(S0);
        doAnswer(readResult(new IOException("expected"))).when(sr0).readAsync();
        AssertExtensions.assertThrows(IOException.class, () -> reader.fetchEvent(pointer));
        verify(sr0).close();
    }

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

    private <T> boolean isTimeout(EventRead<T> eventRead) {
        return eventRead.getEvent() == null && !eventRead.isCheckpoint();
    }

    private ByteBuffer value(int value) {
        ByteBuffer buffer = ByteBuffer.allocate(4).putInt(value);
        buffer.flip();
        return buffer;
    }

    private EndOfSegmentException eos() {
        return new EndOfSegmentException();
    }

    private Answer readResult(ByteBuffer buf) {
        return readResult(CompletableFuture.completedFuture(buf));
    }

    private Answer readResult(Exception e) {
        CompletableFuture<ByteBuffer> promise = new CompletableFuture<>();
        promise.completeExceptionally(e);
        return readResult(promise);
    }

    /**
     * Produce a mock response for {@code readAsync} that simulates the correct handling of the offset for a given future event.
     * @param future a future event.
     */
    private Answer readResult(final CompletableFuture<ByteBuffer> future) {
        return i -> {
            AsyncSegmentEventReader mock = (AsyncSegmentEventReader) i.getMock();
            final long offset = mock.getOffset();
            future.whenComplete((buf, th) -> {
                if (th == null) {
                    final long newOffset = offset + WireCommands.TYPE_PLUS_LENGTH_SIZE + buf.remaining();
                    doReturn(newOffset).when(mock).getOffset();
                }
            });
            return future;
        };
    }

    /**
     * A test context.
     */
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
         * Registers a mock segment event reader for the given segment.
         *
         * @param segment the segment to be associated with the reader.
         * @return the mock reader.
         */
        public AsyncSegmentEventReader registerSegmentReader(Segment segment) {
            return registerSegmentReader(segment, 0L, Long.MAX_VALUE);
        }

        /**
         * Registers a mock segment event reader for the given segment.
         * The mock reader simulates {@code getOffset}, {@code close}, and {@code isClosed}.
         *
         * @param segment the segment to be associated with the reader.
         * @return the mock reader.
         */
        public AsyncSegmentEventReader registerSegmentReader(Segment segment, long startOffset, long endOffset) {
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
         * Registers a mock metadata client for the given segment.
         * @param segment the segment to be associated with the metadata client.
         * @return the mock metadata client.
         */
        public SegmentMetadataClient registerSegmentMetadataClient(Segment segment) {
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
         * Convenience method for acquiring a set of segments on a subsequent call to {@code readNextEvent}.
         * Be sure to create the associated reader for a given segment before invoking this method.
         *
         * @param segments a set of segments to acquire.
         */
        @SneakyThrows
        public void acquire(Segment... segments) {
            Map<Segment, Long> map = new LinkedHashMap<>(segments.length);
            Arrays.stream(segments).map(this::acquired).forEach(map::putAll);
            doReturn(map).doReturn(Collections.emptyMap()).when(this.groupState).acquireNewSegmentsIfNeeded(0L);
        }

        private Map<Segment, Long> acquired(Segment s) {
            checkState(readers.containsKey(s));
            return ImmutableMap.of(s, readers.get(s).getOffset());
        }

        /**
         * Convenience method for releasing a segment on a subsequent call to {@code readNextEvent}.
         *
         * @param segment the segment to release.
         * @param releaseCallback the callback to be invoked when the reader releases the segment.
         */
        @SneakyThrows
        public void release(Segment segment, final Function<Long, Boolean> releaseCallback) {
            doReturn(segment).doReturn(null).when(groupState).findSegmentToReleaseIfRequired();
            doAnswer(i -> {
                if (releaseCallback != null) {
                    return releaseCallback.apply(i.getArgument(1));
                }
                return true;
            }).doReturn(true).when(groupState).releaseSegment(eq(segment), anyLong(), anyLong());
        }

        /**
         * Convenience method for preparing a checkpoint to be observed on a subsequent call to {@code readNextEvent}.
         *
         * @param checkpointCallback the callback to be invoked when the reader takes the checkpoint.
         */
        @SneakyThrows
        public String checkpoint(final Consumer<PositionInternal> checkpointCallback) {
            String checkpointName = UUID.randomUUID().toString();
            doReturn(checkpointName).doReturn(null).when(groupState).getCheckpoint();
            doAnswer(i -> {
                if (checkpointCallback != null) {
                    checkpointCallback.accept(i.getArgument(1));
                }
                return null;
            }).doNothing().when(groupState).checkpoint(eq(checkpointName), any());
            return checkpointName;
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
    }
}

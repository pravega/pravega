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
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.batch.impl.SegmentRangeImpl;
import io.pravega.client.segment.impl.AsyncSegmentEventReader;
import io.pravega.client.segment.impl.AsyncSegmentEventReaderFactory;
import io.pravega.client.segment.impl.EndOfSegmentException;
import io.pravega.client.segment.impl.NoSuchEventException;
import io.pravega.client.segment.impl.NoSuchSegmentException;
import io.pravega.client.segment.impl.EndOfSegmentException.ErrorType;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentInfo;
import io.pravega.client.segment.impl.SegmentMetadataClient;
import io.pravega.client.segment.impl.SegmentMetadataClientFactory;
import io.pravega.client.segment.impl.SegmentTruncatedException;
import io.pravega.client.stream.EventPointer;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.ReaderConfig;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.TruncatedDataException;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.test.common.AssertExtensions;
import lombok.SneakyThrows;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

import static com.google.common.base.Preconditions.checkState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class EventStreamReaderTest {
    private static final Segment S0 = Segment.fromScopedName("Foo/Bar/0");
    private static final Segment S1 = Segment.fromScopedName("Foo/Bar/1");

    private static final int INT_EVENT_LENGTH = WireCommands.TYPE_PLUS_LENGTH_SIZE + Integer.BYTES;

    private static final EventPointerImpl S0_PTR_A = new EventPointerImpl(S0, 42L * INT_EVENT_LENGTH, INT_EVENT_LENGTH);
    private static final EventPointerImpl S1_PTR_B = new EventPointerImpl(S1, 101L * INT_EVENT_LENGTH, INT_EVENT_LENGTH);

    private static final SegmentRange S0_RANGE_A = range(S0_PTR_A);
    private static final SegmentRange S1_RANGE_B = range(S1_PTR_B);

    @Test
    public void testClose() throws Exception {
        Context context = new Context();
        EventStreamReaderImpl<byte[]> reader = context.createEventStreamReader();

        // assign a segment and then read an event to ensure that a reader is acquired and closed
        AsyncSegmentEventReader sr0 = context.registerSegmentReader(S0);
        context.acquire(S0);
        doAnswer(readResult(value(1))).doAnswer(readResult(eos())).when(sr0).readAsync(anyLong());
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

        final CompletableFuture<ByteBuffer> r1 = CompletableFuture.completedFuture(value(1));
        final CompletableFuture<ByteBuffer> r2 = new CompletableFuture<>();
        final CompletableFuture<ByteBuffer> r3 = CompletableFuture.completedFuture(value(2));

        doAnswer(readResult(r1)).doAnswer(readResult(r2)).doAnswer(readResult(eos())).when(sr0).readAsync(anyLong());
        doAnswer(readResult(r3)).doAnswer(readResult(eos())).when(sr1).readAsync(anyLong());

        // async value
        EventRead<byte[]> eventRead = reader.readNextEvent(0);
        assertEquals(value(1), ByteBuffer.wrap(eventRead.getEvent()));

        // timeout (due to slow segment read)
        assertTrue(isTimeout(reader.readNextEvent(0)));

        // end-of-segment (leading to timeout due to lack of acquired successor)
        r2.completeExceptionally(eos());
        assertTrue(isTimeout(reader.readNextEvent(0)));
        verify(context.groupState).handleEndOfSegment(S0, true);
        verify(sr0).close();

        // value from next segment
        context.acquire(S1);
        assertEquals(value(2), ByteBuffer.wrap(reader.readNextEvent(0).getEvent()));

        // timeout (no more data)
        assertTrue(isTimeout(reader.readNextEvent(0)));
        verify(sr1).close();
    }

    @Test
    public void testReadPosition() throws Exception {
        Context context = new Context();
        EventStreamReaderImpl<byte[]> reader = context.createEventStreamReader();
        AsyncSegmentEventReader sr0 = context.registerSegmentReader(S0);
        AsyncSegmentEventReader sr1 = context.registerSegmentReader(S1);
        context.acquire(S0, S1);

        // observable position should advance on calls to readNextEvent (not the underlying async reader)
        doAnswer(readResult(value(0))).doAnswer(readResult(eos())).when(sr0).readAsync(anyLong());
        doAnswer(readResult(value(1))).doAnswer(readResult(eos())).when(sr1).readAsync(anyLong());
        EventRead<byte[]> eventRead = reader.readNextEvent(0);
        assertEquals(new EventPointerImpl(S0, 0, INT_EVENT_LENGTH), eventRead.getEventPointer().asImpl());
        assumeTrue("assume that a completed read is in the queue", reader.getQueue().size() == 2);
        assertEquals(ImmutableMap.of(S0, (long) INT_EVENT_LENGTH, S1, 0L), eventRead.getPosition().asImpl().getOwnedSegmentsWithOffsets());
    }

    @Test
    public void testReadBounded() throws Exception {
        Context context = new Context();
        EventStreamReaderImpl<byte[]> reader = context.createEventStreamReader();
        AsyncSegmentEventReader sr0 = context.registerSegmentReader(S0);
        AsyncSegmentEventReader sr1 = context.registerSegmentReader(S1);
        context.acquire(range(S0_PTR_A), range(S1_PTR_B));

        doAnswer(readResult(value(1))).doAnswer(readResult(eos())).when(sr0).readAsync(anyLong());
        doAnswer(readResult(value(2))).doAnswer(readResult(eos())).when(sr1).readAsync(anyLong());

        // verify that readNextEvent respects configured start/end offset
        EventRead<byte[]> eventRead;
        eventRead = reader.readNextEvent(0);
        verify(context.groupState).handleEndOfSegment(S0, false);
        assertEquals(S0_PTR_A, eventRead.getEventPointer());
        assertEquals(value(1), ByteBuffer.wrap(eventRead.getEvent()));

        // verify that readNextEvent moves to the next segment
        eventRead = reader.readNextEvent(0);
        verify(context.groupState).handleEndOfSegment(S1, false);
        assertEquals(S1_PTR_B, eventRead.getEventPointer());
        assertEquals(value(2), ByteBuffer.wrap(eventRead.getEvent()));

        eventRead = reader.readNextEvent(0);
        assertNull(eventRead.getEvent());
    }

    @Test
    public void testDataTruncated() throws Exception {
        Context context = new Context();
        EventStreamReaderImpl<byte[]> reader = context.createEventStreamReader();

        // prepare to observe two truncations: one due to a non-existent segment,
        // and a second due to data being cutoff before a certain offset.
        SegmentMetadataClient metadataClient0 = context.registerSegmentMetadataClient(S0);
        SegmentMetadataClient metadataClient1 = context.registerSegmentMetadataClient(S1);
        doThrow(new NoSuchSegmentException(S0.getScopedName())).when(metadataClient0).getSegmentInfo();
        SegmentInfo segmentInfo1 = new SegmentInfo(S1, S1_RANGE_B.getStartOffset(), S1_RANGE_B.getEndOffset(), true, 0L);
        doReturn(segmentInfo1).when(metadataClient1).getSegmentInfo();
        AsyncSegmentEventReader sr0 = context.registerSegmentReader(S0);
        AsyncSegmentEventReader sr1 = context.registerSegmentReader(S1);
        doAnswer(readResult(new SegmentTruncatedException())).when(sr0).readAsync(anyLong());
        doAnswer(readResult(new SegmentTruncatedException())).doAnswer(readResult(value(1))).doAnswer(readResult(eos())).when(sr1).readAsync(anyLong());
        context.acquire(S0, S1);

        // observe the first TruncatedDataException
        AssertExtensions.assertThrows(TruncatedDataException.class, () -> reader.readNextEvent(0));
        verify(sr0).readAsync(0L);
        verify(context.groupState).handleEndOfSegment(S0, true);
        verify(sr0).close();

        // observe the second TruncatedDataException
        AssertExtensions.assertThrows(TruncatedDataException.class, () -> reader.readNextEvent(0));
        verify(sr1).readAsync(0L);

        // observe a value at the revised offset
        EventRead<byte[]> eventRead = reader.readNextEvent(0);
        verify(sr1).readAsync(segmentInfo1.getStartingOffset());
        assertEquals(S1_PTR_B, eventRead.getEventPointer());
        assertEquals(value(1), ByteBuffer.wrap(eventRead.getEvent()));
    }

    @Test
    public void testAcquire() throws Exception {
        Context context = new Context();
        EventStreamReaderImpl<byte[]> reader = context.createEventStreamReader();

        // prepare to acquire a segment without completing any reads (to avoid mutating the initial state)
        AsyncSegmentEventReader sr0 = context.registerSegmentReader(S0);
        context.acquire(S0_RANGE_A);
        final CompletableFuture<ByteBuffer> r1 = new CompletableFuture<>();
        doAnswer(readResult(r1)).when(sr0).readAsync(anyLong());

        // read to drive acquisition
        EventRead<byte[]> eventRead = reader.readNextEvent(0);
        assertTrue("expected a timeout", isTimeout(eventRead));

        // verify the acquired reader state
        verify(context.groupState).acquireNewSegmentsIfNeeded(anyLong());
        assertEquals(1, reader.getReaders().size());
        verify(sr0).readAsync(S0_RANGE_A.getStartOffset());
        EventStreamReaderImpl.ReaderState readerState = reader.getReaders().get(0);
        assertEquals(S0_RANGE_A.getStartOffset(), readerState.getReadOffset());
        assertEquals(S0_RANGE_A.getEndOffset(), readerState.getEndOffset());
        assertFalse(readerState.outstandingRead.isDone());
    }

    @Test
    public void testRelease() throws Exception {
        Context context = new Context();
        EventStreamReaderImpl<byte[]> reader = context.createEventStreamReader();
        AsyncSegmentEventReader sr0 = context.registerSegmentReader(S0);
        context.acquire(S0);

        // schedule an async read to a segment
        final CompletableFuture<ByteBuffer> r1 = new CompletableFuture<>();
        doAnswer(readResult(r1)).doAnswer(readResult(eos())).when(sr0).readAsync(anyLong());
        assertTrue("expected a timeout", isTimeout(reader.readNextEvent(0)));

        // use a checkpoint to schedule a release
        context.checkpoint(null); // use a checkpoint to schedule a release
        assertTrue("expected a checkpoint", reader.readNextEvent(0).isCheckpoint());

        // schedule the release of the segment and verify that the released position is accurate
        // note: the completion state of the async read should not affect the position
        r1.complete(value(1));
        assumeTrue("assume that a completed read is in the queue", reader.getQueue().size() == 1);
        context.release(S0, null);
        reader.readNextEvent(0);
        ArgumentCaptor<Long> positionCaptor = ArgumentCaptor.forClass(Long.class);
        verify(context.groupState).releaseSegment(any(), positionCaptor.capture(), anyLong());
        assertEquals("expected the release position to be invariant to the completion state", 0L, positionCaptor.getValue().longValue());
        verify(sr0).close();
        assertEquals(0, reader.getReaders().size());
    }

    @Test
    public void testEventPointer() throws Exception {
        Context context = new Context();
        EventStreamReaderImpl<byte[]> reader = context.createEventStreamReader();
        AsyncSegmentEventReader sr0;

        // verify value
        sr0 = context.registerSegmentReader(S0);
        doAnswer(readResult(value(1))).when(sr0).readAsync(anyLong());
        assertEquals(value(1), ByteBuffer.wrap(reader.fetchEvent(S0_PTR_A)));
        verify(sr0).readAsync(S0_PTR_A.getEventStartOffset());
        verify(sr0).close();

        // verify exception handling
        sr0 = context.registerSegmentReader(S0);
        doAnswer(readResult(eos())).when(sr0).readAsync(anyLong());
        AssertExtensions.assertThrows(NoSuchEventException.class, () -> reader.fetchEvent(S0_PTR_A));
        verify(sr0).close();
        sr0 = context.registerSegmentReader(S0);
        doAnswer(readResult(new NoSuchSegmentException(S0.getScopedName()))).when(sr0).readAsync(anyLong());
        AssertExtensions.assertThrows(NoSuchEventException.class, () -> reader.fetchEvent(S0_PTR_A));
        verify(sr0).close();
        sr0 = context.registerSegmentReader(S0);
        doAnswer(readResult(new SegmentTruncatedException(S0.getScopedName()))).when(sr0).readAsync(anyLong());
        AssertExtensions.assertThrows(NoSuchEventException.class, () -> reader.fetchEvent(S0_PTR_A));
        verify(sr0).close();
        sr0 = context.registerSegmentReader(S0);
        doAnswer(readResult(new IOException("expected"))).when(sr0).readAsync(anyLong());
        AssertExtensions.assertThrows(IOException.class, () -> reader.fetchEvent(S0_PTR_A));
        verify(sr0).close();
    }

    @Test
    public void testCheckpoint() throws Exception {
        Context context = new Context();
        EventStreamReaderImpl<byte[]> reader = context.createEventStreamReader();
        AsyncSegmentEventReader sr0 = context.registerSegmentReader(S0);
        context.acquire(S0);

        // enqueue an async read to ensure that S1 was acquired
        CompletableFuture<ByteBuffer> r1 = new CompletableFuture<>();
        doAnswer(readResult(r1)).doAnswer(readResult(eos())).when(sr0).readAsync(anyLong());
        assertTrue(isTimeout(reader.readNextEvent(0)));
        verify(sr0).readAsync(0L);

        // schedule a checkpoint and then complete the future to enqueue a value
        String checkpointName = context.checkpoint(null);
        r1.complete(value(1));
        assumeTrue("assume that the results were queued", reader.getQueue().size() == 1);

        // complete the checkpoint (which should leave the value unprocessed)
        EventRead<byte[]> eventRead = reader.readNextEvent(0);

        // verify the event
        assertTrue("expected a checkpoint", eventRead.isCheckpoint());
        assertEquals(checkpointName, eventRead.getCheckpointName());
        assertEquals("expected the position to be zero (because the event hasn't been consumed yet)",
                ImmutableMap.of(S0, 0L), eventRead.getPosition().asImpl().getOwnedSegmentsWithOffsets());

        // verify the reader group state
        ArgumentCaptor<PositionInternal> positionCaptor = ArgumentCaptor.forClass(PositionInternal.class);
        verify(context.groupState).checkpoint(eq(checkpointName), positionCaptor.capture());
        assertEquals("expected the position to be zero (because the event hasn't been consumed yet)",
                ImmutableMap.of(S0, 0L), positionCaptor.getValue().getOwnedSegmentsWithOffsets());
    }

    @Test
    public void testRestore() throws Exception {
        Context context = new Context();
        EventStreamReaderImpl<byte[]> reader = context.createEventStreamReader();

        doThrow(new ReinitializationRequiredException()).when(context.groupState).getCheckpoint();
        AssertExtensions.assertThrows(ReinitializationRequiredException.class, () -> reader.readNextEvent(0));
        verify(context.groupState).close();
    }

    private static <T> boolean isTimeout(EventRead<T> eventRead) {
        return eventRead.getEvent() == null && !eventRead.isCheckpoint();
    }

    private static ByteBuffer value(int value) {
        ByteBuffer buffer = ByteBuffer.allocate(4).putInt(value);
        buffer.flip();
        return buffer;
    }

    private static EndOfSegmentException eos() {
        return eos(ErrorType.END_OF_SEGMENT_REACHED);
    }

    private static EndOfSegmentException eos(ErrorType errorType) {
        return new EndOfSegmentException(errorType);
    }

    private static Answer readResult(ByteBuffer buf) {
        return readResult(CompletableFuture.completedFuture(buf));
    }

    private static Answer readResult(Exception e) {
        CompletableFuture<ByteBuffer> promise = new CompletableFuture<>();
        promise.completeExceptionally(e);
        return readResult(promise);
    }

    /**
     * Produce a mock response for {@code readAsync} that returns the given future event.
     * @param future a future event.
     */
    private static Answer readResult(final CompletableFuture<ByteBuffer> future) {
        return i -> future;
    }

    private static SegmentRange range(EventPointer eventPointer) {
        EventPointerInternal p = eventPointer.asImpl();
        return SegmentRangeImpl.builder().segment(p.getSegment()).startOffset(p.getEventStartOffset()).endOffset(p.getEventStartOffset() + p.getEventLength()).build();
    }

    /**
     * A test context.
     */
    private static class Context implements AsyncSegmentEventReaderFactory, SegmentMetadataClientFactory {

        final AtomicLong clock = new AtomicLong();
        final ReaderGroupStateManager groupState = mock(ReaderGroupStateManager.class);
        @SuppressWarnings("unchecked")
        final BlockingQueue<EventStreamReaderImpl.ReaderState> readCompletionQueue = spy(new LinkedBlockingQueue());
        private final Map<Segment, AsyncSegmentEventReader> readers = new HashMap<>();
        private final Map<Segment, SegmentMetadataClient> metadataClients = new HashMap<>();

        public Context() {
            doReturn("reader-1").when(groupState).getReaderId();
            doReturn("token-1").when(groupState).getLatestDelegationToken();
        }

        @Override
        public AsyncSegmentEventReader createEventReaderForSegment(Segment segment, int bufferSize) {
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
         * The mock reader simulates {@code close}.
         *
         * @param segment the segment to be associated with the reader.
         * @return the mock reader.
         */
        public AsyncSegmentEventReader registerSegmentReader(Segment segment) {
            AsyncSegmentEventReader reader = mock(AsyncSegmentEventReader.class);
            doReturn(segment).when(reader).getSegmentId();
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
         *
         * @param segments a set of segments to acquire.
         */
        @SneakyThrows
        public void acquire(Segment... segments) {
            Map<Segment, Long> map = Arrays.stream(segments)
                    .collect(Collectors.toMap(Function.identity(), s -> 0L));
            for (Segment s : segments) {
                doReturn(Long.MAX_VALUE).when(groupState).getEndOffsetForSegment(s);
            }
            doReturn(map).doReturn(Collections.emptyMap()).when(this.groupState).acquireNewSegmentsIfNeeded(0L);
        }

        /**
         * Convenience method for acquiring a set of segments on a subsequent call to {@code readNextEvent}.
         *
         * @param segments a set of segments to acquire with associated boundaries.
         */
        @SneakyThrows
        public void acquire(SegmentRange... segments) {
            Map<Segment, Long> map = Arrays.stream(segments)
                    .collect(Collectors.toMap(this::segment, SegmentRange::getStartOffset));
            for (SegmentRange s : segments) {
                doReturn(s.getEndOffset()).when(groupState).getEndOffsetForSegment(segment(s));
            }
            doReturn(map).doReturn(Collections.emptyMap()).when(this.groupState).acquireNewSegmentsIfNeeded(0L);
        }

        private Segment segment(SegmentRange s) {
            return new Segment(s.getScope(), s.getStreamName(), s.getSegmentId());
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

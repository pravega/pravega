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
import io.pravega.client.segment.impl.EndOfSegmentException;
import io.pravega.client.segment.impl.EventSegmentInputStream;
import io.pravega.client.segment.impl.NoSuchEventException;
import io.pravega.client.segment.impl.NoSuchSegmentException;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentInputStreamFactory;
import io.pravega.client.segment.impl.SegmentMetadataClient;
import io.pravega.client.segment.impl.SegmentMetadataClientFactory;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentSealedException;
import io.pravega.client.segment.impl.SegmentTruncatedException;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.TruncatedDataException;
import io.pravega.client.stream.mock.MockSegmentStreamFactory;
import io.pravega.test.common.AssertExtensions;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import lombok.Cleanup;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;

public class EventStreamReaderTest {
    private final Consumer<Segment> segmentSealedCallback = segment -> { };
    private final EventWriterConfig writerConfig = EventWriterConfig.builder().build();

    @Test(timeout = 10000)
    public void testEndOfSegmentWithoutSuccessors() throws SegmentSealedException, ReinitializationRequiredException {
        AtomicLong clock = new AtomicLong();
        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
        Orderer orderer = new Orderer();
        ReaderGroupStateManager groupState = Mockito.mock(ReaderGroupStateManager.class);
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(segmentStreamFactory, segmentStreamFactory,
                                                                           new ByteArraySerializer(), groupState,
                                                                           orderer, clock::get,
                                                                           ReaderConfig.builder().build());
        Segment segment = Segment.fromScopedName("Foo/Bar/0");
        Mockito.when(groupState.acquireNewSegmentsIfNeeded(0L))
               .thenReturn(ImmutableMap.of(segment, 0L))
               .thenReturn(Collections.emptyMap());
        SegmentOutputStream stream = segmentStreamFactory.createOutputStreamForSegment(segment, segmentSealedCallback, writerConfig, "");
        ByteBuffer buffer = writeInt(stream, 1);
        EventRead<byte[]> read = reader.readNextEvent(0);
        byte[] event = read.getEvent();
        assertEquals(buffer, ByteBuffer.wrap(event));
        read = reader.readNextEvent(0);
        assertNull(read.getEvent());
        read = reader.readNextEvent(0);
        assertNull(read.getEvent());
        assertEquals(0, reader.getReaders().size());
        reader.close();
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 10000)
    public void testReadWithEndOfSegmentException() throws Exception {
        AtomicLong clock = new AtomicLong();
        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();

        //Prep the mocks.
        ReaderGroupStateManager groupState = Mockito.mock(ReaderGroupStateManager.class);

        //Mock for the two SegmentInputStreams.
        Segment segment = Segment.fromScopedName("Foo/Bar/0");
        EventSegmentInputStream segmentInputStream1 = Mockito.mock(EventSegmentInputStream.class);
        Mockito.when(segmentInputStream1.read(anyLong())).thenThrow(new EndOfSegmentException(EndOfSegmentException.ErrorType.END_OFFSET_REACHED));
        Mockito.when(segmentInputStream1.getSegmentId()).thenReturn(segment);

        EventSegmentInputStream segmentInputStream2 = Mockito.mock(EventSegmentInputStream.class);
        SegmentOutputStream stream = segmentStreamFactory.createOutputStreamForSegment(segment, segmentSealedCallback, writerConfig, "");
        ByteBuffer buffer = writeInt(stream, 1);
        Mockito.when(segmentInputStream2.read(anyLong())).thenReturn(buffer);
        Mockito.when(segmentInputStream2.getSegmentId()).thenReturn(Segment.fromScopedName("Foo/test/0"));
        Mockito.when(segmentInputStream2.getOffset()).thenReturn(10L);

        SegmentInputStreamFactory inputStreamFactory = Mockito.mock(SegmentInputStreamFactory.class);
        Mockito.when(inputStreamFactory.createEventInputStreamForSegment(any(Segment.class), anyLong())).thenReturn(segmentInputStream1);
        //Mock Orderer
        Orderer orderer = Mockito.mock(Orderer.class);
        Mockito.when(orderer.nextSegment(any(List.class))).thenReturn(segmentInputStream1).thenReturn(segmentInputStream2);

        @Cleanup
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(inputStreamFactory, segmentStreamFactory,
                new ByteArraySerializer(), groupState,
                orderer, clock::get,
                ReaderConfig.builder().build());

        EventRead<byte[]> event = reader.readNextEvent(100L);
        //Validate that segmentInputStream1.close() is invoked on reaching endOffset.
        Mockito.verify(segmentInputStream1, Mockito.times(1)).close();
        //Validate that groupState.handleEndOfSegment method is invoked.
        Mockito.verify(groupState, Mockito.times(1)).handleEndOfSegment(segment, false);
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = 10000)
    public void testReadWithSegmentTruncatedException() throws Exception {
        AtomicLong clock = new AtomicLong();
        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();

        //Prep the mocks.
        ReaderGroupStateManager groupState = Mockito.mock(ReaderGroupStateManager.class);

        //Mock for the two SegmentInputStreams.
        Segment segment = Segment.fromScopedName("Foo/Bar/0");
        EventSegmentInputStream segmentInputStream1 = Mockito.mock(EventSegmentInputStream.class);
        Mockito.when(segmentInputStream1.read(anyLong())).thenThrow(new SegmentTruncatedException());
        Mockito.when(segmentInputStream1.getSegmentId()).thenReturn(segment);

        EventSegmentInputStream segmentInputStream2 = Mockito.mock(EventSegmentInputStream.class);
        SegmentOutputStream stream = segmentStreamFactory.createOutputStreamForSegment(segment, segmentSealedCallback, writerConfig, "");
        ByteBuffer buffer = writeInt(stream, 1);
        Mockito.when(segmentInputStream2.read(anyLong())).thenReturn(buffer);
        Mockito.when(segmentInputStream2.getSegmentId()).thenReturn(Segment.fromScopedName("Foo/test/0"));
        Mockito.when(segmentInputStream2.getOffset()).thenReturn(10L);

        SegmentInputStreamFactory inputStreamFactory = Mockito.mock(SegmentInputStreamFactory.class);
        Mockito.when(inputStreamFactory.createEventInputStreamForSegment(any(Segment.class), anyLong())).thenReturn(segmentInputStream1);
        //Mock Orderer
        Orderer orderer = Mockito.mock(Orderer.class);
        Mockito.when(orderer.nextSegment(any(List.class))).thenReturn(segmentInputStream1).thenReturn(segmentInputStream2);

        @Cleanup
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(inputStreamFactory, segmentStreamFactory,
                new ByteArraySerializer(), groupState,
                orderer, clock::get,
                ReaderConfig.builder().build());

        AssertExtensions.assertThrows(TruncatedDataException.class,
                () -> reader.readNextEvent(100L));
        //Validate that groupState.getOrRefreshDelegationTokenFor method is invoked.
        Mockito.verify(groupState, Mockito.times(1)).getOrRefreshDelegationTokenFor(segment);
    }

    @Test(timeout = 10000)
    public void testRead() throws SegmentSealedException, ReinitializationRequiredException {
        AtomicLong clock = new AtomicLong();
        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
        Orderer orderer = new Orderer();
        ReaderGroupStateManager groupState = Mockito.mock(ReaderGroupStateManager.class);
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(segmentStreamFactory, segmentStreamFactory,
                                                                           new ByteArraySerializer(), groupState,
                                                                           orderer, clock::get,
                                                                           ReaderConfig.builder().build());
        Segment segment = Segment.fromScopedName("Foo/Bar/0");
        Mockito.when(groupState.acquireNewSegmentsIfNeeded(0L)).thenReturn(ImmutableMap.of(segment, 0L)).thenReturn(Collections.emptyMap());
        SegmentOutputStream stream = segmentStreamFactory.createOutputStreamForSegment(segment, segmentSealedCallback, writerConfig, "");
        ByteBuffer buffer1 = writeInt(stream, 1);
        ByteBuffer buffer2 = writeInt(stream, 2);
        ByteBuffer buffer3 = writeInt(stream, 3);
        assertEquals(buffer1, ByteBuffer.wrap(reader.readNextEvent(0).getEvent()));
        assertEquals(buffer2, ByteBuffer.wrap(reader.readNextEvent(0).getEvent()));
        assertEquals(buffer3, ByteBuffer.wrap(reader.readNextEvent(0).getEvent()));
        assertNull(reader.readNextEvent(0).getEvent());
        reader.close();
    }

    @Test(timeout = 10000)
    public void testReleaseSegment() throws SegmentSealedException, ReinitializationRequiredException {
        AtomicLong clock = new AtomicLong();
        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
        Orderer orderer = new Orderer();
        ReaderGroupStateManager groupState = Mockito.mock(ReaderGroupStateManager.class);
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(segmentStreamFactory, segmentStreamFactory,
                                                                           new ByteArraySerializer(), groupState,
                                                                           orderer, clock::get,
                                                                           ReaderConfig.builder().build());
        Segment segment1 = Segment.fromScopedName("Foo/Bar/0");
        Segment segment2 = Segment.fromScopedName("Foo/Bar/1");
        Mockito.when(groupState.acquireNewSegmentsIfNeeded(0L))
               .thenReturn(ImmutableMap.of(segment1, 0L, segment2, 0L))
               .thenReturn(Collections.emptyMap());
        SegmentOutputStream stream1 = segmentStreamFactory.createOutputStreamForSegment(segment1, segmentSealedCallback, writerConfig, "");
        SegmentOutputStream stream2 = segmentStreamFactory.createOutputStreamForSegment(segment2, segmentSealedCallback, writerConfig, "");
        writeInt(stream1, 1);
        writeInt(stream2, 2);
        reader.readNextEvent(0);
        List<EventSegmentInputStream> readers = reader.getReaders();
        assertEquals(2, readers.size());
        Assert.assertEquals(segment1, readers.get(0).getSegmentId());
        Assert.assertEquals(segment2, readers.get(1).getSegmentId());

        Mockito.when(groupState.getCheckpoint()).thenReturn("checkpoint");
        assertTrue(reader.readNextEvent(0).isCheckpoint());
        Mockito.when(groupState.getCheckpoint()).thenReturn(null);
        Mockito.when(groupState.findSegmentToReleaseIfRequired()).thenReturn(segment2);
        Mockito.when(groupState.releaseSegment(Mockito.eq(segment2), anyLong(), anyLong())).thenReturn(true);
        assertFalse(reader.readNextEvent(0).isCheckpoint());
        Mockito.verify(groupState).releaseSegment(Mockito.eq(segment2), anyLong(), anyLong());
        readers = reader.getReaders();
        assertEquals(1, readers.size());
        Assert.assertEquals(segment1, readers.get(0).getSegmentId());
        reader.close();
    }

    private ByteBuffer writeInt(SegmentOutputStream stream, int value) throws SegmentSealedException {
        ByteBuffer buffer = ByteBuffer.allocate(4).putInt(value);
        buffer.flip();
        stream.write(PendingEvent.withHeader(null, buffer, new CompletableFuture<Void>()));
        return buffer;
    }

    @SuppressWarnings("unused")
    @Test(timeout = 10000)
    public void testAcquireSegment() throws SegmentSealedException, ReinitializationRequiredException {
        AtomicLong clock = new AtomicLong();
        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
        Orderer orderer = new Orderer();
        ReaderGroupStateManager groupState = Mockito.mock(ReaderGroupStateManager.class);
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(segmentStreamFactory, segmentStreamFactory,
                                                                           new ByteArraySerializer(), groupState,
                                                                           orderer, clock::get,
                                                                           ReaderConfig.builder().build());
        Segment segment1 = Segment.fromScopedName("Foo/Bar/0");
        Segment segment2 = Segment.fromScopedName("Foo/Bar/1");
        Mockito.when(groupState.acquireNewSegmentsIfNeeded(0L))
               .thenReturn(ImmutableMap.of(segment1, 0L))
               .thenReturn(ImmutableMap.of(segment2, 0L))
               .thenReturn(Collections.emptyMap());
        SegmentOutputStream stream1 = segmentStreamFactory.createOutputStreamForSegment(segment1, segmentSealedCallback, writerConfig, "");
        SegmentOutputStream stream2 = segmentStreamFactory.createOutputStreamForSegment(segment2, segmentSealedCallback, writerConfig, "");
        writeInt(stream1, 1);
        writeInt(stream1, 2);
        writeInt(stream2, 3);
        writeInt(stream2, 4);
        reader.readNextEvent(0);
        List<EventSegmentInputStream> readers = reader.getReaders();
        assertEquals(1, readers.size());
        Assert.assertEquals(segment1, readers.get(0).getSegmentId());

        reader.readNextEvent(0);
        readers = reader.getReaders();
        assertEquals(2, readers.size());
        Assert.assertEquals(segment1, readers.get(0).getSegmentId());
        Assert.assertEquals(segment2, readers.get(1).getSegmentId());
        reader.close();
    }
    
    @Test
    public void testEventPointer() throws SegmentSealedException, NoSuchEventException, ReinitializationRequiredException {
        AtomicLong clock = new AtomicLong();
        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
        Orderer orderer = new Orderer();
        ReaderGroupStateManager groupState = Mockito.mock(ReaderGroupStateManager.class);
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(segmentStreamFactory, segmentStreamFactory,
                                                                           new ByteArraySerializer(), groupState,
                                                                           orderer, clock::get,
                                                                           ReaderConfig.builder().build());
        Segment segment = Segment.fromScopedName("Foo/Bar/0");
        Mockito.when(groupState.acquireNewSegmentsIfNeeded(0L)).thenReturn(ImmutableMap.of(segment, 0L)).thenReturn(Collections.emptyMap());
        SegmentOutputStream stream = segmentStreamFactory.createOutputStreamForSegment(segment, segmentSealedCallback, writerConfig, "");
        ByteBuffer buffer1 = writeInt(stream, 1);
        ByteBuffer buffer2 = writeInt(stream, 2);
        ByteBuffer buffer3 = writeInt(stream, 3);
        EventRead<byte[]> event1 = reader.readNextEvent(0);
        EventRead<byte[]> event2 = reader.readNextEvent(0);
        EventRead<byte[]> event3 = reader.readNextEvent(0);
        assertEquals(buffer1, ByteBuffer.wrap(event1.getEvent()));
        assertEquals(buffer2, ByteBuffer.wrap(event2.getEvent()));
        assertEquals(buffer3, ByteBuffer.wrap(event3.getEvent()));
        assertNull(reader.readNextEvent(0).getEvent());
        assertEquals(buffer1, ByteBuffer.wrap(reader.fetchEvent(event1.getEventPointer())));
        assertEquals(buffer3, ByteBuffer.wrap(reader.fetchEvent(event3.getEventPointer())));
        assertEquals(buffer2, ByteBuffer.wrap(reader.fetchEvent(event2.getEventPointer())));
        reader.close();
    }

    @Test(timeout = 10000)
    public void testCheckpoint() throws SegmentSealedException, ReinitializationRequiredException {
        AtomicLong clock = new AtomicLong();
        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
        Orderer orderer = new Orderer();
        ReaderGroupStateManager groupState = Mockito.mock(ReaderGroupStateManager.class);
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(segmentStreamFactory, segmentStreamFactory,
                                                                           new ByteArraySerializer(), groupState,
                                                                           orderer, clock::get,
                                                                           ReaderConfig.builder().build());
        Segment segment = Segment.fromScopedName("Foo/Bar/0");
        Mockito.when(groupState.acquireNewSegmentsIfNeeded(0L)).thenReturn(ImmutableMap.of(segment, 0L)).thenReturn(Collections.emptyMap());
        SegmentOutputStream stream = segmentStreamFactory.createOutputStreamForSegment(segment, segmentSealedCallback, writerConfig, "");
        ByteBuffer buffer = writeInt(stream, 1);
        Mockito.when(groupState.getCheckpoint()).thenReturn("Foo").thenReturn(null);
        EventRead<byte[]> eventRead = reader.readNextEvent(0);
        assertTrue(eventRead.isCheckpoint());
        assertNull(eventRead.getEvent());
        assertEquals("Foo", eventRead.getCheckpointName());
        InOrder order = Mockito.inOrder(groupState);
        order.verify(groupState).getCheckpoint();
        order.verify(groupState, Mockito.never()).checkpoint(Mockito.anyString(), Mockito.any());
        assertEquals(buffer, ByteBuffer.wrap(reader.readNextEvent(0).getEvent()));
        assertNull(reader.readNextEvent(0).getEvent());
        order.verify(groupState).checkpoint(Mockito.eq("Foo"), Mockito.any());
        order.verify(groupState).getCheckpoint();
        reader.close();
    }
    
    @Test(timeout = 10000)
    public void testRestore() throws SegmentSealedException, ReinitializationRequiredException {
        AtomicLong clock = new AtomicLong();
        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
        Orderer orderer = new Orderer();
        ReaderGroupStateManager groupState = Mockito.mock(ReaderGroupStateManager.class);
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(segmentStreamFactory, segmentStreamFactory,
                                                                           new ByteArraySerializer(), groupState,
                                                                           orderer, clock::get,
                                                                           ReaderConfig.builder().build());
        Segment segment = Segment.fromScopedName("Foo/Bar/0");
        Mockito.when(groupState.acquireNewSegmentsIfNeeded(0L)).thenReturn(ImmutableMap.of(segment, 0L)).thenReturn(Collections.emptyMap());
        SegmentOutputStream stream = segmentStreamFactory.createOutputStreamForSegment(segment, segmentSealedCallback, writerConfig, "");
        ByteBuffer buffer = writeInt(stream, 1);
        Mockito.when(groupState.getCheckpoint()).thenThrow(new ReinitializationRequiredException());
        try {
            reader.readNextEvent(0);
            fail();
        } catch (ReinitializationRequiredException e) {
            // expected
        }
        assertTrue(reader.getReaders().isEmpty());
        reader.close();
    }

    @Test(timeout = 10000)
    public void testDataTruncated() throws SegmentSealedException, ReinitializationRequiredException {
        AtomicLong clock = new AtomicLong();
        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
        Orderer orderer = new Orderer();
        ReaderGroupStateManager groupState = Mockito.mock(ReaderGroupStateManager.class);
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(segmentStreamFactory, segmentStreamFactory,
                                                                           new ByteArraySerializer(), groupState,
                                                                           orderer, clock::get,
                                                                           ReaderConfig.builder().build());
        Segment segment = Segment.fromScopedName("Foo/Bar/0");
        Mockito.when(groupState.acquireNewSegmentsIfNeeded(0L))
               .thenReturn(ImmutableMap.of(segment, 0L))
               .thenReturn(Collections.emptyMap());
        SegmentOutputStream stream = segmentStreamFactory.createOutputStreamForSegment(segment, segmentSealedCallback,
                                                                                       writerConfig, "");
        SegmentMetadataClient metadataClient = segmentStreamFactory.createSegmentMetadataClient(segment, "");
        ByteBuffer buffer1 = writeInt(stream, 1);
        ByteBuffer buffer2 = writeInt(stream, 2);
        ByteBuffer buffer3 = writeInt(stream, 3);
        long length = metadataClient.fetchCurrentSegmentLength();
        assertEquals(0, length % 3);
        EventRead<byte[]> event1 = reader.readNextEvent(0);
        assertEquals(buffer1, ByteBuffer.wrap(event1.getEvent()));
        metadataClient.truncateSegment(length / 3);
        assertEquals(buffer2, ByteBuffer.wrap(reader.readNextEvent(0).getEvent()));
        metadataClient.truncateSegment(length);
        ByteBuffer buffer4 = writeInt(stream, 4);
        AssertExtensions.assertThrows(TruncatedDataException.class, () -> reader.readNextEvent(0));
        assertEquals(buffer4, ByteBuffer.wrap(reader.readNextEvent(0).getEvent()));
        assertNull(reader.readNextEvent(0).getEvent());
        AssertExtensions.assertThrows(NoSuchEventException.class, () -> reader.fetchEvent(event1.getEventPointer()));
        reader.close();
    }

    /**
     * This test tests a scenario where a lagging reader's SegmentInputStream receives a SegmentTruncatedException and
     * when the reader tries to fetch the start offset of this segment the SegmentStore returns a NoSuchSegmentException.
     */
    @Test
    public void testTruncatedSegmentDeleted() throws Exception {
        AtomicLong clock = new AtomicLong();
        Segment segment = Segment.fromScopedName("Foo/Bar/0");

        // Setup mock.
        SegmentInputStreamFactory segInputStreamFactory = Mockito.mock(SegmentInputStreamFactory.class);
        SegmentMetadataClientFactory segmentMetadataClientFactory = Mockito.mock(SegmentMetadataClientFactory.class);
        SegmentMetadataClient metadataClient = Mockito.mock(SegmentMetadataClient.class);
        EventSegmentInputStream segmentInputStream = Mockito.mock(EventSegmentInputStream.class);
        Mockito.when(segmentMetadataClientFactory.createSegmentMetadataClient(any(Segment.class), any())).thenReturn(metadataClient);
        Mockito.when(segmentInputStream.getSegmentId()).thenReturn(segment);
        Mockito.when(segInputStreamFactory.createEventInputStreamForSegment(any(Segment.class), anyLong())).thenReturn(segmentInputStream);
        // Ensure segmentInputStream.read() returns SegmentTruncatedException.
        Mockito.when(segmentInputStream.read(anyLong())).thenThrow(SegmentTruncatedException.class);
        // Ensure SegmentInfo returns NoSuchSegmentException.
        Mockito.when(metadataClient.getSegmentInfo()).thenThrow(NoSuchSegmentException.class);

        Orderer orderer = new Orderer();
        ReaderGroupStateManager groupState = Mockito.mock(ReaderGroupStateManager.class);
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(segInputStreamFactory, segmentMetadataClientFactory,
                new ByteArraySerializer(), groupState,
                orderer, clock::get,
                ReaderConfig.builder().build());
        Mockito.when(groupState.acquireNewSegmentsIfNeeded(0L))
                .thenReturn(ImmutableMap.of(segment, 0L))
                .thenReturn(Collections.emptyMap());

        // Validate that TruncatedDataException is thrown.
        AssertExtensions.assertThrows(TruncatedDataException.class, () -> reader.readNextEvent(0));
        // Ensure this segment is closed.
        Mockito.verify(segmentInputStream, Mockito.times(1)).close();
        // Ensure groupstate is updated to handle end of segment.
        Mockito.verify(groupState, Mockito.times(1)).handleEndOfSegment(segment, true);
    }
}

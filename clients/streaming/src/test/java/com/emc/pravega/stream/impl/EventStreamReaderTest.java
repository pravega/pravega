/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.stream.EventRead;
import com.emc.pravega.stream.ReaderConfig;
import com.emc.pravega.stream.ReinitializationRequiredException;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.impl.segment.NoSuchEventException;
import com.emc.pravega.stream.impl.segment.SegmentInputStream;
import com.emc.pravega.stream.impl.segment.SegmentOutputStream;
import com.emc.pravega.stream.impl.segment.SegmentSealedException;
import com.emc.pravega.stream.mock.MockSegmentStreamFactory;
import com.google.common.collect.ImmutableMap;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;

public class EventStreamReaderTest {

    @Test(timeout = 10000)
    public void testEndOfSegmentWithoutSuccessors() throws SegmentSealedException, ReinitializationRequiredException {
        AtomicLong clock = new AtomicLong();
        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
        Orderer orderer = new Orderer();
        ReaderGroupStateManager groupState = Mockito.mock(ReaderGroupStateManager.class);
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<byte[]>(segmentStreamFactory,
                new ByteArraySerializer(),
                groupState,
                orderer,
                clock::get,
                ReaderConfig.builder().build());
        Segment segment = Segment.fromScopedName("Foo/Bar/0");
        Mockito.when(groupState.acquireNewSegmentsIfNeeded(0L))
               .thenReturn(ImmutableMap.of(segment, 0L))
               .thenReturn(Collections.emptyMap());
        SegmentOutputStream stream = segmentStreamFactory.createOutputStreamForSegment(segment);
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

    @Test(timeout = 10000)
    public void testRead() throws SegmentSealedException, ReinitializationRequiredException {
        AtomicLong clock = new AtomicLong();
        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
        Orderer orderer = new Orderer();
        ReaderGroupStateManager groupState = Mockito.mock(ReaderGroupStateManager.class);
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<byte[]>(segmentStreamFactory,
                new ByteArraySerializer(),
                groupState,
                orderer,
                clock::get,
                ReaderConfig.builder().build());
        Segment segment = Segment.fromScopedName("Foo/Bar/0");
        Mockito.when(groupState.acquireNewSegmentsIfNeeded(0L)).thenReturn(ImmutableMap.of(segment, 0L)).thenReturn(Collections.emptyMap());
        SegmentOutputStream stream = segmentStreamFactory.createOutputStreamForSegment(segment);
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
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<byte[]>(segmentStreamFactory,
                new ByteArraySerializer(),
                groupState,
                orderer,
                clock::get,
                ReaderConfig.builder().build());
        Segment segment1 = Segment.fromScopedName("Foo/Bar/0");
        Segment segment2 = Segment.fromScopedName("Foo/Bar/1");
        Mockito.when(groupState.acquireNewSegmentsIfNeeded(0L))
               .thenReturn(ImmutableMap.of(segment1, 0L, segment2, 0L))
               .thenReturn(Collections.emptyMap());
        SegmentOutputStream stream1 = segmentStreamFactory.createOutputStreamForSegment(segment1);
        SegmentOutputStream stream2 = segmentStreamFactory.createOutputStreamForSegment(segment2);
        writeInt(stream1, 1);
        writeInt(stream2, 2);
        reader.readNextEvent(0);
        List<SegmentInputStream> readers = reader.getReaders();
        assertEquals(2, readers.size());
        assertEquals(segment1, readers.get(0).getSegmentId());
        assertEquals(segment2, readers.get(1).getSegmentId());

        Mockito.when(groupState.getCheckpoint()).thenReturn("checkpoint");
        Mockito.when(groupState.findSegmentToReleaseIfRequired()).thenReturn(segment2);
        reader.readNextEvent(0);
        reader.readNextEvent(0);
        Mockito.verify(groupState).releaseSegment(Mockito.eq(segment2), Mockito.anyLong(), Mockito.anyLong());
        readers = reader.getReaders();
        assertEquals(1, readers.size());
        assertEquals(segment1, readers.get(0).getSegmentId());
        reader.close();
    }

    private ByteBuffer writeInt(SegmentOutputStream stream, int value) throws SegmentSealedException {
        ByteBuffer buffer = ByteBuffer.allocate(4).putInt(value);
        buffer.flip();
        stream.write(buffer, new CompletableFuture<Boolean>());
        return buffer;
    }

    @Test(timeout = 10000)
    public void testAcquireSegment() throws SegmentSealedException, ReinitializationRequiredException {
        AtomicLong clock = new AtomicLong();
        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
        Orderer orderer = new Orderer();
        ReaderGroupStateManager groupState = Mockito.mock(ReaderGroupStateManager.class);
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<byte[]>(segmentStreamFactory,
                new ByteArraySerializer(),
                groupState,
                orderer,
                clock::get,
                ReaderConfig.builder().build());
        Segment segment1 = Segment.fromScopedName("Foo/Bar/0");
        Segment segment2 = Segment.fromScopedName("Foo/Bar/1");
        Mockito.when(groupState.acquireNewSegmentsIfNeeded(0L))
               .thenReturn(ImmutableMap.of(segment1, 0L))
               .thenReturn(ImmutableMap.of(segment2, 0L))
               .thenReturn(Collections.emptyMap());
        SegmentOutputStream stream1 = segmentStreamFactory.createOutputStreamForSegment(segment1);
        SegmentOutputStream stream2 = segmentStreamFactory.createOutputStreamForSegment(segment2);
        writeInt(stream1, 1);
        writeInt(stream1, 2);
        writeInt(stream2, 3);
        writeInt(stream2, 4);
        reader.readNextEvent(0);
        List<SegmentInputStream> readers = reader.getReaders();
        assertEquals(1, readers.size());
        assertEquals(segment1, readers.get(0).getSegmentId());

        reader.readNextEvent(0);
        readers = reader.getReaders();
        assertEquals(2, readers.size());
        assertEquals(segment1, readers.get(0).getSegmentId());
        assertEquals(segment2, readers.get(1).getSegmentId());
        reader.close();
    }
    
    @Test
    public void testEventPointer() throws SegmentSealedException, NoSuchEventException, ReinitializationRequiredException {
        AtomicLong clock = new AtomicLong();
        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
        Orderer orderer = new Orderer();
        ReaderGroupStateManager groupState = Mockito.mock(ReaderGroupStateManager.class);
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<byte[]>(segmentStreamFactory,
                new ByteArraySerializer(),
                groupState,
                orderer,
                clock::get,
                ReaderConfig.builder().build());
        Segment segment = Segment.fromScopedName("Foo/Bar/0");
        Mockito.when(groupState.acquireNewSegmentsIfNeeded(0L)).thenReturn(ImmutableMap.of(segment, 0L)).thenReturn(Collections.emptyMap());
        SegmentOutputStream stream = segmentStreamFactory.createOutputStreamForSegment(segment);
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
        assertEquals(buffer1, ByteBuffer.wrap(reader.read(event1.getEventPointer())));
        assertEquals(buffer3, ByteBuffer.wrap(reader.read(event3.getEventPointer())));
        assertEquals(buffer2, ByteBuffer.wrap(reader.read(event2.getEventPointer())));
        reader.close();
    }

    @Test(timeout = 10000)
    public void testCheckpoint() throws SegmentSealedException, ReinitializationRequiredException {
        AtomicLong clock = new AtomicLong();
        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
        Orderer orderer = new Orderer();
        ReaderGroupStateManager groupState = Mockito.mock(ReaderGroupStateManager.class);
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<byte[]>(segmentStreamFactory,
                new ByteArraySerializer(),
                groupState,
                orderer,
                clock::get,
                ReaderConfig.builder().build());
        Segment segment = Segment.fromScopedName("Foo/Bar/0");
        Mockito.when(groupState.acquireNewSegmentsIfNeeded(0L)).thenReturn(ImmutableMap.of(segment, 0L)).thenReturn(Collections.emptyMap());
        SegmentOutputStream stream = segmentStreamFactory.createOutputStreamForSegment(segment);
        ByteBuffer buffer = writeInt(stream, 1);
        Mockito.when(groupState.getCheckpoint()).thenReturn("Foo").thenReturn(null);
        EventRead<byte[]> eventRead = reader.readNextEvent(0);
        assertTrue(eventRead.isCheckpoint());
        assertNull(eventRead.getEvent());
        assertEquals("Foo", eventRead.getCheckpointName());
        assertEquals(buffer, ByteBuffer.wrap(reader.readNextEvent(0).getEvent()));
        assertNull(reader.readNextEvent(0).getEvent());
        reader.close();
    }
    
    @Test//(timeout = 10000)
    public void testRestore() throws SegmentSealedException, ReinitializationRequiredException {
        AtomicLong clock = new AtomicLong();
        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
        Orderer orderer = new Orderer();
        ReaderGroupStateManager groupState = Mockito.mock(ReaderGroupStateManager.class);
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<byte[]>(segmentStreamFactory,
                new ByteArraySerializer(),
                groupState,
                orderer,
                clock::get,
                ReaderConfig.builder().build());
        Segment segment = Segment.fromScopedName("Foo/Bar/0");
        Mockito.when(groupState.acquireNewSegmentsIfNeeded(0L)).thenReturn(ImmutableMap.of(segment, 0L)).thenReturn(Collections.emptyMap());
        SegmentOutputStream stream = segmentStreamFactory.createOutputStreamForSegment(segment);
        ByteBuffer buffer = writeInt(stream, 1);
        Mockito.when(groupState.getCheckpoint()).thenThrow(new ReinitializationRequiredException());
        try {
            reader.readNextEvent(0);
            fail();
        } catch(ReinitializationRequiredException e) {
            // expected
        }
        assertTrue(reader.getReaders().isEmpty());
        reader.close();
    }
    
}

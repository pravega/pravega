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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl.ReaderGroupStateInitSerializer;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl.ReaderGroupStateUpdatesSerializer;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.segment.impl.EndOfSegmentException;
import io.pravega.client.segment.impl.EventSegmentReader;
import io.pravega.client.segment.impl.NoSuchEventException;
import io.pravega.client.segment.impl.NoSuchSegmentException;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentInputStreamFactory;
import io.pravega.client.segment.impl.SegmentMetadataClient;
import io.pravega.client.segment.impl.SegmentMetadataClientFactory;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentSealedException;
import io.pravega.client.segment.impl.SegmentTruncatedException;
import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReaderNotInReaderGroupException;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.TruncatedDataException;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.client.stream.mock.MockSegmentStreamFactory;
import io.pravega.shared.NameUtils;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.test.common.AssertExtensions;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import lombok.Cleanup;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import static io.pravega.client.stream.impl.ReaderGroupImpl.getEndSegmentsForStreams;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;

public class EventStreamReaderTest {
    private final Consumer<Segment> segmentSealedCallback = segment -> { };
    private final EventWriterConfig writerConfig = EventWriterConfig.builder().build();

    @Test(timeout = 10000)
    public void testEndOfSegmentWithoutSuccessors() throws SegmentSealedException, ReaderNotInReaderGroupException {
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
        EventSegmentReader segmentInputStream1 = Mockito.mock(EventSegmentReader.class);
        Mockito.when(segmentInputStream1.read(anyLong())).thenThrow(new EndOfSegmentException(EndOfSegmentException.ErrorType.END_OFFSET_REACHED));
        Mockito.when(segmentInputStream1.getSegmentId()).thenReturn(segment);

        EventSegmentReader segmentInputStream2 = Mockito.mock(EventSegmentReader.class);
        SegmentOutputStream stream = segmentStreamFactory.createOutputStreamForSegment(segment, segmentSealedCallback, writerConfig, "");
        ByteBuffer buffer = writeInt(stream, 1);
        Mockito.when(segmentInputStream2.read(anyLong())).thenReturn(buffer);
        Mockito.when(segmentInputStream2.getSegmentId()).thenReturn(Segment.fromScopedName("Foo/test/0"));
        Mockito.when(segmentInputStream2.getOffset()).thenReturn(10L);

        SegmentInputStreamFactory inputStreamFactory = Mockito.mock(SegmentInputStreamFactory.class);
        Mockito.when(inputStreamFactory.createEventReaderForSegment(any(Segment.class), anyLong())).thenReturn(segmentInputStream1);
        //Mock Orderer
        Orderer orderer = Mockito.mock(Orderer.class);
        Mockito.when(orderer.nextSegment(any(List.class))).thenReturn(segmentInputStream1).thenReturn(segmentInputStream2);

        @Cleanup
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(inputStreamFactory, segmentStreamFactory,
                new ByteArraySerializer(), groupState,
                orderer, clock::get,
                ReaderConfig.builder().build());

        InOrder inOrder = Mockito.inOrder(segmentInputStream1, groupState);
        EventRead<byte[]> event = reader.readNextEvent(100L);
        assertNotNull(event.getEvent());
        //Validate that segmentInputStream1.close() is invoked on reaching endOffset.
        Mockito.verify(segmentInputStream1, Mockito.times(1)).close();
        
        // Ensure groupstate is updated not updated before the checkpoint.
        inOrder.verify(groupState, Mockito.times(0)).handleEndOfSegment(segment);
        Mockito.when(groupState.getCheckpoint()).thenReturn("checkpoint").thenReturn(null);
        assertEquals("checkpoint", reader.readNextEvent(0).getCheckpointName());
        inOrder.verify(groupState).getCheckpoint();
        // Ensure groupstate is updated not updated before the checkpoint.
        inOrder.verify(groupState, Mockito.times(0)).handleEndOfSegment(segment);
        event = reader.readNextEvent(0);
        assertFalse(event.isCheckpoint());
        // Now it is called.
        inOrder.verify(groupState, Mockito.times(1)).handleEndOfSegment(segment);
        
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
        EventSegmentReader segmentInputStream1 = Mockito.mock(EventSegmentReader.class);
        Mockito.when(segmentInputStream1.read(anyLong())).thenThrow(new SegmentTruncatedException());
        Mockito.when(segmentInputStream1.getSegmentId()).thenReturn(segment);

        EventSegmentReader segmentInputStream2 = Mockito.mock(EventSegmentReader.class);
        SegmentOutputStream stream = segmentStreamFactory.createOutputStreamForSegment(segment, segmentSealedCallback, writerConfig, "");
        ByteBuffer buffer = writeInt(stream, 1);
        Mockito.when(segmentInputStream2.read(anyLong())).thenReturn(buffer);
        Mockito.when(segmentInputStream2.getSegmentId()).thenReturn(Segment.fromScopedName("Foo/test/0"));
        Mockito.when(segmentInputStream2.getOffset()).thenReturn(10L);

        SegmentInputStreamFactory inputStreamFactory = Mockito.mock(SegmentInputStreamFactory.class);
        Mockito.when(inputStreamFactory.createEventReaderForSegment(any(Segment.class), anyLong())).thenReturn(segmentInputStream1);
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
    public void testRead() throws SegmentSealedException, ReaderNotInReaderGroupException {
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
    public void testReleaseSegment() throws SegmentSealedException, ReaderNotInReaderGroupException {
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
        List<EventSegmentReader> readers = reader.getReaders();
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
    public void testAcquireSegment() throws SegmentSealedException, ReaderNotInReaderGroupException {
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
        List<EventSegmentReader> readers = reader.getReaders();
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
    public void testEventPointer() throws SegmentSealedException, NoSuchEventException, ReaderNotInReaderGroupException {
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
    public void testCheckpoint() throws SegmentSealedException, ReaderNotInReaderGroupException {
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
    public void testRestore() throws SegmentSealedException, ReaderNotInReaderGroupException {
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
    public void testDataTruncated() throws SegmentSealedException, ReaderNotInReaderGroupException {
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
        EventSegmentReader segmentInputStream = Mockito.mock(EventSegmentReader.class);
        Mockito.when(segmentMetadataClientFactory.createSegmentMetadataClient(any(Segment.class), any())).thenReturn(metadataClient);
        Mockito.when(segmentInputStream.getSegmentId()).thenReturn(segment);
        Mockito.when(segInputStreamFactory.createEventReaderForSegment(any(Segment.class), anyLong())).thenReturn(segmentInputStream);
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
        InOrder inOrder = Mockito.inOrder(groupState, segmentInputStream);
        // Validate that TruncatedDataException is thrown.
        AssertExtensions.assertThrows(TruncatedDataException.class, () -> reader.readNextEvent(0));
        inOrder.verify(groupState).getCheckpoint();
        // Ensure this segment is closed.
        inOrder.verify(segmentInputStream, Mockito.times(1)).close();      
        // Ensure groupstate is updated not updated before the checkpoint.
        inOrder.verify(groupState, Mockito.times(0)).handleEndOfSegment(segment);
        Mockito.when(groupState.getCheckpoint()).thenReturn("Foo").thenReturn(null);
        EventRead<byte[]> event = reader.readNextEvent(0);
        assertTrue(event.isCheckpoint());
        assertEquals("Foo", event.getCheckpointName());
        inOrder.verify(groupState).getCheckpoint();
        // Ensure groupstate is updated not updated before the checkpoint.
        inOrder.verify(groupState, Mockito.times(0)).handleEndOfSegment(segment);
        event = reader.readNextEvent(0);
        assertFalse(event.isCheckpoint());
        assertNull(event.getEvent());
        // Now it is called.
        inOrder.verify(groupState).handleEndOfSegment(segment);
    }
    
    @Test(timeout=10000)
    public void testSegmentSplit() throws EndOfSegmentException, SegmentTruncatedException, SegmentSealedException, ReaderNotInReaderGroupException {
        AtomicLong clock = new AtomicLong();
        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();

        //Prep the mocks.
        ReaderGroupStateManager groupState = Mockito.mock(ReaderGroupStateManager.class);

        //Mock for the two SegmentInputStreams.
        Segment segment1 = Segment.fromScopedName("Foo/Bar/1");
        EventSegmentReader segmentInputStream1 = Mockito.mock(EventSegmentReader.class);
        Mockito.when(segmentInputStream1.read(anyLong())).thenThrow(new EndOfSegmentException(EndOfSegmentException.ErrorType.END_OF_SEGMENT_REACHED));
        Mockito.when(segmentInputStream1.getSegmentId()).thenReturn(segment1);

        Segment segment2 = Segment.fromScopedName("Foo/Bar/2");
        EventSegmentReader segmentInputStream2 = Mockito.mock(EventSegmentReader.class);
        SegmentOutputStream stream2 = segmentStreamFactory.createOutputStreamForSegment(segment2, segmentSealedCallback, writerConfig, "");
        Mockito.when(segmentInputStream2.read(anyLong())).thenReturn(writeInt(stream2, 2));
        Mockito.when(segmentInputStream2.getSegmentId()).thenReturn(segment2);
        
        Segment segment3 = Segment.fromScopedName("Foo/Bar/3");
        EventSegmentReader segmentInputStream3 = Mockito.mock(EventSegmentReader.class);
        SegmentOutputStream stream3 = segmentStreamFactory.createOutputStreamForSegment(segment3, segmentSealedCallback, writerConfig, "");
        Mockito.when(segmentInputStream2.read(anyLong())).thenReturn(writeInt(stream3, 3));
        Mockito.when(segmentInputStream2.getSegmentId()).thenReturn(segment3);

        SegmentInputStreamFactory inputStreamFactory = Mockito.mock(SegmentInputStreamFactory.class);
        Mockito.when(inputStreamFactory.createEventReaderForSegment(segment1, Long.MAX_VALUE)).thenReturn(segmentInputStream1);
        Mockito.when(inputStreamFactory.createEventReaderForSegment(segment2, Long.MAX_VALUE)).thenReturn(segmentInputStream2);
        Mockito.when(inputStreamFactory.createEventReaderForSegment(segment3, Long.MAX_VALUE)).thenReturn(segmentInputStream3);     
        
        Mockito.when(groupState.getEndOffsetForSegment(any())).thenReturn(Long.MAX_VALUE);
        
        @Cleanup
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(inputStreamFactory, segmentStreamFactory,
                new ByteArraySerializer(), groupState,
                new Orderer(), clock::get,
                ReaderConfig.builder().build());

        Mockito.when(groupState.acquireNewSegmentsIfNeeded(anyLong())).thenReturn(ImmutableMap.of(segment1, 0L)).thenReturn(Collections.emptyMap());
        
        InOrder inOrder = Mockito.inOrder(segmentInputStream1, groupState);
        EventRead<byte[]> event = reader.readNextEvent(100L);
        assertNull(event.getEvent());
        event = reader.readNextEvent(100L);
        assertNull(event.getEvent());
        
        Mockito.when(groupState.getCheckpoint()).thenReturn("checkpoint").thenReturn(null);
        Mockito.when(groupState.acquireNewSegmentsIfNeeded(anyLong())).thenReturn(ImmutableMap.of(segment2, 0L, segment3, 0L)).thenReturn(Collections.emptyMap());
        assertEquals("checkpoint", reader.readNextEvent(0).getCheckpointName());
        inOrder.verify(groupState).getCheckpoint();
        // Ensure groupstate is updated not updated before the checkpoint.
        inOrder.verify(groupState, Mockito.times(0)).handleEndOfSegment(segment1);
        event = reader.readNextEvent(0);
        assertFalse(event.isCheckpoint());
        // Now it is called.
        inOrder.verify(groupState, Mockito.times(1)).handleEndOfSegment(segment1);
        assertEquals(ImmutableList.of(segmentInputStream2, segmentInputStream3), reader.getReaders());        
        
    }
    
    @Test
    public void testReaderClose() throws EndOfSegmentException, SegmentTruncatedException, SegmentSealedException {
        String scope = "scope";
        String stream = "stream";
        AtomicLong clock = new AtomicLong();
        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", -1);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, false);
        
        //Mock for the two SegmentInputStreams.
        Segment segment1 = new Segment(scope, stream, 0);
        @Cleanup
        SegmentOutputStream stream1 = segmentStreamFactory.createOutputStreamForSegment(segment1, segmentSealedCallback, writerConfig, "");
        writeInt(stream1, 1);
        writeInt(stream1, 1);
        writeInt(stream1, 1);
        Segment segment2 = new Segment(scope, stream, 1);
        @Cleanup
        SegmentOutputStream stream2 = segmentStreamFactory.createOutputStreamForSegment(segment2, segmentSealedCallback, writerConfig, "");
        writeInt(stream2, 2);
        writeInt(stream2, 2);
        writeInt(stream2, 2);
        StateSynchronizer<ReaderGroupState> sync = createStateSynchronizerForReaderGroup(connectionFactory, controller,
                                                                                         segmentStreamFactory,
                                                                                         Stream.of(scope, stream),
                                                                                         "reader1", clock, 2);
        @Cleanup
        EventStreamReaderImpl<byte[]> reader1 = createReader(controller, segmentStreamFactory, "reader1", sync, clock);
        @Cleanup
        EventStreamReaderImpl<byte[]> reader2 = createReader(controller, segmentStreamFactory, "reader2", sync, clock);
        
        assertEquals(1, readInt(reader1.readNextEvent(0)));
        assertEquals(2, readInt(reader2.readNextEvent(0)));
        reader2.close();
        clock.addAndGet(ReaderGroupStateManager.UPDATE_WINDOW.toNanos());
        assertEquals(1, readInt(reader1.readNextEvent(0)));
        assertEquals(2, readInt(reader1.readNextEvent(0)));
        assertEquals(1, readInt(reader1.readNextEvent(0)));
        assertEquals(2, readInt(reader1.readNextEvent(0)));
    }
    
    private int readInt(EventRead<byte[]> eventRead) {
        byte[] event = eventRead.getEvent();
        assertNotNull(event);
        return ByteBuffer.wrap(event).getInt();
    }

    private EventStreamReaderImpl<byte[]> createReader(MockController controller,
                                                       MockSegmentStreamFactory segmentStreamFactory, String readerId,
                                                       StateSynchronizer<ReaderGroupState> sync, AtomicLong clock) {
        ReaderGroupStateManager groupState = new ReaderGroupStateManager(readerId, sync, controller, clock::get);
        groupState.initializeReader(0);
        return new EventStreamReaderImpl<>(segmentStreamFactory, segmentStreamFactory, new ByteArraySerializer(),
                                           groupState, new Orderer(), clock::get, ReaderConfig.builder().build());
    }

    private StateSynchronizer<ReaderGroupState> createStateSynchronizerForReaderGroup(ConnectionFactory connectionFactory,
                                                                                      Controller controller,
                                                                                      MockSegmentStreamFactory streamFactory,
                                                                                      Stream stream, String readerId,
                                                                                      AtomicLong clock,
                                                                                      int numSegments) {
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                                                              .scalingPolicy(ScalingPolicy.fixed(numSegments))
                                                              .build();
        controller.createScope(stream.getScope());
        controller.createStream(stream.getScope(), stream.getStreamName(), streamConfig);
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(stream.getScope(), controller, connectionFactory,
                                                                streamFactory, streamFactory, streamFactory,
                                                                streamFactory);

        ReaderGroupConfig config = ReaderGroupConfig.builder().disableAutomaticCheckpoints().stream(stream).build();
        Map<Segment, Long> segments = ReaderGroupImpl.getSegmentsForStreams(controller, config);
        StateSynchronizer<ReaderGroupState> sync = clientFactory.createStateSynchronizer(NameUtils.getStreamForReaderGroup("readerGroup"),
                                                                                         new ReaderGroupStateUpdatesSerializer(),
                                                                                         new ReaderGroupStateInitSerializer(),
                                                                                         SynchronizerConfig.builder()
                                                                                                           .build());
        sync.initialize(new ReaderGroupState.ReaderGroupStateInit(config,
                                                                  ReaderGroupImpl.getSegmentsForStreams(controller,
                                                                                                        config),
                                                                  getEndSegmentsForStreams(config)));
        return sync;
    }

    @Test
    public void testPositionsContainSealedSegments() throws SegmentSealedException {
        String scope = "scope";
        String stream = "stream";
        AtomicLong clock = new AtomicLong();
        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
        PravegaNodeUri endpoint = new PravegaNodeUri("localhost", -1);
        @Cleanup
        MockConnectionFactoryImpl connectionFactory = new MockConnectionFactoryImpl();
        @Cleanup
        MockController controller = new MockController(endpoint.getEndpoint(), endpoint.getPort(), connectionFactory, false);
        
        //Mock for the two SegmentInputStreams.
        Segment segment1 = new Segment(scope, stream, 0);
        @Cleanup
        SegmentOutputStream stream1 = segmentStreamFactory.createOutputStreamForSegment(segment1, segmentSealedCallback, writerConfig, "");
        writeInt(stream1, 1);
        Segment segment2 = new Segment(scope, stream, 1);
        @Cleanup
        SegmentOutputStream stream2 = segmentStreamFactory.createOutputStreamForSegment(segment2, segmentSealedCallback, writerConfig, "");
        writeInt(stream2, 2);
        writeInt(stream2, 2);
        writeInt(stream2, 2);
        @Cleanup
        StateSynchronizer<ReaderGroupState> sync = createStateSynchronizerForReaderGroup(connectionFactory, controller,
                                                                                         segmentStreamFactory,
                                                                                         Stream.of(scope, stream),
                                                                                         "reader1", clock, 2);
        @Cleanup
        EventStreamReaderImpl<byte[]> reader = createReader(controller, segmentStreamFactory, "reader1", sync, clock);
        EventRead<byte[]> event = reader.readNextEvent(100);
        assertEquals(2, readInt(event));
        assertEquals(ImmutableSet.of(), event.getPosition().asImpl().getCompletedSegments());
        assertEquals(ImmutableSet.of(segment1, segment2), event.getPosition().asImpl().getOwnedSegments());
        clock.addAndGet(ReaderGroupStateManager.UPDATE_WINDOW.toNanos());
        event = reader.readNextEvent(100);
        assertEquals(1, readInt(event));
        assertEquals(ImmutableSet.of(), event.getPosition().asImpl().getCompletedSegments());
        assertEquals(ImmutableSet.of(segment1, segment2), event.getPosition().asImpl().getOwnedSegments());
        clock.addAndGet(ReaderGroupStateManager.UPDATE_WINDOW.toNanos());
        event = reader.readNextEvent(100);
        assertEquals(2, readInt(event));
        assertEquals(ImmutableSet.of(), event.getPosition().asImpl().getCompletedSegments());
        assertEquals(ImmutableSet.of(segment1, segment2), event.getPosition().asImpl().getOwnedSegments());
        clock.addAndGet(ReaderGroupStateManager.UPDATE_WINDOW.toNanos());
        event = reader.readNextEvent(100);
        assertEquals(2, readInt(event));
        assertEquals(ImmutableSet.of(segment1), event.getPosition().asImpl().getCompletedSegments());
        assertEquals(ImmutableSet.of(segment1, segment2), event.getPosition().asImpl().getOwnedSegments());
        clock.addAndGet(ReaderGroupStateManager.UPDATE_WINDOW.toNanos());
        event = reader.readNextEvent(10);
        assertNull(event.getEvent());
        assertEquals(ImmutableSet.of(segment1, segment2), event.getPosition().asImpl().getCompletedSegments());
        assertEquals(ImmutableSet.of(segment1, segment2), event.getPosition().asImpl().getOwnedSegments());
    }
}

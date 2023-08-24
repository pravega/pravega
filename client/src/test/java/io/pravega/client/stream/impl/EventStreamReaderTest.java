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


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl.ReaderGroupStateInitSerializer;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl.ReaderGroupStateUpdatesSerializer;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.security.auth.DelegationTokenProviderFactory;
import io.pravega.client.segment.impl.EndOfSegmentException;
import io.pravega.client.segment.impl.EventSegmentReader;
import io.pravega.client.segment.impl.NoSuchEventException;
import io.pravega.client.segment.impl.NoSuchSegmentException;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentInfo;
import io.pravega.client.segment.impl.SegmentInputStreamFactory;
import io.pravega.client.segment.impl.SegmentMetadataClient;
import io.pravega.client.segment.impl.SegmentMetadataClientFactory;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentSealedException;
import io.pravega.client.segment.impl.SegmentTruncatedException;
import io.pravega.client.state.RevisionedStreamClient;
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
import io.pravega.client.stream.TimeWindow;
import io.pravega.client.stream.TruncatedDataException;
import io.pravega.client.stream.mock.MockClientFactory;
import io.pravega.client.stream.mock.MockConnectionFactoryImpl;
import io.pravega.client.stream.mock.MockController;
import io.pravega.client.stream.mock.MockSegmentStreamFactory;
import io.pravega.client.watermark.WatermarkSerializer;
import io.pravega.shared.NameUtils;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.watermarks.Watermark;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.InlineExecutor;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import lombok.Cleanup;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import static io.pravega.client.stream.impl.ReaderGroupImpl.getEndSegmentsForStreams;
import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;

public class EventStreamReaderTest {
    private final Consumer<Segment> segmentSealedCallback = segment -> { };
    private final EventWriterConfig writerConfig = EventWriterConfig.builder().build();
    private final List<ClientFactoryImpl> createdClientFactories = Collections.synchronizedList(new ArrayList<>());

    @After
    public void tearDown() {
        createdClientFactories.forEach(ClientFactoryImpl::close);
    }

    @Test(timeout = 10000)
    public void testEndOfSegmentWithoutSuccessors() throws ReaderNotInReaderGroupException {
        AtomicLong clock = new AtomicLong();
        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
        Orderer orderer = new Orderer();
        ReaderGroupStateManager groupState = Mockito.mock(ReaderGroupStateManager.class);
        @Cleanup
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(segmentStreamFactory, segmentStreamFactory,
                                                                           new ByteArraySerializer(), groupState,
                                                                           orderer, clock::get,
                                                                           ReaderConfig.builder().build(),
                                                                           createWatermarkReaders(),
                                                                           Mockito.mock(Controller.class));
        Segment segment = Segment.fromScopedName("Foo/Bar/0");
        Mockito.when(groupState.acquireNewSegmentsIfNeeded(eq(0L), any()))
               .thenReturn(ImmutableMap.of(new SegmentWithRange(segment, 0, 1), 0L))
               .thenReturn(Collections.emptyMap());
        Mockito.when(groupState.getEndOffsetForSegment(any(Segment.class))).thenReturn(Long.MAX_VALUE);
        Mockito.when(groupState.handleEndOfSegment(any())).thenReturn(true);
        SegmentOutputStream stream = segmentStreamFactory.createOutputStreamForSegment(segment, segmentSealedCallback, writerConfig,
                DelegationTokenProviderFactory.createWithEmptyToken());
        ByteBuffer buffer = writeInt(stream, 1);
        EventRead<byte[]> read = reader.readNextEvent(0);
        byte[] event = read.getEvent();
        assertEquals(buffer, ByteBuffer.wrap(event));
        read = reader.readNextEvent(0);
        assertNull(read.getEvent());
        read = reader.readNextEvent(0);
        assertNull(read.getEvent());
        assertEquals(0, reader.getReaders().size());
        assertEquals(1, reader.getRanges().size());
        Mockito.when(groupState.getCheckpoint()).thenReturn("CP1");
        read = reader.readNextEvent(0);
        assertTrue(read.isCheckpoint());
        read = reader.readNextEvent(0);
        assertNull(read.getEvent());
        assertEquals(0, reader.getRanges().size());
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
        @Cleanup
        SegmentOutputStream stream = segmentStreamFactory.createOutputStreamForSegment(segment, segmentSealedCallback, writerConfig,
                DelegationTokenProviderFactory.createWithEmptyToken());
        ByteBuffer buffer = writeInt(stream, 1);
        Mockito.when(segmentInputStream2.read(anyLong())).thenReturn(buffer);
        Mockito.when(segmentInputStream2.getSegmentId()).thenReturn(Segment.fromScopedName("Foo/test/0"));
        Mockito.when(segmentInputStream2.getOffset()).thenReturn(10L);

        SegmentInputStreamFactory inputStreamFactory = Mockito.mock(SegmentInputStreamFactory.class);
        Mockito.when(inputStreamFactory.createEventReaderForSegment(any(Segment.class), anyInt(), any(Semaphore.class), anyLong())).thenReturn(segmentInputStream1);
        //Mock Orderer
        Orderer orderer = Mockito.mock(Orderer.class);
        Mockito.when(orderer.nextSegment(any(List.class))).thenReturn(segmentInputStream1).thenReturn(segmentInputStream2);

        @Cleanup
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(inputStreamFactory, segmentStreamFactory,
                new ByteArraySerializer(), groupState,
                orderer, clock::get,
                ReaderConfig.builder().build(), createWatermarkReaders(), Mockito.mock(Controller.class));

        InOrder inOrder = Mockito.inOrder(segmentInputStream1, groupState);
        EventRead<byte[]> event = reader.readNextEvent(100L);
        assertNotNull(event.getEvent());
        //Validate that segmentInputStream1.close() is invoked on reaching endOffset.
        Mockito.verify(segmentInputStream1, Mockito.times(1)).close();
        
        SegmentWithRange segmentWithRange = new SegmentWithRange(segment, 0, 1);
        // Verify groupstate is not updated before the checkpoint, but is after.
        inOrder.verify(groupState, Mockito.times(0)).handleEndOfSegment(segmentWithRange);
        Mockito.when(groupState.getCheckpoint()).thenReturn("checkpoint").thenReturn(null);
        assertEquals("checkpoint", reader.readNextEvent(0).getCheckpointName());
        inOrder.verify(groupState).getCheckpoint();
        // Verify groupstate is not updated before the checkpoint, but is after.
        inOrder.verify(groupState, Mockito.times(0)).handleEndOfSegment(segmentWithRange);
        event = reader.readNextEvent(0);
        assertFalse(event.isCheckpoint());
        // Now it is called.
        inOrder.verify(groupState, Mockito.times(1)).handleEndOfSegment(segmentWithRange);
        
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
        @Cleanup
        SegmentOutputStream stream = segmentStreamFactory.createOutputStreamForSegment(segment, segmentSealedCallback, writerConfig,
                DelegationTokenProviderFactory.createWithEmptyToken());
        ByteBuffer buffer = writeInt(stream, 1);
        Mockito.when(segmentInputStream2.read(anyLong())).thenReturn(buffer);
        Mockito.when(segmentInputStream2.getSegmentId()).thenReturn(Segment.fromScopedName("Foo/test/0"));
        Mockito.when(segmentInputStream2.getOffset()).thenReturn(10L);

        SegmentInputStreamFactory inputStreamFactory = Mockito.mock(SegmentInputStreamFactory.class);
        Mockito.when(inputStreamFactory.createEventReaderForSegment(any(Segment.class), anyInt(), any(Semaphore.class), anyLong())).thenReturn(segmentInputStream1);
        //Mock Orderer
        Orderer orderer = Mockito.mock(Orderer.class);
        Mockito.when(orderer.nextSegment(any(List.class))).thenReturn(segmentInputStream1).thenReturn(segmentInputStream2);

        @Cleanup
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(inputStreamFactory, segmentStreamFactory,
                new ByteArraySerializer(), groupState,
                orderer, clock::get,
                ReaderConfig.builder().build(), createWatermarkReaders(), Mockito.mock(Controller.class));

        assertThrows(TruncatedDataException.class, () -> reader.readNextEvent(100L));
    }

    @Test(timeout = 10000)
    public void testRead() throws SegmentSealedException, ReaderNotInReaderGroupException {
        AtomicLong clock = new AtomicLong();
        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
        Orderer orderer = new Orderer();
        ReaderGroupStateManager groupState = Mockito.mock(ReaderGroupStateManager.class);
        @Cleanup
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(segmentStreamFactory, segmentStreamFactory,
                                                                           new ByteArraySerializer(), groupState,
                                                                           orderer, clock::get,
                                                                           ReaderConfig.builder().build(),
                                                                           createWatermarkReaders(),
                                                                           Mockito.mock(Controller.class));
        SegmentWithRange segment = new SegmentWithRange(Segment.fromScopedName("Foo/Bar/0"), 0, 1);
        Mockito.when(groupState.acquireNewSegmentsIfNeeded(eq(0L), any())).thenReturn(ImmutableMap.of(segment, 0L)).thenReturn(Collections.emptyMap());
        Mockito.when(groupState.getEndOffsetForSegment(any(Segment.class))).thenReturn(Long.MAX_VALUE);
        @Cleanup
        SegmentOutputStream stream = segmentStreamFactory.createOutputStreamForSegment(segment.getSegment(), segmentSealedCallback, writerConfig,
                DelegationTokenProviderFactory.createWithEmptyToken());
        ByteBuffer buffer1 = writeInt(stream, 1);
        ByteBuffer buffer2 = writeInt(stream, 2);
        ByteBuffer buffer3 = writeInt(stream, 3);
        EventRead<byte[]> e = reader.readNextEvent(0);
        StringBuilder name = new StringBuilder();
        name.append("Foo");
        name.append("/");
        name.append("Bar");
        assertEquals(buffer1, ByteBuffer.wrap(e.getEvent()));
        assertEquals(name.toString(), e.getEventPointer().getStream().getScopedName());
        assertEquals(Long.valueOf(WireCommands.TYPE_PLUS_LENGTH_SIZE + Integer.BYTES),
                e.getPosition().asImpl().getOffsetForOwnedSegment(Segment.fromScopedName("Foo/Bar/0")));
        e = reader.readNextEvent(0);
        assertEquals(buffer2, ByteBuffer.wrap(e.getEvent()));
        assertEquals(name.toString(), e.getEventPointer().getStream().getScopedName());
        assertEquals(Long.valueOf(2 * (WireCommands.TYPE_PLUS_LENGTH_SIZE + Integer.BYTES)),
                e.getPosition().asImpl().getOffsetForOwnedSegment(Segment.fromScopedName("Foo/Bar/0")));
        e = reader.readNextEvent(0);
        assertEquals(buffer3, ByteBuffer.wrap(e.getEvent()));
        assertEquals(name.toString(), e.getEventPointer().getStream().getScopedName());
        assertEquals(Long.valueOf(3 * (WireCommands.TYPE_PLUS_LENGTH_SIZE + Integer.BYTES)),
                e.getPosition().asImpl().getOffsetForOwnedSegment(Segment.fromScopedName("Foo/Bar/0")));
        assertEquals(name.toString(), e.getEventPointer().getStream().getScopedName());
        e = reader.readNextEvent(0);
        assertNull(e.getEvent());
        assertNull(e.getEventPointer());
        assertEquals(Long.valueOf(-1), e.getPosition().asImpl().getOffsetForOwnedSegment(Segment.fromScopedName("Foo/Bar/0")));
        reader.close();
    }

    @Test(timeout = 10000)
    public void testReleaseSegment() throws SegmentSealedException, ReaderNotInReaderGroupException {
        AtomicLong clock = new AtomicLong();
        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
        Orderer orderer = new Orderer();
        ReaderGroupStateManager groupState = Mockito.mock(ReaderGroupStateManager.class);
        @Cleanup
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(segmentStreamFactory, segmentStreamFactory,
                                                                           new ByteArraySerializer(), groupState,
                                                                           orderer, clock::get,
                                                                           ReaderConfig.builder().build(),
                                                                           createWatermarkReaders(),
                                                                           Mockito.mock(Controller.class));
        Segment segment1 = Segment.fromScopedName("Foo/Bar/0");
        Segment segment2 = Segment.fromScopedName("Foo/Bar/1");
        Mockito.when(groupState.acquireNewSegmentsIfNeeded(eq(0L), any()))
               .thenReturn(ImmutableMap.of(new SegmentWithRange(segment1, 0, 0.5), 0L, new SegmentWithRange(segment2, 0, 0.5), 0L))
               .thenReturn(Collections.emptyMap());
        Mockito.when(groupState.getEndOffsetForSegment(any(Segment.class))).thenReturn(Long.MAX_VALUE);
        @Cleanup
        SegmentOutputStream stream1 = segmentStreamFactory.createOutputStreamForSegment(segment1, segmentSealedCallback, writerConfig,
                DelegationTokenProviderFactory.createWithEmptyToken());
        @Cleanup
        SegmentOutputStream stream2 = segmentStreamFactory.createOutputStreamForSegment(segment2, segmentSealedCallback, writerConfig,
                DelegationTokenProviderFactory.createWithEmptyToken());
        writeInt(stream1, 1);
        writeInt(stream2, 2);
        reader.readNextEvent(0);
        List<EventSegmentReader> readers = reader.getReaders();
        assertEquals(2, readers.size());
        Assert.assertEquals(segment1, readers.get(0).getSegmentId());
        Assert.assertEquals(segment2, readers.get(1).getSegmentId());

        //Checkpoint is required to release a segment.
        Mockito.when(groupState.getCheckpoint()).thenReturn("checkpoint");
        assertTrue(reader.readNextEvent(0).isCheckpoint());
        Mockito.when(groupState.getCheckpoint()).thenReturn(null);
        Mockito.when(groupState.findSegmentToReleaseIfRequired()).thenReturn(segment2);
        Mockito.when(groupState.releaseSegment(Mockito.eq(segment2), anyLong(), anyLong(), any())).thenReturn(true);
        assertFalse(reader.readNextEvent(0).isCheckpoint());
        Mockito.verify(groupState).releaseSegment(Mockito.eq(segment2), anyLong(), anyLong(), any());
        readers = reader.getReaders();
        assertEquals(1, readers.size());
        Assert.assertEquals(segment1, readers.get(0).getSegmentId());
        reader.close();
    }

    private ByteBuffer writeInt(SegmentOutputStream stream, int value) {
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
        @Cleanup
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(segmentStreamFactory, segmentStreamFactory,
                                                                           new ByteArraySerializer(), groupState,
                                                                           orderer, clock::get,
                                                                           ReaderConfig.builder().build(),
                                                                           createWatermarkReaders(),
                                                                           Mockito.mock(Controller.class));
        Segment segment1 = Segment.fromScopedName("Foo/Bar/0");
        Segment segment2 = Segment.fromScopedName("Foo/Bar/1");
        Mockito.when(groupState.canAcquireSegmentIfNeeded()).thenReturn(true);
        Mockito.when(groupState.acquireNewSegmentsIfNeeded(eq(0L), any()))
               .thenReturn(ImmutableMap.of(new SegmentWithRange(segment1, 0, 0.5), 0L))
               .thenReturn(ImmutableMap.of(new SegmentWithRange(segment2, 0.5, 1.0), 0L))
               .thenReturn(Collections.emptyMap());
        Mockito.when(groupState.getEndOffsetForSegment(any(Segment.class))).thenReturn(Long.MAX_VALUE);
        @Cleanup
        SegmentOutputStream stream1 = segmentStreamFactory.createOutputStreamForSegment(segment1, segmentSealedCallback, writerConfig,
                DelegationTokenProviderFactory.createWithEmptyToken());
        @Cleanup
        SegmentOutputStream stream2 = segmentStreamFactory.createOutputStreamForSegment(segment2, segmentSealedCallback, writerConfig,
                DelegationTokenProviderFactory.createWithEmptyToken());
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
    
    @Test(timeout = 10000)
    public void testAcquireSealedSegment() throws SegmentSealedException, ReaderNotInReaderGroupException {
        AtomicLong clock = new AtomicLong();
        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
        Orderer orderer = new Orderer();
        ReaderGroupStateManager groupState = Mockito.mock(ReaderGroupStateManager.class);
        @Cleanup
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(segmentStreamFactory, segmentStreamFactory,
                                                                           new ByteArraySerializer(), groupState,
                                                                           orderer, clock::get,
                                                                           ReaderConfig.builder().build(),
                                                                           createWatermarkReaders(),
                                                                           Mockito.mock(Controller.class));
        Segment segment1 = Segment.fromScopedName("Foo/Bar/0");
        Mockito.when(groupState.acquireNewSegmentsIfNeeded(eq(0L), any()))
               .thenReturn(ImmutableMap.of(new SegmentWithRange(segment1, 0, 0.5), -1L))
               .thenReturn(Collections.emptyMap());
        Mockito.when(groupState.getEndOffsetForSegment(any(Segment.class))).thenReturn(Long.MAX_VALUE);
        @Cleanup
        SegmentOutputStream stream1 = segmentStreamFactory.createOutputStreamForSegment(segment1, segmentSealedCallback, writerConfig,
                DelegationTokenProviderFactory.createWithEmptyToken());
        writeInt(stream1, 1);
        writeInt(stream1, 2);
        reader.readNextEvent(0);
        List<EventSegmentReader> readers = reader.getReaders();
        assertEquals(0, readers.size());

        reader.readNextEvent(0);
        readers = reader.getReaders();
        assertEquals(0, readers.size());
        reader.close();
    }
    
    @Test
    public void testEventPointer() throws SegmentSealedException, NoSuchEventException, ReaderNotInReaderGroupException {
        AtomicLong clock = new AtomicLong();
        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
        Orderer orderer = new Orderer();
        ReaderGroupStateManager groupState = Mockito.mock(ReaderGroupStateManager.class);
        @Cleanup
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(segmentStreamFactory, segmentStreamFactory,
                                                                           new ByteArraySerializer(), groupState,
                                                                           orderer, clock::get,
                                                                           ReaderConfig.builder().build(),
                                                                           createWatermarkReaders(),
                                                                           Mockito.mock(Controller.class));
        Segment segment = Segment.fromScopedName("Foo/Bar/0");
        Mockito.when(groupState.acquireNewSegmentsIfNeeded(eq(0L), any()))
               .thenReturn(ImmutableMap.of(new SegmentWithRange(segment, 0, 1), 0L))
               .thenReturn(Collections.emptyMap());
        Mockito.when(groupState.getEndOffsetForSegment(any(Segment.class))).thenReturn(Long.MAX_VALUE);
        @Cleanup
        SegmentOutputStream stream = segmentStreamFactory.createOutputStreamForSegment(segment, segmentSealedCallback,
                writerConfig, DelegationTokenProviderFactory.createWithEmptyToken());
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
        @Cleanup
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(segmentStreamFactory, segmentStreamFactory,
                                                                           new ByteArraySerializer(), groupState,
                                                                           orderer, clock::get,
                                                                           ReaderConfig.builder().build(),
                                                                           createWatermarkReaders(),
                                                                           Mockito.mock(Controller.class));
        Segment segment = Segment.fromScopedName("Foo/Bar/0");
        Mockito.when(groupState.acquireNewSegmentsIfNeeded(eq(0L), any()))
               .thenReturn(ImmutableMap.of(new SegmentWithRange(segment, 0, 1), 0L))
               .thenReturn(Collections.emptyMap());
        Mockito.when(groupState.getEndOffsetForSegment(any(Segment.class))).thenReturn(Long.MAX_VALUE);
        @Cleanup
        SegmentOutputStream stream = segmentStreamFactory.createOutputStreamForSegment(segment, segmentSealedCallback,
                writerConfig, DelegationTokenProviderFactory.createWithEmptyToken());
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
    public void testSilentCheckpointFollowingCheckpoint() throws SegmentSealedException, ReaderNotInReaderGroupException {
        AtomicLong clock = new AtomicLong();
        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
        Orderer orderer = new Orderer();
        ReaderGroupStateManager groupState = Mockito.mock(ReaderGroupStateManager.class);
        @Cleanup
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(segmentStreamFactory, segmentStreamFactory,
                                                                           new ByteArraySerializer(), groupState,
                                                                           orderer, clock::get,
                                                                           ReaderConfig.builder().build(),
                                                                           createWatermarkReaders(),
                                                                           Mockito.mock(Controller.class));
        Segment segment = Segment.fromScopedName("Foo/Bar/0");
        Mockito.when(groupState.acquireNewSegmentsIfNeeded(eq(0L), any()))
               .thenReturn(ImmutableMap.of(new SegmentWithRange(segment, 0, 1), 0L))
               .thenReturn(Collections.emptyMap());
        Mockito.when(groupState.getEndOffsetForSegment(any(Segment.class))).thenReturn(Long.MAX_VALUE);
        @Cleanup
        SegmentOutputStream stream = segmentStreamFactory.createOutputStreamForSegment(segment, segmentSealedCallback,
                writerConfig, DelegationTokenProviderFactory.createWithEmptyToken());
        ByteBuffer buffer = writeInt(stream, 1);
        Mockito.doReturn(true).when(groupState).isCheckpointSilent(Mockito.eq(ReaderGroupImpl.SILENT + "Foo"));
        Mockito.when(groupState.getCheckpoint())
               .thenReturn("Bar")
               .thenReturn(ReaderGroupImpl.SILENT + "Foo")
               .thenReturn(null);
        EventRead<byte[]> eventRead = reader.readNextEvent(10000);
        assertTrue(eventRead.isCheckpoint());
        assertNull(eventRead.getEvent());
        assertEquals("Bar", eventRead.getCheckpointName());
        assertEquals(buffer, ByteBuffer.wrap(reader.readNextEvent(0).getEvent()));
        InOrder order = Mockito.inOrder(groupState);
        order.verify(groupState).getCheckpoint();
        order.verify(groupState).checkpoint(Mockito.eq("Bar"), Mockito.any());
        order.verify(groupState).getCheckpoint();
        order.verify(groupState).checkpoint(Mockito.eq(ReaderGroupImpl.SILENT + "Foo"), Mockito.any());
        order.verify(groupState).getCheckpoint();
        reader.close();
    }
    
    @Test(timeout = 10000)
    public void testCheckpointFollowingSilentCheckpoint() throws SegmentSealedException, ReaderNotInReaderGroupException {
        AtomicLong clock = new AtomicLong();
        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
        Orderer orderer = new Orderer();
        ReaderGroupStateManager groupState = Mockito.mock(ReaderGroupStateManager.class);
        @Cleanup
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(segmentStreamFactory, segmentStreamFactory,
                                                                           new ByteArraySerializer(), groupState,
                                                                           orderer, clock::get,
                                                                           ReaderConfig.builder().build(),
                                                                           createWatermarkReaders(),
                                                                           Mockito.mock(Controller.class));
        Segment segment = Segment.fromScopedName("Foo/Bar/0");
        Mockito.when(groupState.acquireNewSegmentsIfNeeded(eq(0L), any()))
               .thenReturn(ImmutableMap.of(new SegmentWithRange(segment, 0, 1), 0L))
               .thenReturn(Collections.emptyMap());
        Mockito.when(groupState.getEndOffsetForSegment(any(Segment.class))).thenReturn(Long.MAX_VALUE);
        @Cleanup
        SegmentOutputStream stream = segmentStreamFactory.createOutputStreamForSegment(segment, segmentSealedCallback,
                writerConfig, DelegationTokenProviderFactory.createWithEmptyToken());
        ByteBuffer buffer = writeInt(stream, 1);
        Mockito.doReturn(true).when(groupState).isCheckpointSilent(Mockito.eq(ReaderGroupImpl.SILENT + "Foo"));
        Mockito.when(groupState.getCheckpoint())
               .thenReturn(ReaderGroupImpl.SILENT + "Foo")
               .thenReturn("Bar")
               .thenReturn(null);
        EventRead<byte[]> eventRead = reader.readNextEvent(10000);
        assertTrue(eventRead.isCheckpoint());
        assertNull(eventRead.getEvent());
        assertEquals("Bar", eventRead.getCheckpointName());
        InOrder order = Mockito.inOrder(groupState);
        order.verify(groupState).getCheckpoint();
        order.verify(groupState).checkpoint(Mockito.eq(ReaderGroupImpl.SILENT + "Foo"), Mockito.any());
        assertEquals(buffer, ByteBuffer.wrap(reader.readNextEvent(0).getEvent()));
        order.verify(groupState).getCheckpoint();
        order.verify(groupState).checkpoint(Mockito.eq("Bar"), Mockito.any());
        order.verify(groupState).getCheckpoint();
        reader.close();
    }
    
    @Test(timeout = 10000)
    public void testSilentCheckpointFollowingCheckpointFollowingSilentCheckpoint() throws SegmentSealedException, ReaderNotInReaderGroupException {
        AtomicLong clock = new AtomicLong();
        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
        Orderer orderer = new Orderer();
        ReaderGroupStateManager groupState = Mockito.mock(ReaderGroupStateManager.class);
        @Cleanup
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(segmentStreamFactory, segmentStreamFactory,
                                                                           new ByteArraySerializer(), groupState,
                                                                           orderer, clock::get,
                                                                           ReaderConfig.builder().build(),
                                                                           createWatermarkReaders(),
                                                                           Mockito.mock(Controller.class));
        Segment segment = Segment.fromScopedName("Foo/Bar/0");
        Mockito.when(groupState.acquireNewSegmentsIfNeeded(eq(0L), any()))
               .thenReturn(ImmutableMap.of(new SegmentWithRange(segment, 0, 1), 0L))
               .thenReturn(Collections.emptyMap());
        @Cleanup
        SegmentOutputStream stream = segmentStreamFactory.createOutputStreamForSegment(segment, segmentSealedCallback,
                                                                                       writerConfig, DelegationTokenProviderFactory.createWithEmptyToken());
        ByteBuffer buffer = writeInt(stream, 1);
        Mockito.doReturn(true).when(groupState).isCheckpointSilent(Mockito.startsWith(ReaderGroupImpl.SILENT));
        Mockito.when(groupState.getCheckpoint())
               .thenReturn(ReaderGroupImpl.SILENT + "Foo")
               .thenReturn("Bar")
               .thenReturn(ReaderGroupImpl.SILENT + "Baz")
               .thenReturn(null);
        Mockito.when(groupState.getEndOffsetForSegment(any(Segment.class))).thenReturn(Long.MAX_VALUE);
        EventRead<byte[]> eventRead = reader.readNextEvent(10000);
        assertTrue(eventRead.isCheckpoint());
        assertNull(eventRead.getEvent());
        assertEquals("Bar", eventRead.getCheckpointName());
        InOrder order = Mockito.inOrder(groupState);
        order.verify(groupState).getCheckpoint();
        order.verify(groupState).checkpoint(Mockito.eq(ReaderGroupImpl.SILENT + "Foo"), Mockito.any());
        assertEquals(buffer, ByteBuffer.wrap(reader.readNextEvent(0).getEvent()));
        order.verify(groupState).getCheckpoint();
        order.verify(groupState).checkpoint(Mockito.eq("Bar"), Mockito.any());
        order.verify(groupState).getCheckpoint();
        order.verify(groupState).checkpoint(Mockito.eq(ReaderGroupImpl.SILENT + "Baz"), Mockito.any());
        order.verify(groupState).findSegmentToReleaseIfRequired();
        order.verify(groupState).getCheckpoint();
        reader.close();
    }
    
    @Test(timeout = 10000)
    public void testCheckpointFollowingSilentCheckpointFollowingCheckpoint() throws SegmentSealedException, ReaderNotInReaderGroupException {
        AtomicLong clock = new AtomicLong();
        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
        Orderer orderer = new Orderer();
        ReaderGroupStateManager groupState = Mockito.mock(ReaderGroupStateManager.class);
        @Cleanup
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(segmentStreamFactory, segmentStreamFactory,
                                                                           new ByteArraySerializer(), groupState,
                                                                           orderer, clock::get,
                                                                           ReaderConfig.builder().build(),
                                                                           createWatermarkReaders(),
                                                                           Mockito.mock(Controller.class));
        Segment segment = Segment.fromScopedName("Foo/Bar/0");
        Mockito.when(groupState.acquireNewSegmentsIfNeeded(eq(0L), any()))
               .thenReturn(ImmutableMap.of(new SegmentWithRange(segment, 0, 1), 0L))
               .thenReturn(Collections.emptyMap());
        @Cleanup
        SegmentOutputStream stream = segmentStreamFactory.createOutputStreamForSegment(segment, segmentSealedCallback,
                                                                                       writerConfig, DelegationTokenProviderFactory.createWithEmptyToken());
        ByteBuffer buffer = writeInt(stream, 1);
        Mockito.doReturn(true).when(groupState).isCheckpointSilent(Mockito.startsWith(ReaderGroupImpl.SILENT));
        Mockito.when(groupState.getCheckpoint())
               .thenReturn("Foo")
               .thenReturn(ReaderGroupImpl.SILENT + "Bar")
               .thenReturn("Baz")
               .thenReturn(null);
        Mockito.when(groupState.getEndOffsetForSegment(any(Segment.class))).thenReturn(Long.MAX_VALUE);
        EventRead<byte[]> eventRead = reader.readNextEvent(10000);
        assertTrue(eventRead.isCheckpoint());
        assertNull(eventRead.getEvent());
        assertEquals("Foo", eventRead.getCheckpointName());
        eventRead = reader.readNextEvent(0);
        assertTrue(eventRead.isCheckpoint());
        assertNull(eventRead.getEvent());
        assertEquals("Baz", eventRead.getCheckpointName());
        InOrder order = Mockito.inOrder(groupState);
        order.verify(groupState).getCheckpoint();
        order.verify(groupState).checkpoint(Mockito.eq("Foo"), Mockito.any());
        assertEquals(buffer, ByteBuffer.wrap(reader.readNextEvent(0).getEvent()));
        order.verify(groupState).getCheckpoint();
        order.verify(groupState).checkpoint(Mockito.eq(ReaderGroupImpl.SILENT + "Bar"), Mockito.any());
        order.verify(groupState).getCheckpoint();
        order.verify(groupState).checkpoint(Mockito.eq("Baz"), Mockito.any());
        order.verify(groupState).findSegmentToReleaseIfRequired();
        order.verify(groupState).getCheckpoint();
        reader.close();
    }
    
    @Test(timeout = 10000)
    public void testRestore() throws SegmentSealedException, ReaderNotInReaderGroupException {
        AtomicLong clock = new AtomicLong();
        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
        Orderer orderer = new Orderer();
        ReaderGroupStateManager groupState = Mockito.mock(ReaderGroupStateManager.class);
        @Cleanup
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(segmentStreamFactory, segmentStreamFactory,
                                                                           new ByteArraySerializer(), groupState,
                                                                           orderer, clock::get,
                                                                           ReaderConfig.builder().build(), createWatermarkReaders(),
                                                                           Mockito.mock(Controller.class));
        Segment segment = Segment.fromScopedName("Foo/Bar/0");
        Mockito.when(groupState.acquireNewSegmentsIfNeeded(eq(0L), any()))
               .thenReturn(ImmutableMap.of(new SegmentWithRange(segment, 0, 1), 0L))
               .thenReturn(Collections.emptyMap());
        @Cleanup
        SegmentOutputStream stream = segmentStreamFactory.createOutputStreamForSegment(segment, segmentSealedCallback,
                                                                                       writerConfig, DelegationTokenProviderFactory.createWithEmptyToken());
        writeInt(stream, 1);
        Mockito.when(groupState.getCheckpoint()).thenThrow(new ReinitializationRequiredException());
        assertThrows(ReinitializationRequiredException.class, () -> reader.readNextEvent(0)); 
        assertTrue(reader.getReaders().isEmpty());
        reader.close();
    }

    @Test(timeout = 10000)
    public void testDataTruncated() throws SegmentSealedException, ReaderNotInReaderGroupException {
        AtomicLong clock = new AtomicLong();
        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
        Orderer orderer = new Orderer();
        ReaderGroupStateManager groupState = Mockito.mock(ReaderGroupStateManager.class);
        @Cleanup
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(segmentStreamFactory, segmentStreamFactory,
                                                                           new ByteArraySerializer(), groupState,
                                                                           orderer, clock::get,
                                                                           ReaderConfig.builder().build(),
                                                                           createWatermarkReaders(), Mockito.mock(Controller.class));
        Segment segment = Segment.fromScopedName("Foo/Bar/0");
        Mockito.when(groupState.acquireNewSegmentsIfNeeded(eq(0L), any()))
               .thenReturn(ImmutableMap.of(new SegmentWithRange(segment, 0, 1), 0L))
               .thenReturn(Collections.emptyMap());
        Mockito.when(groupState.getEndOffsetForSegment(any(Segment.class))).thenReturn(Long.MAX_VALUE);
        @Cleanup
        SegmentOutputStream stream = segmentStreamFactory.createOutputStreamForSegment(segment, segmentSealedCallback,
                                                                                       writerConfig, DelegationTokenProviderFactory.createWithEmptyToken());
        @Cleanup
        SegmentMetadataClient metadataClient = segmentStreamFactory.createSegmentMetadataClient(segment,
                DelegationTokenProviderFactory.createWithEmptyToken());
        ByteBuffer buffer1 = writeInt(stream, 1);
        ByteBuffer buffer2 = writeInt(stream, 2);
        writeInt(stream, 3);
        long length = metadataClient.fetchCurrentSegmentLength().join();
        assertEquals(0, length % 3);
        EventRead<byte[]> event1 = reader.readNextEvent(0);
        assertEquals(buffer1, ByteBuffer.wrap(event1.getEvent()));
        metadataClient.truncateSegment(length / 3).join();
        assertEquals(buffer2, ByteBuffer.wrap(reader.readNextEvent(0).getEvent()));
        metadataClient.truncateSegment(length).join();
        ByteBuffer buffer4 = writeInt(stream, 4);
        assertThrows(TruncatedDataException.class, () -> reader.readNextEvent(0));
        assertEquals(buffer4, ByteBuffer.wrap(reader.readNextEvent(0).getEvent()));
        assertNull(reader.readNextEvent(0).getEvent());
        assertThrows(NoSuchEventException.class, () -> reader.fetchEvent(event1.getEventPointer()));
        reader.close();
    }

    @Test(timeout = 10000L)
    public void testThrowingServerTimeoutException() throws Exception {
        AtomicLong clock = new AtomicLong();
        Segment segment = Segment.fromScopedName("Foo/Bar/0");
        SegmentWithRange rangedSegment = new SegmentWithRange(segment, 0, 1);
        // Setup mock.
        SegmentInputStreamFactory segInputStreamFactory = Mockito.mock(SegmentInputStreamFactory.class);
        SegmentMetadataClientFactory segmentMetadataClientFactory = Mockito.mock(SegmentMetadataClientFactory.class);
        SegmentMetadataClient metadataClient = Mockito.mock(SegmentMetadataClient.class);
        EventSegmentReader segmentInputStream = Mockito.mock(EventSegmentReader.class);
        Mockito.when(segmentMetadataClientFactory.createSegmentMetadataClient(any(Segment.class), any())).thenReturn(metadataClient);
        Mockito.when(segmentInputStream.getSegmentId()).thenReturn(segment);
        Mockito.when(segInputStreamFactory.createEventReaderForSegment(any(Segment.class), anyInt(), any(Semaphore.class), anyLong())).thenReturn(segmentInputStream);
        Mockito.when(segmentInputStream.isSegmentReady()).thenReturn(true);
        Mockito.when(segmentInputStream.read(anyLong())).thenThrow(SegmentTruncatedException.class);
        // Ensure SegmentInfo returns incompleteFuture.
        CompletableFuture<SegmentInfo> incompleteFuture = new CompletableFuture();
        Mockito.when(metadataClient.getSegmentInfo()).thenReturn(incompleteFuture);

        Orderer orderer = new Orderer();
        ReaderGroupStateManager groupState = Mockito.mock(ReaderGroupStateManager.class);
        Mockito.when(groupState.getEndOffsetForSegment(any(Segment.class))).thenReturn(Long.MAX_VALUE);
        @Cleanup
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(segInputStreamFactory, segmentMetadataClientFactory,
                new ByteArraySerializer(), groupState,
                orderer, clock::get,
                ReaderConfig.builder().build(), createWatermarkReaders(), Mockito.mock(Controller.class));
        Mockito.when(groupState.acquireNewSegmentsIfNeeded(eq(0L), any()))
                .thenReturn(ImmutableMap.of(rangedSegment, 0L))
                .thenReturn(Collections.emptyMap());
        InOrder inOrder = Mockito.inOrder(groupState, segmentInputStream);
        // It will wait for the mentioned time in readNextEvent if it is not able to read then it will print
        // a Warn message and throw truncatedDataException
        AssertExtensions.assertThrows(TruncatedDataException.class, () -> reader.readNextEvent(1000));
    }

    /**
     * This test tests a scenario where a lagging reader's SegmentInputStream receives a SegmentTruncatedException and
     * when the reader tries to fetch the start offset of this segment the SegmentStore returns a NoSuchSegmentException.
     */
    @Test
    public void testTruncatedSegmentDeleted() throws Exception {
        AtomicLong clock = new AtomicLong();
        Segment segment = Segment.fromScopedName("Foo/Bar/0");
        SegmentWithRange rangedSegment = new SegmentWithRange(segment, 0, 1);
        // Setup mock.
        SegmentInputStreamFactory segInputStreamFactory = Mockito.mock(SegmentInputStreamFactory.class);
        SegmentMetadataClientFactory segmentMetadataClientFactory = Mockito.mock(SegmentMetadataClientFactory.class);
        SegmentMetadataClient metadataClient = Mockito.mock(SegmentMetadataClient.class);
        EventSegmentReader segmentInputStream = Mockito.mock(EventSegmentReader.class);
        Mockito.when(segmentMetadataClientFactory.createSegmentMetadataClient(any(Segment.class), any())).thenReturn(metadataClient);
        Mockito.when(segmentInputStream.getSegmentId()).thenReturn(segment);
        Mockito.when(segInputStreamFactory.createEventReaderForSegment(any(Segment.class), anyInt(), any(Semaphore.class), anyLong())).thenReturn(segmentInputStream);
        // Ensure segmentInputStream.read() returns SegmentTruncatedException.
        Mockito.when(segmentInputStream.isSegmentReady()).thenReturn(true);
        Mockito.when(segmentInputStream.read(anyLong())).thenThrow(SegmentTruncatedException.class);
        // Ensure SegmentInfo returns NoSuchSegmentException.
        Mockito.when(metadataClient.getSegmentInfo()).thenThrow(NoSuchSegmentException.class);

        Orderer orderer = new Orderer();
        ReaderGroupStateManager groupState = Mockito.mock(ReaderGroupStateManager.class);
        Mockito.when(groupState.getEndOffsetForSegment(any(Segment.class))).thenReturn(Long.MAX_VALUE);
        @Cleanup
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(segInputStreamFactory, segmentMetadataClientFactory,
                new ByteArraySerializer(), groupState,
                orderer, clock::get,
                ReaderConfig.builder().build(), createWatermarkReaders(), Mockito.mock(Controller.class));
        Mockito.when(groupState.acquireNewSegmentsIfNeeded(eq(0L), any()))
                .thenReturn(ImmutableMap.of(rangedSegment, 0L))
                .thenReturn(Collections.emptyMap());
        InOrder inOrder = Mockito.inOrder(groupState, segmentInputStream);
        // Validate that TruncatedDataException is thrown.
        AssertExtensions.assertThrows(TruncatedDataException.class, () -> reader.readNextEvent(0));
        inOrder.verify(groupState).getCheckpoint();
        // Ensure this segment is closed.
        inOrder.verify(segmentInputStream, Mockito.times(1)).close();      
        // Ensure groupstate is not updated before the checkpoint.
        inOrder.verify(groupState, Mockito.times(0)).handleEndOfSegment(rangedSegment);
        Mockito.when(groupState.getCheckpoint()).thenReturn("Foo").thenReturn(null);
        EventRead<byte[]> event = reader.readNextEvent(0);
        assertTrue(event.isCheckpoint());
        assertEquals("Foo", event.getCheckpointName());
        inOrder.verify(groupState).getCheckpoint();
        // Verify groupstate is not updated before the checkpoint, but is after.
        inOrder.verify(groupState, Mockito.times(0)).handleEndOfSegment(rangedSegment);
        event = reader.readNextEvent(0);
        assertFalse(event.isCheckpoint());
        assertNull(event.getEvent());
        // Now it is called.
        inOrder.verify(groupState).handleEndOfSegment(rangedSegment);
    }
    
    @Test(timeout = 10000)
    public void testSegmentSplit() throws EndOfSegmentException, SegmentTruncatedException, SegmentSealedException, ReaderNotInReaderGroupException {
        AtomicLong clock = new AtomicLong();
        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();

        //Prep the mocks.
        ReaderGroupStateManager groupState = Mockito.mock(ReaderGroupStateManager.class);

        //Mock for the two SegmentInputStreams.
        Segment segment1 = Segment.fromScopedName("Foo/Bar/1");
        SegmentWithRange s1range = new SegmentWithRange(segment1, 0, 1);
        EventSegmentReader segmentInputStream1 = Mockito.mock(EventSegmentReader.class);
        Mockito.when(segmentInputStream1.isSegmentReady()).thenReturn(true);
        Mockito.when(segmentInputStream1.read(anyLong())).thenThrow(new EndOfSegmentException(EndOfSegmentException.ErrorType.END_OF_SEGMENT_REACHED));
        Mockito.when(segmentInputStream1.getSegmentId()).thenReturn(segment1);

        Segment segment2 = Segment.fromScopedName("Foo/Bar/2");
        SegmentWithRange s2range = new SegmentWithRange(segment2, 0, 0.5);
        EventSegmentReader segmentInputStream2 = Mockito.mock(EventSegmentReader.class);
        @Cleanup
        SegmentOutputStream stream2 = segmentStreamFactory.createOutputStreamForSegment(segment2, segmentSealedCallback, writerConfig, DelegationTokenProviderFactory.createWithEmptyToken());
        Mockito.when(segmentInputStream2.read(anyLong())).thenReturn(writeInt(stream2, 2));
        Mockito.when(segmentInputStream2.getSegmentId()).thenReturn(segment2);
        
        Segment segment3 = Segment.fromScopedName("Foo/Bar/3");
        SegmentWithRange s3range = new SegmentWithRange(segment3, 0.5, 1.0);
        EventSegmentReader segmentInputStream3 = Mockito.mock(EventSegmentReader.class);
        @Cleanup
        SegmentOutputStream stream3 = segmentStreamFactory.createOutputStreamForSegment(segment3, segmentSealedCallback, writerConfig, DelegationTokenProviderFactory.createWithEmptyToken());
        Mockito.when(segmentInputStream3.read(anyLong())).thenReturn(writeInt(stream3, 3));
        Mockito.when(segmentInputStream3.getSegmentId()).thenReturn(segment3);

        SegmentInputStreamFactory inputStreamFactory = Mockito.mock(SegmentInputStreamFactory.class);
        Mockito.when(inputStreamFactory.createEventReaderForSegment(eq(segment1), anyInt(), any(Semaphore.class), eq(Long.MAX_VALUE))).thenReturn(segmentInputStream1);
        Mockito.when(inputStreamFactory.createEventReaderForSegment(eq(segment2), anyInt(), any(Semaphore.class), eq(Long.MAX_VALUE))).thenReturn(segmentInputStream2);
        Mockito.when(inputStreamFactory.createEventReaderForSegment(eq(segment3), anyInt(), any(Semaphore.class), eq(Long.MAX_VALUE))).thenReturn(segmentInputStream3);     
        
        Mockito.when(groupState.getEndOffsetForSegment(any())).thenReturn(Long.MAX_VALUE);
        
        @Cleanup
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(inputStreamFactory, segmentStreamFactory,
                                                                           new ByteArraySerializer(), groupState,
                                                                           new Orderer(), clock::get,
                                                                           ReaderConfig.builder().build(), createWatermarkReaders(),
                                                                           Mockito.mock(Controller.class));

        Mockito.when(groupState.acquireNewSegmentsIfNeeded(anyLong(), any()))
               .thenReturn(ImmutableMap.of(s1range, 0L))
               .thenReturn(Collections.emptyMap());

        InOrder inOrder = Mockito.inOrder(segmentInputStream1, groupState);
        EventRead<byte[]> event = reader.readNextEvent(100L);
        assertNull(event.getEvent());
        event = reader.readNextEvent(100L);
        assertNull(event.getEvent());

        Mockito.when(groupState.getCheckpoint())
               .thenReturn("checkpoint")
               .thenReturn(null);
        Mockito.when(groupState.acquireNewSegmentsIfNeeded(anyLong(), any()))
               .thenReturn(ImmutableMap.of(s2range, 0L, s3range, 0L))
               .thenReturn(Collections.emptyMap());
        assertEquals("checkpoint", reader.readNextEvent(0).getCheckpointName());
        inOrder.verify(groupState).getCheckpoint();
        // Ensure groupstate is not updated before the checkpoint.
        inOrder.verify(groupState, Mockito.times(0)).handleEndOfSegment(s1range);
        event = reader.readNextEvent(0);
        assertFalse(event.isCheckpoint());
        // Now it is called.
        inOrder.verify(groupState, Mockito.times(1)).handleEndOfSegment(s1range);
        assertEquals(ImmutableList.of(segmentInputStream2, segmentInputStream3), reader.getReaders());        
        
    }
    
    @Test
    public void testReaderClose() throws SegmentSealedException {
        String scope = "scope";
        String stream = "stream";
        String groupName = "readerGroup";
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
        SegmentOutputStream stream1 = segmentStreamFactory.createOutputStreamForSegment(segment1, segmentSealedCallback, writerConfig, DelegationTokenProviderFactory.createWithEmptyToken());
        writeInt(stream1, 1);
        writeInt(stream1, 1);
        writeInt(stream1, 1);
        Segment segment2 = new Segment(scope, stream, 1);
        @Cleanup
        SegmentOutputStream stream2 = segmentStreamFactory.createOutputStreamForSegment(segment2, segmentSealedCallback, writerConfig, DelegationTokenProviderFactory.createWithEmptyToken());
        writeInt(stream2, 2);
        writeInt(stream2, 2);
        writeInt(stream2, 2);
        StateSynchronizer<ReaderGroupState> sync = createStateSynchronizerForReaderGroup(connectionFactory, controller,
                                                                                         segmentStreamFactory,
                                                                                         Stream.of(scope, stream),
                                                                                         "reader1", clock, 2, groupName);
        @Cleanup
        EventStreamReaderImpl<byte[]> reader1 = createReader(controller, segmentStreamFactory, "reader1", sync, clock, scope, groupName);
        @Cleanup
        EventStreamReaderImpl<byte[]> reader2 = createReader(controller, segmentStreamFactory, "reader2", sync, clock, scope, groupName);
        
        assertEquals(1, readInt(reader1.readNextEvent(0)));
        assertEquals(2, readInt(reader2.readNextEvent(0)));
        reader2.close();
        clock.addAndGet(ReaderGroupStateManager.UPDATE_WINDOW.toNanos());
        assertEquals(1, readInt(reader1.readNextEvent(0)));
        assertEquals(2, readInt(reader1.readNextEvent(0)));
        assertEquals(1, readInt(reader1.readNextEvent(0)));
        assertEquals(2, readInt(reader1.readNextEvent(0)));
    }
    
    @Test
    public void testPositionsContainSealedSegments() throws SegmentSealedException {
        String scope = "scope";
        String stream = "stream";
        String groupName = "readerGroup";
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
        SegmentOutputStream stream1 = segmentStreamFactory.createOutputStreamForSegment(segment1, segmentSealedCallback, writerConfig, DelegationTokenProviderFactory.createWithEmptyToken());
        writeInt(stream1, 1);
        Segment segment2 = new Segment(scope, stream, 1);
        @Cleanup
        SegmentOutputStream stream2 = segmentStreamFactory.createOutputStreamForSegment(segment2, segmentSealedCallback, writerConfig, DelegationTokenProviderFactory.createWithEmptyToken());
        writeInt(stream2, 2);
        writeInt(stream2, 2);
        writeInt(stream2, 2);
        @Cleanup
        StateSynchronizer<ReaderGroupState> sync = createStateSynchronizerForReaderGroup(connectionFactory, controller,
                                                                                         segmentStreamFactory,
                                                                                         Stream.of(scope, stream),
                                                                                         "reader1", clock, 2, groupName);
        @Cleanup
        EventStreamReaderImpl<byte[]> reader = createReader(controller, segmentStreamFactory, "reader1", sync, clock, scope, groupName);
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
    
    @Test
    public void testTimeWindow() throws SegmentSealedException {
        String scope = "scope";
        String streamName = "stream";
        Stream stream = Stream.of(scope, streamName);
        String groupName = "readerGroup";
        String readerGroupStream = NameUtils.getStreamForReaderGroup(groupName);
        String markStream = NameUtils.getMarkStreamForStream(streamName);
        
        //Create factories
        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
        @Cleanup
        MockClientFactory clientFactory = new MockClientFactory(scope, segmentStreamFactory);
        MockController controller = (MockController) clientFactory.getController();
        @Cleanup
        InlineExecutor executor = new InlineExecutor();
        
        //Create streams
        controller.createScope(scope).join();
        controller.createStream(scope, streamName,
                                StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(2)).build());
        controller.createStream(scope, readerGroupStream,
                                StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
        
        //Reader group state synchronizer
        ReaderGroupConfig config = ReaderGroupConfig.builder().disableAutomaticCheckpoints().stream(stream).build();
        StateSynchronizer<ReaderGroupState> sync = clientFactory.createStateSynchronizer(readerGroupStream,
                                                                                         new ReaderGroupStateUpdatesSerializer(),
                                                                                         new ReaderGroupStateInitSerializer(),
                                                                                         SynchronizerConfig.builder()
                                                                                         .build());
        //Watermark reader/writer
        @Cleanup
        RevisionedStreamClient<Watermark> markWriter = clientFactory.createRevisionedStreamClient(markStream,
                                                                                              new WatermarkSerializer(),
                                                                                              SynchronizerConfig.builder().build());
        @Cleanup
        WatermarkReaderImpl markReader = new WatermarkReaderImpl(stream, markWriter, executor);

        //Initialize reader group state
        Map<SegmentWithRange, Long> segments = ReaderGroupImpl.getSegmentsForStreams(controller, config);
        sync.initialize(new ReaderGroupState.ReaderGroupStateInit(config,
                                                                  segments,
                                                                  getEndSegmentsForStreams(config), false));
        //Data segment writers
        Segment segment1 = new Segment(scope, streamName, 0);
        Segment segment2 = new Segment(scope, streamName, 1);
        @Cleanup
        SegmentOutputStream stream1 = segmentStreamFactory.createOutputStreamForSegment(segment1, segmentSealedCallback, writerConfig, DelegationTokenProviderFactory.createWithEmptyToken());
        @Cleanup
        SegmentOutputStream stream2 = segmentStreamFactory.createOutputStreamForSegment(segment2, segmentSealedCallback, writerConfig, DelegationTokenProviderFactory.createWithEmptyToken());
        
        //Write stream data
        writeInt(stream1, 1);
        writeInt(stream2, 2);
        writeInt(stream2, 2);
        writeInt(stream2, 2);
        
        //Write mark data
        val r1 = new SegmentWithRange(segment1, 0, 0.5).convert();
        val r2 = new SegmentWithRange(segment2, 0.5, 1).convert();
        markWriter.writeUnconditionally(new Watermark(  0L,  99L, ImmutableMap.of(r1, 0L, r2, 0L)));
        markWriter.writeUnconditionally(new Watermark(100L, 199L, ImmutableMap.of(r1, 12L, r2, 0L)));
        markWriter.writeUnconditionally(new Watermark(200L, 299L, ImmutableMap.of(r1, 12L, r2, 12L)));
        markWriter.writeUnconditionally(new Watermark(300L, 399L, ImmutableMap.of(r1, 12L, r2, 24L)));
        markWriter.writeUnconditionally(new Watermark(400L, 499L, ImmutableMap.of(r1, 12L, r2, 36L)));
        
        //Create reader
        AtomicLong clock = new AtomicLong();
        ReaderGroupStateManager groupState = new ReaderGroupStateManager(scope, groupName, "reader1", sync, controller, clock::get);
        groupState.initializeReader(0);
        @Cleanup
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(segmentStreamFactory, segmentStreamFactory,
                                                                           new ByteArraySerializer(), groupState,
                                                                           new Orderer(), clock::get,
                                                                           ReaderConfig.builder().build(),
                                                                           ImmutableMap.of(stream, markReader), Mockito.mock(Controller.class));
        
        clock.addAndGet(ReaderGroupStateManager.UPDATE_WINDOW.toNanos());
        EventRead<byte[]> event = reader.readNextEvent(100);
        assertEquals(2, readInt(event));
        TimeWindow timeWindow = reader.getCurrentTimeWindow(Stream.of(scope, streamName));
        assertEquals(0, timeWindow.getLowerTimeBound().longValue());
        assertEquals(199, timeWindow.getUpperTimeBound().longValue());
        
        clock.addAndGet(ReaderGroupStateManager.UPDATE_WINDOW.toNanos());
        event = reader.readNextEvent(100);
        assertEquals(1, readInt(event));
        timeWindow = reader.getCurrentTimeWindow(Stream.of(scope, streamName));
        assertEquals(0, timeWindow.getLowerTimeBound().longValue());
        assertEquals(299, timeWindow.getUpperTimeBound().longValue());
        
        clock.addAndGet(ReaderGroupStateManager.UPDATE_WINDOW.toNanos());
        event = reader.readNextEvent(100);
        assertEquals(2, readInt(event));
        timeWindow = reader.getCurrentTimeWindow(Stream.of(scope, streamName));
        assertEquals(200, timeWindow.getLowerTimeBound().longValue());
        assertEquals(399, timeWindow.getUpperTimeBound().longValue());
        
        clock.addAndGet(ReaderGroupStateManager.UPDATE_WINDOW.toNanos());
        event = reader.readNextEvent(100);
        assertEquals(2, readInt(event));
        timeWindow = reader.getCurrentTimeWindow(Stream.of(scope, streamName));
        assertEquals(300, timeWindow.getLowerTimeBound().longValue());
        assertEquals(499, timeWindow.getUpperTimeBound().longValue());
        
        clock.addAndGet(ReaderGroupStateManager.UPDATE_WINDOW.toNanos());
        event = reader.readNextEvent(100);
        assertEquals(null, event.getEvent());
        timeWindow = reader.getCurrentTimeWindow(Stream.of(scope, streamName));
        assertEquals(400, timeWindow.getLowerTimeBound().longValue());
        assertEquals(null, timeWindow.getUpperTimeBound());
    }

    @Test(timeout = 10000)
    public void testEndOfStream() throws SegmentSealedException, ReaderNotInReaderGroupException {
        AtomicLong clock = new AtomicLong();
        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
        Orderer orderer = Mockito.mock(Orderer.class);
        ReaderGroupStateManager groupState = Mockito.mock(ReaderGroupStateManager.class);
        @Cleanup
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(segmentStreamFactory, segmentStreamFactory,
                new ByteArraySerializer(), groupState,
                orderer, clock::get,
                ReaderConfig.builder().build(),
                createWatermarkReaders(),
                Mockito.mock(Controller.class));
        Segment segment = Segment.fromScopedName("Foo/Bar/0");
        Mockito.when(groupState.acquireNewSegmentsIfNeeded(eq(0L), any()))
               .thenReturn(ImmutableMap.of(new SegmentWithRange(segment, 0, 1), 0L))
               .thenReturn(Collections.emptyMap());
        Mockito.when(groupState.getEndOffsetForSegment(any(Segment.class))).thenReturn(Long.MAX_VALUE);
        @Cleanup
        SegmentOutputStream stream = segmentStreamFactory.createOutputStreamForSegment(segment, segmentSealedCallback,
                writerConfig, DelegationTokenProviderFactory.createWithEmptyToken());
        Mockito.when(orderer.nextSegment(any(List.class))).thenReturn(null);
        Mockito.when(groupState.getCheckpoint()).thenReturn(null);
        Mockito.when(groupState.reachedEndOfStream()).thenReturn(true);
        EventRead<byte[]> eventRead = reader.readNextEvent(0);
        assertNull(eventRead.getEvent());
        assertTrue(eventRead.isReadCompleted());
        reader.close();
    }

    @Test(timeout = 10000)
    public void testReleaseCompletedSegment() throws SegmentSealedException, ReaderNotInReaderGroupException {
        AtomicLong clock = new AtomicLong();
        MockSegmentStreamFactory segmentStreamFactory = new MockSegmentStreamFactory();
        Orderer orderer = new Orderer();
        ReaderGroupStateManager groupState = Mockito.mock(ReaderGroupStateManager.class);
        @Cleanup
        EventStreamReaderImpl<byte[]> reader = new EventStreamReaderImpl<>(segmentStreamFactory, segmentStreamFactory,
                new ByteArraySerializer(), groupState,
                orderer, clock::get,
                ReaderConfig.builder().build(),
                createWatermarkReaders(),
                Mockito.mock(Controller.class));
        Segment segment1 = Segment.fromScopedName("Foo/Bar/0");
        Segment segment2 = Segment.fromScopedName("Foo/Bar/1");
        Mockito.when(groupState.acquireNewSegmentsIfNeeded(eq(0L), any()))
               .thenReturn(ImmutableMap.of(new SegmentWithRange(segment1, 0, 0.5), 0L, new SegmentWithRange(segment2, 0, 0.5), 0L))
               .thenReturn(Collections.emptyMap());
        Mockito.when(groupState.getEndOffsetForSegment(any(Segment.class))).thenReturn(12L);
        @Cleanup
        SegmentOutputStream stream1 = segmentStreamFactory.createOutputStreamForSegment(segment1, segmentSealedCallback, writerConfig,
                DelegationTokenProviderFactory.createWithEmptyToken());
        @Cleanup
        SegmentOutputStream stream2 = segmentStreamFactory.createOutputStreamForSegment(segment2, segmentSealedCallback, writerConfig,
                DelegationTokenProviderFactory.createWithEmptyToken());
        writeInt(stream1, 1);
        writeInt(stream2, 2);
        reader.readNextEvent(0);
        List<EventSegmentReader> readers = reader.getReaders();
        assertEquals(2, readers.size());
        Assert.assertEquals(segment1, readers.get(0).getSegmentId());
        Assert.assertEquals(segment2, readers.get(1).getSegmentId());

        //Checkpoint is required to release a segment.
        Mockito.when(groupState.getCheckpoint()).thenReturn("checkpoint");
        assertTrue(reader.readNextEvent(0).isCheckpoint());
        Mockito.when(groupState.getCheckpoint()).thenReturn(null);
        Mockito.when(groupState.findSegmentToReleaseIfRequired()).thenReturn(segment1);
        Mockito.when(groupState.releaseSegment(Mockito.eq(segment1), anyLong(), anyLong(), any())).thenReturn(true);
        assertFalse(reader.readNextEvent(0).isCheckpoint());
        Mockito.verify(groupState).releaseSegment(Mockito.eq(segment1), anyLong(), anyLong(), any());
        readers = reader.getReaders();
        assertEquals(0, readers.size());
        reader.close();
    }

    private int readInt(EventRead<byte[]> eventRead) {
        byte[] event = eventRead.getEvent();
        assertNotNull(event);
        return ByteBuffer.wrap(event).getInt();
    }

    private EventStreamReaderImpl<byte[]> createReader(MockController controller,
                                                       MockSegmentStreamFactory segmentStreamFactory, String readerId,
                                                       StateSynchronizer<ReaderGroupState> sync, AtomicLong clock,
                                                       String scope, String groupName) {
        ReaderGroupStateManager groupState = new ReaderGroupStateManager(scope, groupName, readerId, sync, controller, clock::get);
        groupState.initializeReader(0);
        return new EventStreamReaderImpl<>(segmentStreamFactory, segmentStreamFactory, new ByteArraySerializer(),
                                           groupState, new Orderer(), clock::get, ReaderConfig.builder().build(),
                                           ImmutableMap.of(Stream.of("scope/stream"),
                                                           Mockito.mock(WatermarkReaderImpl.class)), Mockito.mock(Controller.class));
    }

    private StateSynchronizer<ReaderGroupState> createStateSynchronizerForReaderGroup(ConnectionPool connectionPool,
                                                                                      Controller controller,
                                                                                      MockSegmentStreamFactory streamFactory,
                                                                                      Stream stream, String readerId,
                                                                                      AtomicLong clock,
                                                                                      int numSegments,
                                                                                      String groupName) {
        String readerGroupStream = NameUtils.getStreamForReaderGroup(groupName);
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                                                              .scalingPolicy(ScalingPolicy.fixed(numSegments))
                                                              .build();
        controller.createScope(stream.getScope());
        controller.createStream(stream.getScope(), stream.getStreamName(), streamConfig);
        controller.createStream(stream.getScope(), readerGroupStream, StreamConfiguration.builder()
                                                                                         .scalingPolicy(ScalingPolicy.fixed(1)).build());
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(stream.getScope(), controller, connectionPool,
                                                                streamFactory, streamFactory, streamFactory,
                                                                streamFactory);
        createdClientFactories.add(clientFactory);
        ReaderGroupConfig config = ReaderGroupConfig.builder().disableAutomaticCheckpoints().stream(stream).build();

        StateSynchronizer<ReaderGroupState> sync = clientFactory.createStateSynchronizer(readerGroupStream,
                                                                                         new ReaderGroupStateUpdatesSerializer(),
                                                                                         new ReaderGroupStateInitSerializer(),
                                                                                         SynchronizerConfig.builder()
                                                                                                           .build());
        Map<SegmentWithRange, Long> segments = ReaderGroupImpl.getSegmentsForStreams(controller, config);
        sync.initialize(new ReaderGroupState.ReaderGroupStateInit(config,
                                                                  segments,
                                                                  getEndSegmentsForStreams(config), false));
        return sync;
    }
    
    private ImmutableMap<Stream, WatermarkReaderImpl> createWatermarkReaders() {
        return ImmutableMap.of(Stream.of("Foo/Bar"), Mockito.mock(WatermarkReaderImpl.class));
    }

}

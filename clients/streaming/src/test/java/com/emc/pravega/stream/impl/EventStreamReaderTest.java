/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.stream.EventRead;
import com.emc.pravega.stream.ReaderConfig;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.impl.segment.SegmentOutputStream;
import com.emc.pravega.stream.impl.segment.SegmentSealedException;
import com.emc.pravega.stream.mock.MockSegmentStreamFactory;
import com.google.common.collect.ImmutableMap;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;

public class EventStreamReaderTest {

    @Test
    public void testRead() throws SegmentSealedException {
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
        Mockito.when(groupState.acquireNewSegmentsIfNeeded(0L)).thenReturn(ImmutableMap.of(segment, 0L));
        SegmentOutputStream stream = segmentStreamFactory.createOutputStreamForSegment(segment);
        ByteBuffer buffer = ByteBuffer.allocate(4).putInt(1);
        buffer.flip();
        stream.write(buffer, new CompletableFuture<Boolean>());
        EventRead<byte[]> read = reader.readNextEvent(0);
        byte[] event = read.getEvent();
        assertEquals(buffer, ByteBuffer.wrap(event));
        reader.close();
    }

}

/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.batch.impl;

import io.pravega.client.batch.SegmentRange;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.StreamCutImpl;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SegmentRangeImplTest {

    @Test(expected = IllegalStateException.class)
    public void testInvalidSegmentRange() {
        //StartOffset > endOffset.
        SegmentRangeImpl.builder().endOffset(10L).startOffset(20L)
                        .segment(new Segment("scope", "stream", 1)).build();
    }

    @Test(expected = NullPointerException.class)
    public void testNullSegment() {
        SegmentRangeImpl.builder().startOffset(10L).endOffset(20L).build();
    }

    @Test
    public void testValid() {
        SegmentRange segmentRange = SegmentRangeImpl.builder().endOffset(20L).segment(new Segment("scope", "stream",
                0)).build();
        assertEquals(0L, segmentRange.getStartOffset());
        assertEquals(20L, segmentRange.getEndOffset());
        assertEquals(new Segment("scope", "stream", 0), ((SegmentRangeImpl) segmentRange).getSegment());
        assertEquals("scope", segmentRange.getScope());
        assertEquals("stream", segmentRange.getStreamName());
    }

    @Test
    public void testFromStreamCutsValid() {
        Stream stream = Stream.of("scope", "stream");

        Segment segment = new Segment(stream.getScope(), stream.getStreamName(), 1L);

        StreamCut startStreamCut = new StreamCutImpl(stream, segment, 100L);
        StreamCut endStreamCut = new StreamCutImpl(stream, segment, 200L);

        SegmentRange segmentRange = SegmentRangeImpl.fromStreamCuts(segment, startStreamCut, endStreamCut);

        assertEquals(1L, segmentRange.getSegmentId());
        assertEquals(100L, segmentRange.getStartOffset());
        assertEquals(200L, segmentRange.getEndOffset());
        assertEquals("scope", segmentRange.getScope());
        assertEquals("stream", segmentRange.getStreamName());
    }

    @Test(expected = IllegalStateException.class)
    public void testFromStreamCutsInvalidOffsets() {
        Stream stream = Stream.of("scope", "stream");

        Segment segment = new Segment(stream.getScope(), stream.getStreamName(), 1L);

        StreamCut startStreamCut = new StreamCutImpl(stream, segment, 100L);
        StreamCut endStreamCut = new StreamCutImpl(stream, segment, 200L);

        // swap start+end args, will fail as end must be > start
        SegmentRangeImpl.fromStreamCuts(segment, endStreamCut, startStreamCut);
    }

    @Test(expected = IllegalStateException.class)
    public void testFromStreamCutsInvalidSegment() {
        Stream stream = Stream.of("scope", "stream");

        Segment segment = new Segment(stream.getScope(), stream.getStreamName(), 1L);

        StreamCut startStreamCut = new StreamCutImpl(stream, segment, 100L);
        StreamCut endStreamCut = new StreamCutImpl(stream, segment, 200L);

        // segment id 2 not found in given stream cuts
        SegmentRangeImpl.fromStreamCuts(new Segment(stream.getScope(), stream.getStreamName(), 2L), startStreamCut, endStreamCut);
    }

    @Test(expected = IllegalStateException.class)
    public void testFromStreamCutsInvalidUnbounded() {
        Stream stream = Stream.of("scope", "stream");

        Segment segment = new Segment(stream.getScope(), stream.getStreamName(), 1L);

        StreamCut startStreamCut = new StreamCutImpl(stream, segment, 100L);
        StreamCut endStreamCut = StreamCut.UNBOUNDED;

        // start, end cannot be UNBOUNDED
        SegmentRangeImpl.fromStreamCuts(segment, startStreamCut, endStreamCut);
    }
}

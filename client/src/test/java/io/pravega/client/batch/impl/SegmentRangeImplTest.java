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
}

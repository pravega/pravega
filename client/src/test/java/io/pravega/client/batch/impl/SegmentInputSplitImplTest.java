/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.batch.impl;

import io.pravega.client.segment.impl.Segment;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SegmentInputSplitImplTest {

    @Test(expected = IllegalStateException.class)
    public void testInvalidSegmentInputSplit() {
        //StartOffset > endOffset.
        SegmentInputSplitImpl.builder().endOffset(10L).startOffset(20L)
                                       .segment(new Segment("scope", "stream", 1)).build();
    }

    @Test(expected = NullPointerException.class)
    public void testNullSegment() {
        SegmentInputSplitImpl.builder().startOffset(10L).endOffset(20L).build();
    }

    @Test
    public void testValid() {
        SegmentInputSplitImpl split = SegmentInputSplitImpl.builder().endOffset(20L).segment(new Segment("scope", "stream",
                0)).build();
        assertEquals(0L, split.getStartOffset());
        assertEquals(20L, split.getEndOffset());
        assertEquals(new Segment("scope", "stream", 0), split.getSegment());
    }
}
